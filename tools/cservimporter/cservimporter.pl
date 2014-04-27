#!/usr/bin/perl
use strict;
use warnings;
use MongoDB;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Carp;
use JSON;

=head1 cservimporter.pl

Usage: cservimporter.pl [options] host port database

This tool connects to the old CServ Mongo database and imports its contents to
the Liminfra SQL database.

=cut

if(!caller) {
	# We were called directly
	my $database = pop @ARGV;
	my $port = pop @ARGV;
	my $host = pop @ARGV;
	if(!$database) {
		die "Usage: $0 [options] host port database\n";
	}

	my $lim = Limesco->new_from_args(\@ARGV);

	import_from_cserv_mongo($lim, $host, $port, $database);
}

=head2 Methods

=head3 import_from_cserv_mongo ($lim, $host, $port, $database)

Connect to the old CServ Mongo database and import its contents to the Liminfra
SQL database.

=cut

sub import_from_cserv_mongo {
	my ($lim, $host, $port, $database) = @_;
	my $client = MongoDB::MongoClient->new(host => $host, port => $port);
	my $cservdb = $client->get_database($database);
	my $dbh = $lim->get_database_handle();

	try {
		$dbh->begin_work;
		$dbh->do("LOCK TABLE account IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE cdr IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE invoice IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE invoice_itemline IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE phonenumber IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE pricing IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE sim IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE speakup_account IN ACCESS EXCLUSIVE MODE;");

		my $accounts_map = import_accounts($lim, $cservdb, $dbh);
		import_sims($lim, $cservdb, $dbh, $accounts_map);
		import_invoices($lim, $cservdb, $dbh, $accounts_map);
		my $pricing_map = import_pricings($lim, $cservdb, $dbh);
		import_cdrs($lim, $cservdb, $dbh, $accounts_map, $pricing_map);

		$dbh->commit;
		return;
	} catch {
		$dbh->rollback;
	}
}

=head3 verify_table_empty($dbh, $table)

Checks that the given table exists and is empty in the database pointed to by
$dbh. Throws if not.

=cut

sub verify_table_empty {
	my ($dbh, $table) = @_;
	my $sth = $dbh->prepare("SELECT EXISTS (SELECT relname FROM pg_class WHERE relname=?)");
	$sth->execute($table);
	if(!$sth->fetchrow_arrayref()->[0]) {
		croak "Will not continue: table $table does not exist";
	}

	# TODO: apparantly, $table exists so it was safe to use in a CREATE TABLE statement
	# however, it would be nicer here if we didn't have to insert it directly into the query
	$sth = $dbh->prepare("SELECT EXISTS (SELECT 1 FROM $table)");
	$sth->execute();
	if($sth->fetchrow_arrayref()->[0]) {
		croak "Will not continue: table $table is not empty";
	}
}

=head3 array_to_postgres($arrayref)

Takes a single Perl arrayref and converts it to a Postgres array format, simply
stringifying its contents. Make sure to only give trusted data to this function
as the output will not be checked or escaped.

=cut

sub array_to_postgres {
	my ($arrayref) = @_;
	if(ref($arrayref) ne "ARRAY") {
		return;
	}
	if(!@$arrayref) {
		return '[]';
	}
	my $r;
	foreach(@$arrayref) {
		s/\\/\\\\/g;
		s/"/\\"/g;
		if(!defined $r) {
			$r = $_;
		} else {
			$r .= '", "' . $_;
		}
	}
	return '{"' . $r . '"}';
}

=head3 is_stub_account($cservdb, $accountid)

Returns if the given account ID belongs to a stub account (i.e. account state is UNPAID).

=cut

sub is_stub_account {
	my ($cservdb, $accountid) = @_;
	my $collection = $cservdb->get_collection("accounts");

	my $cursor = $collection->find({'state' => 'UNPAID', '_id' => MongoDB::OID->new($accountid)});
	return $cursor->next ? 1 : 0;
}

=head3 import_accounts($lim, $database, $dbh)

Import the accounts from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'account' table.

This method returns a hashref containing CServ account ID's as keys and
liminfra account ID's as values.

=cut

sub import_accounts {
	my ($lim, $database, $dbh) = @_;
	my $collection = $database->get_collection("accounts");

	verify_table_empty($dbh, "account");
	# restart the account ID sequence
	$dbh->do("ALTER SEQUENCE account_id_seq RESTART WITH 1");

	my $cursor = $collection->find();
	my %accounts_map;
	while(my $account = $cursor->next) {
		my $id = $account->{'_id'}->to_string();
		my $speakup_account = $account->{'externalAccounts'}{'speakup'};
		my $email = $account->{'email'};
		if(!$email) {
			# invalid account, just add the speakup_account and continue
			if($speakup_account) {
				$dbh->do("INSERT INTO speakup_account (name, period) VALUES (?, '(,)')", undef, $speakup_account);
			}
			next;
		}
		my $sth = $dbh->prepare("INSERT INTO account (id, period, company_name, first_name, last_name, street_address, postal_code,
			city, email, state) VALUES (NEXTVAL('account_id_seq'), '(,)', ?, ?, ?, ?, ?, ?, ?, ?)");
		$sth->execute($account->{'companyName'},
			$account->{'fullName'}{'firstName'}, $account->{'fullName'}{'lastName'},
			$account->{'address'}{'streetAddress'}, $account->{'address'}{'postalCode'},
			$account->{'address'}{'locality'}, $email, $account->{'state'});
		my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});
		$accounts_map{$id} = $account_id;
		if($speakup_account) {
			$dbh->do("INSERT INTO speakup_account (name, period, account_id) VALUES (?, '(,)', ?)", undef, $speakup_account, $account_id);
		}
	}
	return \%accounts_map;
}

=head3 import_sims($lim, $database, $dbh, $accounts_map)

Import the SIMs from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'sim' and 'phonenumber' tables. The
accounts_map is a hashref containing CServ account ID's as keys and liminfra
account ID's as values.

=cut

sub import_sims {
	my ($lim, $database, $dbh, $accounts_map) = @_;
	my $collection = $database->get_collection("sims");

	verify_table_empty($dbh, "sim");
	verify_table_empty($dbh, "phonenumber");

	my $cursor = $collection->find();
	while(my $sim = $cursor->next) {
		my $iccid = $sim->{'_id'};
		my $puk = $sim->{'puk'};
		my $state = $sim->{'state'};
		if($state eq "STOCK") {
			$dbh->do("INSERT INTO sim (iccid, period, puk, state) VALUES (?, '(,)', ?, 'STOCK')", undef, $iccid, $puk);
			next;
		}

		my $mongo_account_id = $sim->{'ownerAccountId'};
		my $account_id = $accounts_map->{$mongo_account_id};

		$sim->{'sipSettings'} = {} if(!$sim->{'sipSettings'});

		my $lastMonthlyFeesInvoice;
		my $lastMonthlyFeesDate;
		if($sim->{'lastMonthlyFeesInvoice'}) {
			my $lastMonthlyFeesYear = $sim->{'lastMonthlyFeesInvoice'}{'year'};
			my $lastMonthlyFeesMonth = $sim->{'lastMonthlyFeesInvoice'}{'month'};
			$lastMonthlyFeesInvoice = $sim->{'lastMonthlyFeesInvoice'}{'invoiceId'};
			$lastMonthlyFeesDate = sprintf("%04d-%02d-01", $lastMonthlyFeesYear, $lastMonthlyFeesMonth + 1);
		}

		my $sth = $dbh->prepare("INSERT INTO sim (iccid, period, state, puk, owner_account_id, data_type,
			exempt_from_cost_contribution, porting_state, activation_invoice_id, last_monthly_fees_invoice_id,
			last_monthly_fees_month, call_connectivity_type, sip_realm, sip_username, sip_authentication_username,
			sip_password, sip_uri, sip_expiry, sip_trunk_password) VALUES (?, '(,)', ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

		my @sipSettings;
		for(qw(realm username authenticationUsername password uri expiry speakupTrunkPassword)) {
			push @sipSettings, $sim->{'sipSettings'}{$_};
		}
		$sth->execute($iccid, $state, $puk, $account_id, $sim->{'apnType'}, $sim->{'exemptFromCostContribution'},
			$sim->{'portingState'}, $sim->{'activationInvoiceId'}, $lastMonthlyFeesInvoice, $lastMonthlyFeesDate,
			$sim->{'callConnectivityType'}, @sipSettings);

		my $phone = $sim->{'phoneNumber'};
		# Don't import phone numbers when state is DISABLED, or we have duplicates
		if($phone && $state ne "DISABLED") {
			$phone =~ s/-//;
			if($phone =~ /^06(\d+)$/) {
				$phone = "316$1";
			}
			$sth = $dbh->prepare("INSERT INTO phonenumber (phonenumber, period, sim_iccid) VALUES (?, '(,)', ?)");
			$sth->execute($phone, $iccid);
		}
	}
}

=head3 import_invoices($lim, $cservdb, $dbh, $accounts_map)

Import the invoices from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'invoice' and 'invoice_itemline' tables.
The accounts_map is a hashref containing CServ account ID's as keys and
liminfra account ID's as values.

=cut

sub import_invoices {
	my ($lim, $cservdb, $dbh, $accounts_map) = @_;
	my $collection = $cservdb->get_collection("invoices");

	verify_table_empty($dbh, "invoice");
	verify_table_empty($dbh, "invoice_itemline");

	my $cursor = $collection->find();
	while(my $invoice = $cursor->next) {
		my $id = $invoice->{'_id'};
		my $mongo_account_id = $invoice->{'accountId'};
		my $account_id = $accounts_map->{$mongo_account_id};

		my $date = $invoice->{'creationDate'};
		my $invoice_date = $date->ymd;
		my $creation_time = $date->iso8601();

		my $sth = $dbh->prepare("INSERT INTO invoice (id, account_id, date,
		   creation_time, rounded_without_taxes, rounded_with_taxes) VALUES
		   (?, ?, ?, ?, floorn(?::numeric/10000, 2), floorn(?::numeric/10000, 2));");
		$sth->execute($id, $account_id, $invoice_date, $creation_time,
			$invoice->{'totalWithoutTaxes'}, $invoice->{'totalWithTaxes'});

		$sth = $dbh->prepare("INSERT INTO invoice_itemline (invoice_id, description,
		   type, base_amount, taxrate, item_price, item_count, rounded_total,
		   number_of_calls, number_of_seconds, price_per_call,
		   price_per_minute) VALUES (?, ?, ?, floorn(?::numeric/10000, 2),
		   ?, floorn(?::numeric/10000, 4), ?, floorn(?::numeric/10000, 2), ?, ?,
		   floorn(?::numeric/10000, 4), floorn(?::numeric/10000, 4));");
		foreach my $line (@{$invoice->{'itemLines'}}) {
			my $description = $line->{'description'};
			if($line->{'multilineDescription'} && @{$line->{'multilineDescription'}}) {
				$description = join "\n", @{$line->{'multilineDescription'}};
			}
			$sth->execute($id, $description, uc($line->{'type'}), undef,
				map {$line->{$_}} qw(taxRate itemPrice
				itemCount totalPrice numberOfCalls
				numberOfSeconds pricePerCall pricePerMinute));
		}
		foreach my $line (@{$invoice->{'taxLines'}}) {
			$sth->execute($id, "Tax", "TAX", $line->{'baseAmount'}, $line->{'taxRate'},
				$line->{'taxAmount'}, 1, $line->{'taxAmount'}, undef, undef, undef, undef);
		}
	}
}

=head3 import_pricings($lim, $cservdb, $dbh)

Import the pricings from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'pricing' table.

This method returns a hashref containing CServ pricing ID's as keys and
liminfra pricing ID's as values.

=cut

sub import_pricings {
	my ($lim, $cservdb, $dbh) = @_;
	my $collection = $cservdb->get_collection("pricing");

	verify_table_empty($dbh, "pricing");

	my %pricing_map;

	my $sth = $dbh->prepare("INSERT INTO pricing (period, description, service,
		hidden, call_connectivity_type, source, destination, direction, connected,
		cost_per_line, cost_per_unit, price_per_line, price_per_unit)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::numeric/10000, ?::numeric/10000,
		?::numeric/10000, ?::numeric/10000);");

	# Add pricing for unconnected voice calls
	$sth->execute('(,)', "Niet-opgenomen oproepen", "VOICE",
		"t", [], [], [], [], ["f"], 0, 0, 0, 0);

	my $cursor = $collection->find();
	while(my $pricing = $cursor->next) {
		my $id = $pricing->{'_id'}->to_string();
		my $service = $pricing->{'service'};
		my $period = '[' . $pricing->{'applicability'}{'validFrom'}->ymd() . ',)';
		my $cdrt = $pricing->{'applicability'}{'cdrType'};
		my $ccv = array_to_postgres($pricing->{'applicability'}{'callConnectivityType'});
		my $dest = array_to_postgres($pricing->{'applicability'}{'destination'});
		my $dir = [];
		if("EXT_PBX" ~~ $cdrt || "EXT_MOBILE" ~~ $cdrt) {
			push @$dir, "IN";
		}
		if("MOBILE_PBX" ~~ $cdrt || "PBX_MOBILE" ~~ $cdrt || "MOBILE_EXT" ~~ $cdrt) {
			push @$dir, "OUT";
		}
		if(@$dir > 1) {
			die "More than one direction allowed, pricing rule cdrtype is ambiguous";
		}
		# 'Connected' was not checked earlier: for voice it must be true, otherwise it must allow any
		my $conn = [];
		if(uc($service) eq "VOICE") {
			$conn = ['t'];
		}

		my ($costperline, $costperunit, $priceperline, $priceperunit);
		if($service eq "voice") {
			$costperline = $pricing->{'cost'}{'perCall'};
			$costperunit = $pricing->{'cost'}{'perMinute'} / 60;
			$priceperline = $pricing->{'price'}{'perCall'};
			$priceperunit = $pricing->{'price'}{'perMinute'} / 60;
		} elsif($service eq "sms") {
			$costperline = $pricing->{'cost'}{'perSms'};
			$costperunit = 0;
			$priceperline = $pricing->{'price'}{'perSms'};
			$priceperunit = 0;
		} elsif($service eq "data") {
			$costperline = 0;
			$costperunit = $pricing->{'cost'}{'perKilobyte'};
			$priceperline = 0;
			$priceperunit = $pricing->{'price'}{'perKilobyte'};
		}

		$ccv ||= [];
		$dest ||= [];
		$dir ||= [];
		$sth->execute($period, $pricing->{'description'}, uc($pricing->{'service'}),
			$pricing->{'hidden'} || "false", $ccv, [], $dest, $dir, $conn, $costperline,
			$costperunit, $priceperline, $priceperunit);

		my $pricing_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "pricing_id_seq"});
		$pricing_map{$id} = $pricing_id;
	}
	return \%pricing_map;
}

=head3 import_cdrs($lim, $cservdb, $dbh, $accounts_map, $pricing_map)

Import the CDRs from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'cdr' table.  The accounts_map is a
hashref containing CServ account ID's as keys and liminfra account ID's as
values; the pricing_map is a hashref containing CServ pricing ID's as keys and
liminfra pricing ID's as values.

=cut

sub import_cdrs {
	my ($lim, $cservdb, $dbh, $accounts_map, $pricing_map) = @_;
	my $collection = $cservdb->get_collection("cdr");

	verify_table_empty($dbh, "cdr");

	my %pricing_map;
	my %unlinked_cdrs_map;

	my $cursor = $collection->find();
	while(my $cdr = $cursor->next) {
		my $sth = $dbh->prepare("INSERT INTO cdr (speakup_account, direction, pricing_id, pricing_info,
			time, service, units, call_id, \"from\", \"to\", invoice_id,
			connected, destination, computed_price, computed_cost) VALUES
			(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::numeric/10000, ?::numeric/10000);");

		my $service = $cdr->{'service'};

		my $pricing_id;
		if($cdr->{'pricing'}) {
			$pricing_id = $pricing_map->{$cdr->{'pricing'}{'pricingRuleId'}};
		}

		my $time = $cdr->{'time'}->iso8601();
		my $units = $service eq "voice" ? $cdr->{'seconds'} :
			$service eq "data" ? $cdr->{'kilobytes'} : 1;
		my $speakup_account = $cdr->{'additionalInfo'}{'externalAccount'};
		$speakup_account =~ s/^\s+//;
		$speakup_account =~ s/\s+$//;
		my $direction = uc($cdr->{'additionalInfo'}{'10'});

		# check if the account this cdr was linked to is still the same
		my $should_be_mongo_account = $cdr->{'account'};
		my $check_sth = $dbh->prepare("SELECT account_id, period FROM speakup_account WHERE lower(name)=lower(?) and period @> ?::date");
		$check_sth->execute($speakup_account, $time);
		my $check_result = $check_sth->fetchrow_arrayref();
		if(is_stub_account($cservdb, $should_be_mongo_account) && $check_result && defined($check_result->[0])) {
			die "CDR account points at stub account, but it's not stub in liminfra\n";
		} elsif(!is_stub_account($cservdb, $should_be_mongo_account) && !$check_result) {
			warn "Nonstub Mongo account in CDR: $should_be_mongo_account\n";
			warn "No account ID in speakup_account for name=$speakup_account, period=$time\n";
			die "CDR account points at real account, but it's stub in liminfra\n";
		} elsif(!$accounts_map->{$should_be_mongo_account}) {
			# CDR is linked to nonexistant account, unlink it
			$unlinked_cdrs_map{$should_be_mongo_account} ||= [];
			push @{$unlinked_cdrs_map{$should_be_mongo_account}}, $speakup_account;
		} elsif($check_result->[0] != $accounts_map->{$should_be_mongo_account}) {
			die "This CDR was owned by a different account than expected from the externalAccount field\n";
		}

		# check if the computed price and cost still makes sense
		my $pricing_info;
		if($pricing_id) {
			$pricing_info = {description => "Imported from CServ"};
			$check_sth = $dbh->prepare("SELECT price_per_line + price_per_unit * ? AS price, cost_per_line + cost_per_unit * ? AS cost FROM pricing WHERE id=?");
			$check_sth->execute($units, $units, $pricing_id);
			$check_result = $check_sth->fetchrow_arrayref();
			if(!$check_result) {
				die "Could not recompute price for CDR\n";
			} elsif($cdr->{'service'} eq "voice" && !$cdr->{'connected'}) {
				if($cdr->{'pricing'}{'computedPrice'} > 0
				|| $cdr->{'pricing'}{'computedCost'} > 0) {
					die "Unconnected CDR with pricing\n";
				}
			} elsif(abs($check_result->[0] - $cdr->{'pricing'}{'computedPrice'} / 10000) >= 1) {
				warn "CDR ID: " . $cdr->{'_id'} . "\n";
				warn sprintf("Expected price: %f, computed price: %f\n", $check_result->[0], $cdr->{'pricing'}{'computedPrice'});
				die "Mismatch in price of CDR\n";
			} elsif(abs($check_result->[1] - $cdr->{'pricing'}{'computedCost'} / 10000) >= 1) {
				die "Mismatch in cost of CDR\n";
			}
		}

		$sth->execute($speakup_account, $direction, $pricing_info ? $pricing_id : undef, $pricing_info ? encode_json($pricing_info) : undef, $time, uc($service), $units,
			(map { $cdr->{$_} } qw(callId from to invoice connected destination)),
			$cdr->{'pricing'}{'computedPrice'}, $cdr->{'pricing'}{'computedCost'});
	}

	if(%unlinked_cdrs_map) {
		print "Unlinked CDR counts for nonexistant accounts:\n";
		foreach my $account (keys %unlinked_cdrs_map) {
			my @accounts;
			foreach my $a(@{$unlinked_cdrs_map{$account}}) {
				if(!grep {$a eq $_} @accounts) {
					push @accounts, $a;
				}
			}
			print "  $account (" . join(", ", @accounts) . ")\n";
		}
	}
}

1;
