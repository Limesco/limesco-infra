#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::MT940;
use Try::Tiny;
use DateTime;
use Term::Menu;
use Data::Dumper;
use LWP::UserAgent;
use HTML::DOM;
use HTTP::Cookies;
use CAM::PDF;
use CAM::PDF::PageText;
use Text::CSV;
use JSON;
use URI::Encode qw(uri_encode);

do '../directdebit/directdebit.pl' or die $!;
do '../invoice-export/invoice-export.pl' or die $!;

Limesco::DirectDebit->import(qw(get_directdebit_file mark_directdebit_file));

=head1 bankaccount-import.pl

Usage:
  bankaccount-import.pl [infra-options] --mt940 filename
  bankaccount-import.pl [infra-options] --json filename
  bankaccount-import.pl [infra-options] --create-payment

If --mt940 is given, import bank account transactions from an mt940 file. All
transactions not corresponding to the configured bank account are ignored. the
start date and balance must correspond to the last end date and end balance
stored in the database.

During import, Payments will be created for all transactions containing an
invoice number. If a transaction is missed, it can be created interactively
using --create-payment.

=cut

if(!caller) {
	my $mt940;
	my $json;
	my $create;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--mt940") {
			$mt940 = $args->[++$$iref];
		} elsif($arg eq "--create-payment") {
			$create = 1;
		} elsif($arg eq "--json") {
			$json = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if($create) {
		create_payment_interactively($lim);
	} elsif($mt940) {
		import_mt940_file($lim, $mt940);
	} elsif($json) {
		import_json_file($lim, $json);
	} else {
		die "Use --help to get usage information.\n";
	}
}

=head2 Methods

=cut

sub parse_mt940_balance {
	my ($d_c, $year, $month, $day, $balance) = $_[0] =~ /^([DC])(\d\d)(\d\d)(\d\d)EUR([\d]+,\d\d)$/;
	if(!$day) {
		die "Did not understand balance value: " . $_[0] . "\n";
	}
	$balance =~ s/,/./;
	$balance += 0; # convert to number
	if($d_c eq "D") {
		$balance = -$balance;
	}
	my $date = DateTime->new(year => "20$year", month => $month, day => $day);
	return ($date, $balance);
}

=head3 import_json_file($lim, $filename)

Import a JSON file to the Payments table. The JSON file must consist of an object
with a 'payments' key, which contains a list of payments. Each payment must have
'account_id', 'type', 'amount', 'date' (YYYY-MM-DD), 'origin' and 'description' keys.

=cut

sub import_json_file {
	my ($lim, $filename) = @_;
	open my $fh, '<', $filename or die $!;
	my $json_text;
	while(<$fh>) {
		$json_text .= $_;
	}
	close $fh;
	my $json = decode_json($json_text);
	my $dbh = $lim->get_database_handle;
	$dbh->begin_work;

	try {
		foreach my $payment (@{$json->{'payments'}}) {
			create_payment($dbh, $payment);
		}
		$dbh->commit;
	} catch {
		my $exception = $_ || "Unknown error";
		$dbh->rollback;
		die $exception;
	};
}

=head3 import_mt940_file($lim, $filename)

Import an MT940 file to the Payments table. The start date and balance for the
first statement in the file must correspond to the end date and balance for the
last statement in the bankaccountimports table, if any. The bankaccountimports
table will be updated with the loaded statements.

Payments will be created for any transactions containing an invoice ID in the
description.

=cut

sub import_mt940_file {
	my ($lim, $filename) = @_;
	my $dbh = $lim->get_database_handle;
	$dbh->begin_work;

	try {
		$dbh->do("LOCK TABLE bankaccountimports");

		my ($current_date, $current_balance);
		{
			my $sth = $dbh->prepare("SELECT enddate, endbalance FROM bankaccountimports ORDER BY enddate DESC LIMIT 1");
			$sth->execute;
			my $row = $sth->fetchrow_hashref;
			if($row && $row->{'enddate'} =~ /^(\d{4})-(\d\d)-(\d\d)$/) {
				$current_date = DateTime->new(
					year => $1,
					month => $2,
					day => $3,
				);
				$current_balance = $row->{'endbalance'};
			}
		}

		my ($first_date, $first_balance);

		# An MT940 file consists of statements for a specific day and
		# account. In the weekends, no statements are written. Each
		# statement begins with begin and end balances, including "date
		# of balance". Here, we check that every begin balance
		# corresponds with the last end balance, including the dates.

		my @dates = Limesco::MT940->parse_from_file($filename);
		my @accounts = ("NL24RABO0169207587 EUR");
		my $transactions = 0;
		my $payments = 0;
		foreach(@dates) {
			my $account = $_->{'account'};
			if(!grep { $_ eq $account || $_ eq "$account EUR" } @accounts) {
				# Skip transactions to unknown accounts
				next;
			}

			my ($start_date, $start_balance) = parse_mt940_balance($_->{'start_balance'});
			my ($end_date, $end_balance) = parse_mt940_balance($_->{'end_balance'});

			if(defined($current_date) && $current_date->ymd ne $start_date->ymd) {
				die "Expected transactions entry with begin date " . $current_date->ymd . ", but got one with " . $start_date->ymd . "\n";
			}
			if(defined($current_balance) && $current_balance != $start_balance) {
				die "Expected transactions entry on begin date " . $current_date->ymd . " to have start balance $current_balance, but it has $start_balance\n";
			}

			foreach(@{$_->{'transactions'}}) {
				$transactions++;
				my $description = $_->{'description'};
				my $transaction = $_->{'transaction'};
				my ($year, $month, $day, $direction, $amount, $peer) =
					$transaction =~ /^(\d\d)(\d\d)(\d\d)([DC])0(\d+,\d\d)N\d{3}\w+\s*(\w\w\d\d.+)?$/;
				if(!$direction) {
					warn "Could not parse transaction description: $transaction\n";
				}
				$amount =~ s/,/./;
				$amount += 0;
				if($direction eq 'D') {
					$amount = -$amount;
				}
				$payments += evaluate_transaction($lim, $dbh,
					new DateTime(year => "20$year", month => $month, day => $day),
					$peer, $description, $amount);
			}

			if(!defined($first_date)) {
				$first_date = $start_date;
				$first_balance = $start_balance;
			}
			$current_date = $end_date;
			$current_balance = $end_balance;
		}

		if(!$first_date) {
			die "No correct date records found. Maybe the correct account isn't added to the configuration file?\n";
		}

		my $sth = $dbh->prepare("INSERT INTO bankaccountimports (startdate, startbalance, enddate, endbalance, transactions, payments) VALUES (?, ?, ?, ?, ?, ?)");
		$sth->execute($first_date->ymd, $first_balance, $current_date->ymd, $current_balance, $transactions, $payments);

		$dbh->commit;
	} catch {
		my $exception = $_ || "Unknown error";
		$dbh->rollback;
		die $exception;
	};
}

=head2 evaluate_transaction($lim, $peer, $description, $amount)

Evaluate this transaction. Returns the number of payments added for it.

=cut

sub evaluate_transaction {
	my ($lim, $dbh, $date, $peer, $description, $amount) = @_;
	my $single_line_description = $description;
	$single_line_description =~ s/[\n\r]//g;
	my @invoices = $single_line_description =~ /(\d\dC\d{6})\D/i;
	if(@invoices) {
		# Explicit invoice ID(s), so handle them
		my $account_id;
		foreach(@invoices) {
			my $account_from_invoice = $dbh->prepare("SELECT account_id FROM invoice WHERE id=?");
			$account_from_invoice->execute(uc($_));
			my $this_account_id = $account_from_invoice->fetchrow_hashref();
			if(!$this_account_id) {
				warn "Invoice ID $_ referenced but not found, ignoring\n";
				next;
			}
			$this_account_id = $this_account_id->{'account_id'};
			if(defined($account_id) && $this_account_id != $account_id) {
				warn "Transaction pays for multiple accounts; that's unsupported, ignoring it\n";
				undef $account_id;
				last;
			}
			$account_id = $this_account_id;
		}
		if($account_id) {
			create_payment($dbh, {
				account_id => $account_id,
				type => 'BANK_TRANSFER',
				amount => $amount,
				date => $date->ymd,
				origin => $peer,
				description => $description,
			});
			return 1;
		}
	}

	if($description =~ /^\/PREF\/(LDD-20\d\d-\d\d-\d\d-(?:FRST|RCUR)\d*)$/) {
		my $directdebit_id = $1;
		my $file = get_directdebit_file($dbh, $directdebit_id);
		my $num_payments = 0;
		my $sum_price = 0;
		foreach my $transaction (@{$file->{'transactions'}}) {
			my $invoice_id = $transaction->{'invoice_id'};
			my $invoice_price = $transaction->{'amount'};
			if(!defined($invoice_price)) {
				# Until June 3 (082a99f), direct debit transactions did not have their
				# amount stored along with them. For these transactions, it is unclear
				# what part of the directdebit aggregate transaction belongs to this
				# transaction. The SQL table should be updated, in those cases, to reflect
				# the actual amount from the XML file originally sent to the bank.
				die "Old DirectDebit transaction without amount encountered in bank transaction\n";
			}

			my $sth = $dbh->prepare("SELECT account_id, rounded_with_taxes FROM invoice WHERE id=?");
			$sth->execute($invoice_id);
			my $invoice = $sth->fetchrow_hashref();

			if($invoice_price > 0) {
				$sum_price += $invoice_price;
				$num_payments++;
				create_payment($dbh, {
					account_id => $invoice->{'account_id'},
					type => 'DIRECTDEBIT',
					amount => $invoice_price,
					date => $date->ymd,
					origin => $directdebit_id,
					description => $directdebit_id,
				});
			}
		}

		my $diff = abs($sum_price - $amount);
		if($diff > 0.05) {
			warn "Incoming DirectDebit aggregate transaction $directdebit_id had amount $amount, while sum of payments created was $sum_price.\n";
			print STDERR "OK to process like this? [Yn] ";
			my $ok = <STDIN>;
			1 while chomp $ok;
			if($ok =~ /^(y.*|$)/g) {
				print STDERR "\n";
			} else {
				die "OK, stopping.\n";
			}
		}
		mark_directdebit_file($dbh, $file->{'id'}, "SUCCESS");
		return $num_payments;
	}

	if($single_line_description =~ /\/\/NAME\/[a-zA-Z ]+ (?:Derdengelden T|TargetMedia)\/.+klantnr 76992 (\d+)\s*(?:\/|$)/i) {
		print "Asking for TargetPay transactions with ID $1\n";
		my %transactions = targetpay_get_transactions($lim, $1);
		my $num_payments = 0;
		foreach my $payment (values %transactions) {
			my $ymd = substr($payment->{'DateTime'}, 0, 10);
			my $amount = $payment->{'Amount'};
			$amount =~ s/,/./g;
			warn "TargetPay transaction at " . $payment->{'DateTime'} . " of $amount must be hand-linked.\n";
			warn "  Name: " . $payment->{'Name'} . "\n";
			warn "  Description: " . $payment->{'Description'} . "\n";
			warn "  Origin bank account: " . $payment->{'Account'} . "\n";
			print STDERR "Enter account ID or leave empty to skip: ";
			my $account_id = <STDIN>;
			1 while chomp $account_id;
			if($account_id) {
				$num_payments++;
				create_payment($dbh, {
					account_id => $account_id,
					type => 'TARGETPAY',
					amount => $amount,
					date => $ymd,
					origin => $payment->{'Account'},
					description => $payment->{'Description'},
				});
				warn "OK, payment created.\n\n";
			}
		}
		return $num_payments;
	}

	# Is this an incoming transaction from a single known bank account?
	my $sth = $dbh->prepare("SELECT DISTINCT account_id FROM payment WHERE type='BANK_TRANSFER' AND origin=?");
	$sth->execute($peer);
	my $account_id = $sth->fetchrow_hashref;
	if($account_id && $sth->fetchrow_hashref) {
		warn "Ignoring transaction at " . $date->ymd . " of $amount from $peer; it had no invoice number and could be linked to multiple account ID's:\n$description\n";
		return 0;
	} elsif($account_id && $amount < 0) {
		warn "Ignoring transaction at " . $date->ymd . " of $amount from $peer: it had no invoice number and is outgoing, and I won't automatically link outgoing transactions:\n$description\n";
		return 0;
	} elsif($account_id) {
		$account_id = $account_id->{'account_id'};
		my $account_sth = $dbh->prepare("SELECT first_name, last_name, company_name FROM account WHERE id=?");
		$account_sth->execute($account_id);
		my $account = $account_sth->fetchrow_hashref;
		if($account) {
			warn "Transaction at " . $date->ymd . " of $amount from $peer can be linked using bank account:\n";
			warn "  Account: " . $account->{'first_name'} . " " . $account->{'last_name'} . " (" . $account->{'company_name'} . ")\n";
			warn "  Description: " . $description . "\n";
			$|++;
			while(1) {
				print STDERR "OK to process like this? [Yn] ";
				my $ok = <STDIN>;
				1 while chomp $ok;
				if($ok =~ /^(y.*|$)/i) {
					create_payment($dbh, {
						account_id => $account_id,
						type => 'BANK_TRANSFER',
						amount => $amount,
						date => $date->ymd,
						origin => $peer,
						description => $description,
					});
					warn "OK, payment created.\n\n";
					return 1;
				} elsif($ok =~ /^n.*/i) {
					warn "OK, skipped.\n\n";
					return 0;
				}
			}
		} else {
			warn "Transaction at " . $date->ymd . " of $amount from $peer was known bank account but account ID $account_id did not exist at that date.\n";
			warn "  Description: " . $description . "\n";
			return 0;
		}
	} elsif($amount > 0) {
		$peer ||= "unknown bank account";
		warn "Incoming transaction at " . $date->ymd . " of $amount from $peer had no invoice number and unknown bank account, so could not be linked.\n";
		warn "  Description: " . $description . "\n";
		print STDERR "Enter account ID or leave empty to skip: ";
		my $account_id = <STDIN>;
		1 while chomp $account_id;
		if($account_id) {
			create_payment($dbh, {
				account_id => $account_id,
				type => 'BANK_TRANSFER',
				amount => $amount,
				date => $date->ymd,
				origin => $peer,
				description => $description,
			});
			warn "OK, payment created.\n\n";
			return 1;
		}
		warn "\n";
	}
	return 0;
}

=head3 create_payment($lim / $dbh, $payment)

Insert the given payment into the database.

=cut

sub create_payment {
	my ($lim, $payment) = @_;
	my $dbh = ref($lim) eq "Limesco" ? $lim->get_database_handle() : $lim;
	my $sth = $dbh->prepare("INSERT INTO payment (account_id, type, amount, date, origin, description) VALUES (?, ?, ?, ?, ?, ?)");
	$sth->execute(map { $payment->{$_} } qw/account_id type amount date origin description/);
}

=head3 create_payment_interactively($lim)

Ask a few questions to interactively create a payment.

=cut

sub create_payment_interactively {
	my ($lim) = @_;
	my $payment = {};
	$|++;
	my $menu = Term::Menu->new();
	until($payment->{'account_id'}) {
		my $accountlike = $menu->question("Add payment for which account? ");
		1 while chomp $accountlike;
		try {
			if($accountlike =~ /^\d+$/) {
				my $account = $lim->get_account($accountlike);
				$payment->{'account_id'} = $account->{'id'} if($account);
			} else {
				my $account = $lim->get_account_like($accountlike);
				$payment->{'account_id'} = $account->{'id'} if($account);
			}
		} catch {
			warn $_;
		};
	}

	until($payment->{'type'}) {
		$payment->{'type'} = $menu->menu(
			BANK_TRANSFER => ["Bank transaction", 1],
			TARGETPAY => ["TargetPay (iDeal)", 2],
			DIRECTDEBIT => ["DirectDebit", 3],
			COINQY => ["Coinqy (Bitcoin)", 4],
			BITKASSA => ["BitKassa (Bitcoin)", 5],
			ADMINISTRATIVE => ["Administrative (other)", 6],
		);
	}

	until($payment->{'amount'}) {
		$payment->{'amount'} = $menu->question("Amount to pay (negative for amounts paid to them)? ");
		$payment->{'amount'} += 0;
	}

	until($payment->{'date'}) {
		my $dt = DateTime->now();
		$payment->{'date'} = $menu->question("Date of payment? [" . $dt->ymd . "] ");
		1 while chomp $payment->{'date'};
		$payment->{'date'} ||= $dt->ymd;
		delete $payment->{'date'} if $payment->{'date'} !~ /^\d{4}-\d\d-\d\d$/;
	}

	$payment->{'description'} = $menu->question("Description? ");
	1 while chomp $payment->{'description'};

	my $origin_question = $payment->{'type'} eq "BANK_TRANSACTION" ? "Origin bank account number" : "Origin (optional)";
	$payment->{'origin'} = $menu->question("$origin_question? ");
	1 while chomp $payment->{'origin'};

	print "Are you sure you want to create the following payment?\n";
	print Dumper($payment);
	if($menu->question("Are you sure? [yN] ") =~ /^y$/i) {
		create_payment($lim, $payment);
	}
}

=head3 targetpay_get_transactions($lim, $invoiceid)

Retrieve a TargetPay transaction list for the given TargetMedia invoice ID.

=cut

sub targetpay_get_transactions {
	my ($lim, $invoiceid) = @_;
	my $config = $lim->targetpay_config();

	my $cookie_jar = HTTP::Cookies->new;
	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->agent("Liminfra/0.0 ");
	$ua->cookie_jar($cookie_jar);

	# Step 1: log in
	my $response = $ua->get($config->{'uri_base'} . '/auth/login');
	my ($csrf) = $response->decoded_content =~ /name="csrf-token" content="([^"]+)">/;
	if(!$csrf) {
		die "TargetPay login CSRF request failed: " . $response->status_line;
	}

	$response = $ua->post($config->{'uri_base'} . '/auth/login', Content => 
		uri_encode('_csrf='.$csrf) . '&' .
		uri_encode('LoginForm[username]=' . $config->{'username'}) . '&' .
		uri_encode('LoginForm[password]=' . $config->{'password'}) . '&' .
		uri_encode('LoginForm[rememberMe]=0') . '&' .
		uri_encode('LoginForm[rememberMe]=1') . '&' .
		uri_encode('login-button='),
	);
	if($response->code != 302) {
		die "TargetPay login token request failed: " . $response->status_line;
	}
	if($cookie_jar->as_string() !~ /_identity/) {
		die "No identity found in cookies, TargetPay login request failed";
	}

	# Step 2: retrieve PDF download link for this invoice ID
	$response = $ua->get($config->{'uri_base'} . '/crm/invoices/overview?product=ide&m=2&InvoiceSearch%5Bfactuurnr%5D=' . int($invoiceid));
	if($response->code != 200) {
		die "TargetPay invoice overview request failed: " . $response->status_line;
	}
	my ($a_link) = $response->decoded_content =~ /href="(\/crm\/invoices\/show-invoice\?invoiceID=\d+)"/;
	if(!$a_link) {
		die "Failed to find PDF URI in invoice overview";
	}

	# Step 3: retrieve PDF and find payment IDs for this invoice
	$response = $ua->get($config->{'uri_base'} . $a_link);
	if($response->code != 200) {
		die "TargetPay invoice PDF request failed: " . $response->status_line;
	}
	my $pdf = CAM::PDF->new($response->decoded_content);
	my $text = CAM::PDF::PageText->render($pdf->getPageContentTree(1));
	my @words = split /\s+/, $text;
	my $counting = 0;
	my @paymentids;
	foreach(@words) {
		if($counting && /^(\d+)(?:\(.*\))?,?$/) {
			push @paymentids, $1;
		} elsif($counting) {
			$counting = 0;
		} elsif($_ eq "Betalingskenmerken:") {
			$counting = 1;
		}
	}

	# Step 4: retrieve information on all payments
	my ($day, $month, $year) = $text =~ /(\d\d?) (jan|feb|maa|apr|mei|jun|jul|aug|sep|okt|nov|dec) (20\d\d)/;
	if(!$year) {
		die "Failed to parse TargetPay date from HTTP response\n";
	}
	my %month = (jan => 1, feb => 2, maa => 3, apr => 4, mei => 5, jun => 6, jul => 7, aug => 8, sep => 9, okt => 10, nov => 11, dec => 12);
	my $date = DateTime->new(year => $year, month => $month{$month}, day => $day);
	my %payments;
	my $start = $date->clone->subtract(months => 1);
	my $end = $date->clone->add(months => 1);
	foreach my $txid (@paymentids) {
		my %parameters = (
			'OrderdetailsSearch[datetime]' => sprintf("%02d-%02d-%04d - %02d-%02d-%04d", $start->day, $start->month, $start->year, $end->day, $end->month, $end->year),
			'OrderdetailsSearch[txid]' => $txid,
			'OrderdetailsSearch[cname]' => '',
			'OrderdetailsSearch[cbank]' => '',
			'OrderdetailsSearch[cprice]' => '',
			'OrderdetailsSearch[txdescription]' => '',
			'OrderdetailsSearch[nietgeleverd]' => '',
			'OrderdetailsSearch[id]' => '',
			'product' => 'ide',
			'm' => 2,
		);
		my $url = '/crm/orderdetails/overview?';
		foreach(keys %parameters) {
			$url .= uri_encode($_) . '=' . uri_encode($parameters{$_}) . '&';
		}
		$response = $ua->get($config->{'uri_base'} . $url);
		if($response->code != 200) {
			die "TargetPay invoice information request failed: " . $response->status_line;
		}
		my $dom_tree = new HTML::DOM;
		$dom_tree->write($response->decoded_content);
		$dom_tree->close();

		my $div = $dom_tree->getElementById("order-details-container");
		if(!$div) {
			die "TargetPay invoice information request failed: no information for payment $txid";
		}
		my $table = $div->getElementsByTagName('table')->[0];
		if(!$table) {
			die "TargetPay invoice information request failed: no information for payment $txid";
		}
		my $tbody = $table->getElementsByTagName('tbody')->[0];
		my $tr = $tbody->getElementsByTagName('tr')->[0];
		my @children = $tr->childNodes;
		my ($day, $month, $year) = $children[0]->as_text() =~ /(\d\d)-(\d\d)-(\d{4})/;
		my $time = $children[1]->as_text();
		my $name = $children[2]->as_text();
		my $bankaccount = $children[3]->as_text();
		my ($amount) = $children[4]->as_text() =~ / (\d+[,.]\d+)$/;
		my $kenmerk = $children[5]->as_text();

		$payments{$txid} = {
			DateTime => "$year-$month-$day $time",
			Amount => $amount,
			Name => $name,
			Description => $kenmerk,
			Account => $bankaccount,
		};
	}

	return %payments;
}

1;
