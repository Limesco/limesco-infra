#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use JSON;
use Try::Tiny;
use Data::Dumper;

=head1 cdr-pricing.pl

Usage: cdr-pricing.pl [infra-options] [--cdr id]

* retrieve all unpriced CDRs
* for each cdr, get the relevant SIM and account
	account by speakup_account, SIM by phone number
* try to find an applicable pricing rule for that cdr
* compute price and cost and store them in the CDR table

=cut

if(!caller) {
	my $cdr;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--cdr") {
			$cdr = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if($cdr) {
		$cdr = get_cdr($lim, $cdr);
		print "Pricing CDR:\n  " . Dumper($cdr) . "\n";
		try {
			price_cdr($lim, $cdr);
			print "After pricing:\n  " . Dumper($cdr) . "\n";
		} catch {
			print "Caught exception: $_\n";
		};
		exit;
	}

	my $pricable_cdrs = 0;
	my @unpricable_cdrs;

	for_every_unpriced_cdr($lim, sub {
		my ($lim, $cdr) = @_;
		try {
			price_cdr($lim, $cdr);
			write_cdr_pricing($lim, $cdr);
			$pricable_cdrs++;
		} catch {
			if($_ =~ /\bunpricable\b/i) {
				push @unpricable_cdrs, $cdr;
			} else {
				# rethrow
				die $_;
			}
		};
	});

	print "$pricable_cdrs priced.\n";
	if(@unpricable_cdrs) {
		print "Unpricable CDRs:\n";
		foreach(@unpricable_cdrs) {
			print "  " . dump_cdr($_) . "\n";
		}
		print "$pricable_cdrs priced, " . scalar(@unpricable_cdrs) . " are still unpricable.\n\n";
		print "The first unpricable CDR looks like this:\n";
		print Dumper($unpricable_cdrs[0]);
		print "The error that occured while pricing the first unpricable CDR:\n";
		price_cdr($lim, $unpricable_cdrs[0]);
	}
}

=head2 Methods

=head3 get_cdr($lim, $cdr_id)

Fetch a specific CDR from the database.

=cut

sub get_cdr {
	my ($lim, $cdr_id) = @_;
	my $dbh = $lim->get_database_handle();

	my $sth = $dbh->prepare("SELECT * FROM cdr WHERE id=?");
	$sth->execute($cdr_id) or die "Query failed";
	my $cdr = $sth->fetchrow_hashref();
	if(!$cdr) {
		die "Couldn't find a CDR with that id: $cdr_id\n";
	}
	return $cdr;
}

=head3 for_every_unpriced_cdr($lim, $callback)

Call the given callback function for every unpriced CDR in the database. The callback
gets $lim as the first parameter, and the unpriced CDR as the second.

=cut

sub for_every_unpriced_cdr {
	my ($lim, $callback) = @_;
	my $dbh = $lim->get_database_handle();

	my $sth = $dbh->prepare("SELECT * FROM cdr WHERE pricing_id IS NULL ORDER BY time ASC");
	$sth->execute() or die "Query failed";
	while(my $cdr = $sth->fetchrow_hashref()) {
		$callback->($lim, $cdr);
	}
}

=head3 price_cdr($lim, $cdr)

Try to price a CDR. Updates its pricing_id, computed_price and computed_cost,
and adds computational details into the pricing_info field. Does not write into
the database, use 'write_cdr_pricing' for that.

This method uses temporal information from various tables (speakup_account,
account, sim) -- it takes information from the time the CDR was formed. This
means if a SIM changed settings halfway through the month, CDRs before that
moment will be priced using the old settings, and after that moment will be
priced using the new settings.

If a CDR cannot be priced, an error is thrown whose text contains 'unpricable',
and some explanation as to why this particular CDR is unpricable.

=cut

sub price_cdr {
	my ($lim, $cdr) = @_;
	my $dbh = $lim->get_database_handle();

	# A CDR is unpricable if its externalAccount is unmatched
	my $sth = $dbh->prepare("SELECT account_id FROM speakup_account WHERE lower(name)=lower(?) AND period @> ?::date");
	$sth->execute($cdr->{'speakup_account'}, $cdr->{'time'});
	my $account_id = $sth->fetchrow_arrayref();
	if(!$account_id || !$account_id->[0]) {
		die sprintf("This CDR is unpricable: its speakup_account, %s, is not matched to a Limesco account", $cdr->{'speakup_account'});
	}
	if($sth->fetchrow_arrayref()) {
		die sprintf("This CDR is unpricable: its speakup_account, %s, matches to multiple Limesco accounts", $cdr->{'speakup_account'});
	}
	$account_id = $account_id->[0];

	# A CDR is unpricable if its phone number has no matching SIM
	my $is_in = $cdr->{'direction'} && $cdr->{'direction'} eq "IN";
	my $phone = $is_in ? $cdr->{'to'} : $cdr->{'from'};
	$phone =~ s/-//;
	if($phone =~ /^0?6(\d{8})$/) {
		$phone = "316$1";
	}

	# Try to fetch corresponding SIM information. If this succeeds, try to find the correct
	# pricing rule using the found SIM. If not, try to find a pricing rule regardless of SIM.
	# If we can't find a SIM and only one pricing rule matches, we can use it anyway; if an
	# ambiguous situation appears without a SIM (e.g. a pricing rule for OOTB and a different
	# one for DIY) the CDR will be unpricable anyway.
	$sth = $dbh->prepare("SELECT * FROM sim WHERE owner_account_id=? AND period @> ?::date AND iccid=(SELECT sim_iccid FROM phonenumber WHERE phonenumber.phonenumber=? AND period @> ?::date)");
	$sth->execute($account_id, $cdr->{'time'}, $phone, $cdr->{'time'});
	my $sim = $sth->fetchrow_hashref();
	my $unpricable_error;
	if(!$sim) {
		$unpricable_error = sprintf("This CDR is unpricable: its phone number, %s, does not belong to a SIM card within speakup_account %s", $phone, $cdr->{'speakup_account'});
	}
	if($sim && $sth->fetchrow_hashref()) {
		$unpricable_error = sprintf("This CDR is unpricable: its phone number, %s, matches to multiple SIM cards within speakup_account %s", $phone, $cdr->{'speakup_account'});
		undef $sim;
	}

	# Enter monstruous query to find all pricing rules that match this CDR
	my $sim_specific_where = $sim ? "AND constraint_list_matches(?, call_connectivity_type::text[])" : "";
	my $sim_specific_variables = $sim ? [$sim->{'call_connectivity_type'}] : [];

	my $query = "SELECT id, description, cost_per_line, cost_per_unit, price_per_line, price_per_unit "
		."FROM pricing WHERE service=? AND period @> ?::date AND constraint_list_matches(?, source::text[]) "
		."AND constraint_list_matches(?, destination::text[]) AND constraint_list_matches(?::text, direction::text[]) "
		."AND constraint_list_matches(?::boolean::text, connected::text[]) " . $sim_specific_where . ";";
	my @variables = ($cdr->{'service'}, $cdr->{'time'}, $cdr->{'source'}, $cdr->{'destination'}, $cdr->{'direction'}, $cdr->{'connected'},
		@$sim_specific_variables);
	$sth = $dbh->prepare($query);
	$sth->execute(@variables);
	my $pricing_rule = $sth->fetchrow_hashref();
	if(!$pricing_rule) {
		my $i = 0;
		my $variables = "";
		foreach(@variables) {
			$variables .= "  " . (++$i) . ": ";
			$variables .= defined $_ ? "'$_'\n" : "(undef)\n";
		}
		die sprintf("This CDR is unpricable: no pricing rule could be found for this CDR.\nQuery was: %s\nVariables were:\n%s", $query, $variables);
	}
	if($sth->fetchrow_hashref()) {
		if($unpricable_error) {
			# Probably, multiple pricing rules were found because they depend on SIM specific information
			# but we have no SIM card to take this information from. Throw that error.
			die $unpricable_error;
		} else {
			die sprintf("This CDR is unpricable: multiple pricing rules could be found for this CDR");
		}
	}

	$cdr->{'computed_price'} = $pricing_rule->{'price_per_line'} + $cdr->{'units'} * $pricing_rule->{'price_per_unit'};
	$cdr->{'computed_cost'} = $pricing_rule->{'cost_per_line'} + $cdr->{'units'} * $pricing_rule->{'cost_per_unit'};
	$cdr->{'pricing_id'} = $pricing_rule->{'id'};
	$cdr->{'pricing_info'} = {
		description => $pricing_rule->{'description'},
		iccid => $sim->{'iccid'},
	};
}

=head3 write_cdr_pricing($lim, $cdr)

Write a CDR's pricing information into the database.

=cut

sub write_cdr_pricing {
	my ($lim, $cdr) = @_;
	my $dbh = $lim->get_database_handle();
	$dbh->do("UPDATE cdr SET pricing_id=?, pricing_info=?, computed_cost=?, computed_price=? WHERE id=?",
		undef, $cdr->{'pricing_id'}, encode_json($cdr->{'pricing_info'}), $cdr->{'computed_cost'}, $cdr->{'computed_price'}, $cdr->{'id'})
		or die "Query failed";
}

=head3 dump_cdr($cdr)

Return a single-line string with some information that identifies a CDR.

=cut

sub dump_cdr {
	my ($cdr) = @_;
	return sprintf("ID %d, Call ID %s, SU account %s, date %s", $cdr->{'id'}, $cdr->{'call_id'}, $cdr->{'speakup_account'}, $cdr->{'time'});
}

1;
