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

	my $sth = $dbh->prepare("SELECT * FROM cdr WHERE pricing_info IS NULL");
	$sth->execute() or die "Query failed";
	while(my $cdr = $sth->fetchrow_hashref()) {
		$callback->($lim, $cdr);
	}
}

=head3 price_cdr($lim, $cdr)

Try to price a CDR. Updates its computed_price and computed_cost, and adds
computational details into the pricing_info field. Does not write into the
database, use 'write_cdr_pricing' for that.

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

	$sth = $dbh->prepare("SELECT * FROM sim WHERE owner_account_id=? AND period @> ?::date AND iccid=(SELECT sim_iccid FROM phonenumber WHERE phonenumber.phonenumber=? AND period @> ?::date)");
	$sth->execute($account_id, $cdr->{'time'}, $phone, $cdr->{'time'});
	my $sim = $sth->fetchrow_hashref();
	if(!$sim) {
		die sprintf("This CDR is unpricable: its phone number, %s, does not belong to a SIM card within speakup_account %s", $phone, $cdr->{'speakup_account'});
	}
	if($sth->fetchrow_hashref()) {
		die sprintf("This CDR is unpricable: its phone number, %s, matches to multiple SIM cards within speakup_account %s", $phone, $cdr->{'speakup_account'});
	}

	if($sim->{'state'} ne "ALLOCATED" && $sim->{'state'} ne "ACTIVATION_REQUESTED" && $sim->{'state'} ne "ACTIVATED") {
		die sprintf("This CDR is unpricable: it belongs to SIM with ICCID %s, but that SIM is in %s state", $sim->{'iccid'}, $sim->{'state'});
	}

	my $callConnectivityType = $sim->{'call_connectivity_type'};

	# Enter monstruous query to find all pricing rules that match this CDR
	$sth = $dbh->prepare("SELECT id, description, cost_per_line, cost_per_unit, price_per_line, price_per_unit "
		."FROM pricing WHERE service=? AND period @> ?::date AND constraint_list_matches(?, source::text[]) "
		."AND constraint_list_matches(?, destination::text[]) AND constraint_list_matches(?::text, direction::text[]) "
		."AND constraint_list_matches(?, call_connectivity_type::text[]) AND constraint_list_matches(?::boolean::text, connected::text[]);";
	$sth->execute($cdr->{'service'}, $cdr->{'time'}, $cdr->{'source'}, $cdr->{'destination'}, $cdr->{'direction'}, $callConnectivityType, $cdr->{'connected'});
	my $pricing_rule = $sth->fetchrow_hashref();
	if(!$pricing_rule) {
		die sprintf("This CDR is unpricable: no pricing rule could be found for this CDR");
	}
	if($sth->fetchrow_hashref()) {
		die sprintf("This CDR is unpricable: multiple pricing rules could be found for this CDR");
	}

	$cdr->{'computed_price'} = $pricing_rule->{'price_per_line'} + $cdr->{'units'} * $pricing_rule->{'price_per_unit'};
	$cdr->{'computed_cost'} = $pricing_rule->{'cost_per_line'} + $cdr->{'units'} * $pricing_rule->{'cost_per_unit'};
	$cdr->{'pricing_info'} = {
		pricing_rule => $pricing_rule->{'id'},
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
	$dbh->do("UPDATE cdr SET pricing_info=?, computed_cost=?, computed_price=? WHERE id=?",
		undef, encode_json($cdr->{'pricing_info'}), $cdr->{'computed_cost'}, $cdr->{'computed_price'})
		or die "Query failed";
}

=head3 dump_cdr($cdr)

Return a single-line string with some information that identifies a CDR.

=cut

sub dump_cdr {
	my ($cdr) = @_;
	return sprintf("ID %d, Call ID %s, date %s", $cdr->{'id'}, $cdr->{'call_id'}, $cdr->{'time'});
}

1;
