#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use JSON;

=head1 cdr-pricing.pl

Usage: cdr-pricing.pl [infra-options]

* retrieve all unpriced CDRs
* for each cdr, get the relevant SIM and account
	account by speakup_account, SIM by phone number
* try to find an applicable pricing rule for that cdr
* compute price and cost and store them in the CDR table

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		#if($arg eq "--account") {
		#	$account_id = $args->[++$$iref];
		#} elsif($arg eq "--date") {
		#	$date = $args->[++$$iref];
		if(0) {
		} else {
			return 0;
		}
	});

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

If a CDR cannot be priced, an error is thrown whose text contains 'unpricable'.

=cut

sub price_cdr {
	my ($lim, $cdr) = @_;
	die "All CDRs are unpricable";
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
	return sprintf("ID %d, Call ID %s, date %s %s", $cdr->{'id'}, $cdr->{'call_id'}, $cdr->{'time'}->ymd, $cdr->{'time'}->hms);
}

1;
