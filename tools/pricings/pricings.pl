#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::TemporalSupport;
use Try::Tiny;
use v5.14; # Unicode string features
use open qw( :encoding(UTF-8) :std);

=head1 pricings.pl

Usage: pricings.pl [--service <service>]

=cut

if(!caller) {
	my $service;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--service") {
			$service = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	my @pricings = list_pricings($lim);
	foreach(@pricings) {
		if($service && $_->{'service'} ne uc($service)) {
			next;
		}
		print pricing_to_string($lim, $_);
	}
}

=head2 Methods

=cut

# The field information used by change-support to handle temporal pricing
# changes
sub _pricing_object_info {
	return {
		required_fields => [qw(description hidden service
			call_connectivity_type source destination direction
			connected cost_per_line cost_per_unit price_per_line
			price_per_unit legreason)],
		optional_fields => [],
		table_name => "pricing",
		primary_key => "id",
		primary_key_seq => "pricing_id_seq",
	};
}

=head3 create_pricing($lim | $dbh, $pricing, [$date])

=cut

sub create_pricing {
	my ($lim, $pricing, $date) = @_;
	return create_object($lim, _pricing_object_info(), $pricing, $date);
}

=head3 get_pricing($lim | $dbh, $pricing_id, [$date])

Retrieve an pricing. If $date is given, retrieve an pricing on the given
date.

=cut

sub get_pricing {
	my ($lim, $pricing_id, $date) = @_;
	return get_object($lim, _pricing_object_info(), $pricing_id, $date);
}

=head3 list_pricings($lim | $dbh, [$date])

Retrieve a list of pricings active on the given $date. If $date is not given,
only pricings active 'today' are returned.

=cut

sub list_pricings {
	my ($lim, $date) = @_;
	return list_objects($lim, _pricing_object_info(), $date);
}

=head3 update_pricing($lim | $dbh, $pricing_id, $changes, [$date])

Update an pricing. $date is the optional date of the changes; if not given,
'today' is assumed. $pricing_id is the ID of the pricing to change, $changes
is a hashref of wanted changes. If any of the given changes is impossible, the
full update is aborted and an exception is thrown. $changes must only contain
fields that are either required or optional for an pricing.

This method returns the updated pricing, or throws an exception if something
failed.

=cut

sub update_pricing {
	my ($lim, $pricing_id, $changes, $date) = @_;
	return update_object($lim, _pricing_object_info(), $pricing_id, $changes, $date);
}

=head3 delete_pricing($lim | $dbh, $pricing_id, [$date, [$force]])

Delete an pricing. $date is the optional date of deletion; if not given,
'today' is assumed. If $force is true, allow deleting the pricing even though
$date is a historic record (i.e. delete future changes too).

=cut

sub delete_pricing {
	my ($lim, $pricing_id, $date, $force) = @_;
	delete_object($lim, _pricing_object_info(), $pricing_id, $date, $force);
}

=head3 pricing_changes_between($lim | $dbh, $pricing_id, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given pricing ID before 2014-03-01, including changes done on
2014-03-01.

=cut

sub pricing_changes_between {
	my ($lim, $pricing_id, $startdate, $enddate) = @_;
	object_changes_between($lim, _pricing_object_info(), $pricing_id, $startdate, $enddate);
}

=head3 pricing_to_string($lim, $pricing)

=cut

sub pricing_to_string {
	my ($lim, $pricing) = @_;

	my $str = sprintf("%s pricing %04d: %s\n", $pricing->{'service'}, $pricing->{'id'}, $pricing->{'description'});
	$str   .= sprintf("  Period: %s\n", $pricing->{'period'});
	$str   .= sprintf("  For service: %s\n", $pricing->{'service'});
	$str   .= sprintf("  Is hidden\n") if($pricing->{'hidden'});

	my %constraints = (
		call_connectivity_type => "call connectivity type",
		source => "source",
		destination => "destination",
		direction => "direction",
		connected => "connected",
		legreason => "leg reason",
	);
	foreach my $c (keys %constraints) {
		my $v = $pricing->{$c};
		if(ref($v) eq "ARRAY" && @$v == 0) {
			# empty constraint, matches all
		} elsif($v eq "{}") {
			# empty constraint non-arraytype, matches all
		} else {
			$v = '{' . join(",", @$v) . '}' if ref($v) eq "ARRAY";
			$str .= sprintf("    Only if %s in: %s\n", $constraints{$c}, $v);
		}
	}
	$str .= sprintf("  Cost : %.08f per line, %.08f per unit\n", $pricing->{'cost_per_line'}, $pricing->{'cost_per_unit'});
	$str .= sprintf("  Price: %.08f per line, %.08f per unit\n", $pricing->{'price_per_line'}, $pricing->{'price_per_unit'});
	return $str;
}

1;
