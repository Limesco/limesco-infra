#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::TemporalSupport;
use Try::Tiny;

=head1 sim-change.pl

Usage: sim-change.pl [no CLI options available yet]

This file contains methods to create SIMs, change their properties and delete
them, in the temporal fashion that the database uses. See the documentation for
Limesco::TemporalSupport for more information.

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--export") {
			print $args->[++$$iref];
		} else {
			return 0;
		}
	});
}

=head2 Methods

=cut

# The field information used by change-support to handle temporal SIM
# changes
sub _sim_object_info {
	return {
		# The fields without which a SIM may not be created. This should
		# correspond to NOT NULL fields without a DEFAULT in the SIM table.
		required_fields => [qw(iccid state puk)],
		# The fields which can be automatically filled in by the database. This list
		# should correspond to either NULL fields or those with a DEFAULT in the
		# sim table.
		optional_fields => [qw(owner_account_id data_type exempt_from_cost_contribution
			porting_state activation_invoice_id last_monthly_fees_invoice_id
			last_monthly_fees_month call_connectivity_type sip_realm sip_username
			sip_authentication_username sip_password sip_uri sip_expiry sip_trunk_password)],
		table_name => "sim",
		primary_key => "iccid",
	};
}

# and the same for temporal phone number changes
sub _phonenumber_object_info {
	return {
		# The fields without which a phone number may not be created
		required_fields => [qw(phonenumber sim_iccid)],
		table_name => "phonenumber",
		primary_key => "phonenumber",
	};
}

=head3 create_sim($lim | $dbh, $sim, [$date])

Create a SIM. $date is the optional starting date of the SIM; if not given,
'today' is assumed. $sim must contain all required fields (e.g. iccid and
state), must contain all state-required fields (e.g. data_type if the state is
not 'STOCK'), may contain optional fields (e.g. sip_realm) and must contain
nothing else.

This method returns the newly created SIM, or throws an exception if
something failed.

=cut

sub create_sim {
	my ($lim, $sim, $date) = @_;
	return create_object($lim, _sim_object_info(), $sim, $date);
}

=head3 get_sim($lim | $dbh, $sim_iccid, [$date])

Retrieve a SIM. If $date is given, retrieve a SIM on the given date.

=cut

sub get_sim {
	my ($lim, $sim_id, $date) = @_;
	return get_object($lim, _sim_object_info(), $sim_id, $date);
}

=head3 list_sims($lim | $dbh, [$date])

Retrieve a list of SIMs active on the given $date. If $date is not given,
only SIMs active 'today' are returned.

=cut

sub list_sims {
	my ($lim, $date) = @_;
	return list_objects($lim, _sim_object_info(), $date);
}

=head3 update_sim($lim | $dbh, $sim_iccid, $changes, [$date])

Update a SIM. $date is the optional date of the changes; if not given, 'today'
is assumed. $sim_iccid is the ICCID of the SIM to change, $changes is a hashref
of wanted changes. If any of the given changes is impossible, the full update
is aborted and an exception is thrown. $changes must only contain fields that
are either required or optional for a SIM.

This method returns the updated SIM, or throws an exception if something
failed.

=cut

sub update_sim {
	my ($lim, $sim_iccid, $changes, $date) = @_;
	return update_object($lim, _sim_object_info(), $sim_iccid, $changes, $date);
}

=head3 delete_sim($lim | $dbh, $sim_iccid, [$date])

Delete a SIM. $date is the optional date of deletion; if not given, 'today' is
assumed.

=cut

sub delete_sim {
	my ($lim, $sim_iccid, $date) = @_;
	delete_object($lim, _sim_object_info(), $sim_iccid, $date);
}

=head3 sim_changes_between($lim | $dbh, $sim_iccid, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given SIM before 2014-03-01, including changes done on
2014-03-01.

=cut

sub sim_changes_between {
	my ($lim, $sim_iccid, $startdate, $enddate) = @_;
	object_changes_between($lim, _sim_object_info(), $sim_iccid, $startdate, $enddate);
}

=head3 normalize_phonenumber($phonenumber)

Take a free-form phonenumber, and return a normalized one. If it's not a
correct phonenumber, die().

=cut

sub normalize_phonenumber {
	my ($phonenumber) = @_;
	$phonenumber =~ s/[- ]+//g;
	if($phonenumber =~ /^(?:\+?31|\+?0031|0)6(\d{8})$/) {
		return "316$1";
	} else {
		die "Did not recognize phone number: $phonenumber";
	}
}

=head3 create_phonenumber($lim | $dbh, $phonenumber, $sim_iccid, [$date])

Create a phone number. $date is the optional starting date of the SIM; if not
given, 'today' is assumed. $phonenumber must start with '316' and $sim_iccid
must point to a valid SIM at the given date.

This method returns the newly created phone number, or throws an exception if
something failed.

=cut

sub create_phonenumber {
	my ($lim, $phonenumber, $sim_iccid, $date) = @_;
	$phonenumber = normalize_phonenumber($phonenumber);
	return create_object($lim, _phonenumber_object_info(),
		{phonenumber => $phonenumber, sim_iccid => $sim_iccid},
		$date);
}

=head3 get_phonenumber($lim | $dbh, $phonenumber, [$date])

Retrieve phone number information. If $date is given, retrieve phone number
information on the given date.

=cut

sub get_phonenumber {
	my ($lim, $phonenumber, $date) = @_;
	$phonenumber = normalize_phonenumber($phonenumber);
	return get_object($lim, _phonenumber_object_info(), $phonenumber, $date);
}

=head3 list_phonenumbers($lim | $dbh, [$sim_iccid, [$date]])

Retrieve a list of phone numbers active on the given $date. If $date is not
given, only phone numbers active 'today' are returned. If $sim_iccid (a string
that looks like an ICCID) is given, filter out phone numbers belonging to the
given SIM.

=cut

sub list_phonenumbers {
	my ($lim, $arg1, $arg2) = @_;
	my ($iccid, $date);
	if($arg1) {
		if($arg1 =~ /^20\d\d-\d\d-\d\d$/ || $arg1 eq "today") {
			$date = $arg1;
		} elsif($arg1 =~ /^89310\d{13}\d{0,2}$/) {
			$iccid = $arg1;
		} else {
			die "Didn't understand parameter: $arg1\n";
		}
	}
	if($arg2) {
		if(!$date && ($arg2 =~ /^20\d\d-\d\d-\d\d$/ || $arg2 eq "today")) {
			$date = $arg2;
		} else {
			die "Didn't understand parameter: $arg2\n";
		}
	}
	# TODO: add a WHERE clause
	my @objects = list_objects($lim, _phonenumber_object_info(), $date);
	if($iccid) {
		return grep { $_->{'sim_iccid'} eq $iccid } @objects;
	} else {
		return @objects;
	}
}

=head3 delete_phonenumber($lim | $dbh, $phonenumber, [$date])

Delete a phone number from a SIM. $date is the optional date of deletion; if
not given, 'today' is assumed. This method does not check what ICCID a phone
number belongs to: you must check this yourself.

=cut

sub delete_phonenumber {
	my ($lim, $phonenumber, $date) = @_;
	$phonenumber = normalize_phonenumber($phonenumber);
	delete_object($lim, _phonenumber_object_info(), $phonenumber, $date);
}

=head3 phonenumber_changes_between($lim | $dbh, $phonenumber, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given phonenumber before 2014-03-01, including changes done on
2014-03-01.

=cut

sub phonenumber_changes_between {
	my ($lim, $phonenumber, $startdate, $enddate) = @_;
	$phonenumber = normalize_phonenumber($phonenumber);
	object_changes_between($lim, _phonenumber_object_info(), $phonenumber, $startdate, $enddate);
}

1;
