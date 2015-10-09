#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;

=head1 account-change.pl

Usage: account-change.pl [no CLI options available yet]

This file contains methods to create accounts, change their properties and
delete them, in the temporal fashion that the database uses. See the
documentation for Limesco::TemporalSupport for more information.

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

package Limesco::AccountChange;
use strict;
use warnings;
no warnings 'redefine';
use Limesco::TemporalSupport;
use Try::Tiny;
use Exporter;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(create_account get_account list_accounts update_account
	delete_account account_changes_between link_speakup_account
	get_speakup_account list_speakup_accounts unlink_speakup_account
	speakup_account_changes_between);

=head2 Methods

=cut

# The field information used by change-support to handle temporal account
# changes
sub _account_object_info {
	return {
		# The fields without which an account may not be created. This should
		# correspond to NOT NULL fields without a DEFAULT in the account table.
		required_fields => [qw(first_name last_name street_address postal_code city email contribution)],
		# The fields which can be automatically filled in by the database. This list
		# should correspond to either NULL fields or those with a DEFAULT in the
		# account table.
		optional_fields => [qw(company_name password_hash admin last_contribution_month)],
		table_name => "account",
		primary_key => "id",
		primary_key_seq => "account_id_seq",
	};
}

# and the same for temporal speakup account changes
sub _speakup_account_object_info {
	return {
		# The fields without which a speakup account may not be created
		required_fields => [qw(name account_id)],
		table_name => "speakup_account",
		primary_key => "name",
	};
}

=head3 create_account($lim | $dbh, $account, [$date])

Create an account. $date is the optional starting date of the account; if not
given, 'today' is assumed. $account must contain all required fields (e.g.
first_name and street_address), may contain zero or more of the optional
fields (e.g. company_name), and may not contain any other fields.

This method returns the newly created account, or throws an exception if
something failed.

=cut

sub create_account {
	my ($lim, $account, $date) = @_;
	return create_object($lim, _account_object_info(), $account, $date);
}

=head3 get_account($lim | $dbh, $account_id, [$date])

Retrieve an account. If $date is given, retrieve an account on the given
date.

=cut

sub get_account {
	my ($lim, $account_id, $date) = @_;
	return get_object($lim, _account_object_info(), $account_id, $date);
}

=head3 list_accounts($lim | $dbh, [$date])

Retrieve a list of accounts active on the given $date. If $date is not given,
only accounts active 'today' are returned.

=cut

sub list_accounts {
	my ($lim, $date) = @_;
	return list_objects($lim, _account_object_info(), $date);
}

=head3 update_account($lim | $dbh, $account_id, $changes, [$date])

Update an account. $date is the optional date of the changes; if not given,
'today' is assumed. $account_id is the ID of the account to change, $changes
is a hashref of wanted changes. If any of the given changes is impossible, the
full update is aborted and an exception is thrown. $changes must only contain
fields that are either required or optional for an account.

This method returns the updated account, or throws an exception if something
failed.

=cut

sub update_account {
	my ($lim, $account_id, $changes, $date) = @_;
	return update_object($lim, _account_object_info(), $account_id, $changes, $date);
}

=head3 delete_account($lim | $dbh, $account_id, [$date, [$force]])

Delete an account. $date is the optional date of deletion; if not given,
'today' is assumed. If $force is true, allow deleting the account even though
$date is a historic record (i.e. delete future changes too).

=cut

sub delete_account {
	my ($lim, $account_id, $date, $force) = @_;
	delete_object($lim, _account_object_info(), $account_id, $date, $force);
}

=head3 account_changes_between($lim | $dbh, $account_id, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given account ID before 2014-03-01, including changes done on
2014-03-01.

=cut

sub account_changes_between {
	my ($lim, $account_id, $startdate, $enddate) = @_;
	object_changes_between($lim, _account_object_info(), $account_id, $startdate, $enddate);
}

=head3 link_speakup_account($lim | $dbh, $speakup_account, $account_id, [$date])

Link a speakup account to a liminfra account. $date is the optional starting
date of the link; if not given, 'today' is assumed. $account_id must point to a
valid account at the given date.

This method returns the newly created speakup account, or throws an exception
if something failed.

=cut

sub link_speakup_account {
	my ($lim, $speakup_account, $account_id, $date) = @_;
	return create_object($lim, _speakup_account_object_info(),
		{name => $speakup_account, account_id => $account_id},
		$date);
}

=head3 get_speakup_account($lim | $dbh, $speakup_account, [$date])

Retrieve speakup account information. If $date is given, retrieve speakup
account information on the given date.

=cut

sub get_speakup_account {
	my ($lim, $speakup_account, $date) = @_;
	return get_object($lim, _speakup_account_object_info(), $speakup_account, $date);
}

=head3 list_speakup_accounts($lim | $dbh, [$account_id, [$date]])

Retrieve a list of speakup accounts active on the given $date. If $date is not
given, only speakup accounts active 'today' are returned. If an $account_id is
given, filter out speakup accounts belonging to the given account.

=cut

sub list_speakup_accounts {
	my ($lim, $arg1, $arg2) = @_;
	my ($id, $date);
	if($arg1) {
		if($arg1 =~ /^20\d\d-\d\d-\d\d$/ || $arg1 eq "today") {
			$date = $arg1;
		} elsif($arg1 =~ /^\d+$/) {
			$id = $arg1;
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
	my @objects = list_objects($lim, _speakup_account_object_info(), $date);
	if($id) {
		return grep { $_->{'account_id'} eq $id } @objects;
	} else {
		return @objects;
	}
}

=head3 unlink_speakup_account($lim | $dbh, $speakup_account, [$date, [$force]])

Unlink a speakup account from an account. $date is the optional date of
unlinking; if not given, 'today' is assumed.  If $force is true, allow deleting
the speakup account even though $date is a historic record (i.e. delete future
changes too).  This method does not check what account ID a speakup account
belongs to: you must check this yourself.

=cut

sub unlink_speakup_account {
	my ($lim, $speakup_account, $date, $force) = @_;
	delete_object($lim, _speakup_account_object_info(), $speakup_account, $date, $force);
}

=head3 speakup_account_changes_between($lim | $dbh, $speakup_account, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given speakup_account before 2014-03-01, including changes done
on 2014-03-01.

=cut

sub speakup_account_changes_between {
	my ($lim, $speakup_account, $startdate, $enddate) = @_;
	object_changes_between($lim, _speakup_account_object_info(), $speakup_account, $startdate, $enddate);
}

1;
