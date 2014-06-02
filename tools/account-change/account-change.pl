#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

do 'change-support.pm';

=head1 account-change.pl

Usage: account-change.pl [no CLI options available yet]

This file contains methods to create accounts, change their properties and
delete them, in the temporal fashion that the database uses.

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

# The fields without which an account may not be created. This should
# correspond to NOT NULL fields without a DEFAULT in the account table.
sub _account_required_fields {
	return qw(first_name last_name street_address postal_code city email state);
}

# The fields which can be automatically filled in by the database. This list
# should correspond to either NULL fields or those with a DEFAULT in the
# account table.
sub _account_optional_fields {
	return qw(company_name password_hash admin);
}

# The field information used by change-support to handle temporal account
# changes
sub _account_object_info {
	return {
		required_fields => [_account_required_fields()],
		optional_fields => [_account_optional_fields()],
	};
}

=head3 create_account($lim, $account, [$date])

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

=head3 get_account($lim, $account_id, [$date])

Retrieve an account. If $date is given, retrieve an account on the given
date.

=cut

sub get_account {
	my ($lim, $account_id, $date) = @_;
	return $lim->get_account($account_id, $date);
}

=head3 update_account($lim, $account_id, $changes, [$date])

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

=head3 delete_account($lim, $account_id, [$date])

Delete an account. $date is the optional date of deletion; if not given,
'today' is assumed.

=cut

sub delete_account {
	my ($lim, $account_id, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	$dbh->begin_work;

	try {
		$dbh->do("LOCK TABLE account;");
		my $sth = $dbh->prepare("SELECT id, period, lower(period) AS old_date, ?::date AS new_date FROM account WHERE id=? AND upper(period) IS NULL AND period @> ?::date");
		$sth->execute($date, $account_id, $date);
		my $old_account = $sth->fetchrow_hashref;
		if(!$old_account) {
			die "Cannout change account $account_id at date $date, doesn't exist or it is already historical";
		}

		# If the new date overwrites the last period, delete the row, otherwise update it
		my $changed_rows;
		if($old_account->{'old_date'} eq $old_account->{'new_date'}) {
			my $sth = $dbh->prepare("DELETE FROM account WHERE id=? AND period=?");
			$changed_rows = $sth->execute($old_account->{'id'}, $old_account->{'period'});
		} else {
			my $sth = $dbh->prepare("UPDATE account SET period=daterange(lower(period), ?) WHERE id=? AND period=?");
			$changed_rows = $sth->execute($date, $old_account->{id}, $old_account->{period});
		}

		if(!$changed_rows) {
			die "Failed to delete account $account_id, even though it existed";
		}

		$dbh->commit;
	} catch {
		$dbh->rollback;
		die $_;
	};
}

1;
