#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::TemporalSupport;
use Try::Tiny;

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

=head2 Methods

=cut

# The field information used by change-support to handle temporal account
# changes
sub _account_object_info {
	return {
		# The fields without which an account may not be created. This should
		# correspond to NOT NULL fields without a DEFAULT in the account table.
		required_fields => [qw(first_name last_name street_address postal_code city email state)],
		# The fields which can be automatically filled in by the database. This list
		# should correspond to either NULL fields or those with a DEFAULT in the
		# account table.
		optional_fields => [qw(company_name password_hash admin)],
		table_name => "account",
		primary_key => "id",
		primary_key_seq => "account_id_seq",
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

=head3 delete_account($lim | $dbh, $account_id, [$date])

Delete an account. $date is the optional date of deletion; if not given,
'today' is assumed.

=cut

sub delete_account {
	my ($lim, $account_id, $date) = @_;
	delete_object($lim, _account_object_info(), $account_id, $date);
}

1;
