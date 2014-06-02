#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

=head3 create_object($lim, $object_info, $account, [$date])

Create an account. $date is the optional starting date of the account; if not
given, 'today' is assumed. $account must contain all required fields (e.g.
first_name and street_address), may contain zero or more of the optional
fields (e.g. company_name), and may not contain any other fields.

This method returns the newly created account, or throws an exception if
something failed.

=cut

sub create_object {
	my ($lim, $object_info, $account, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	my @db_fields;
	my @db_values;

	foreach(@{$object_info->{'required_fields'}}) {
		if(!exists($account->{$_}) || length($account->{$_}) == 0) {
			die "Required account field $_ is missing in create_account";
		}
		push @db_fields, $_;
		push @db_values, delete $account->{$_};
	}

	foreach(@{$object_info->{'optional_fields'}}) {
		if(exists($account->{$_})) {
			push @db_fields, $_;
			push @db_values, delete $account->{$_};
		}
	}

	foreach(keys %$account) {
		die "Unknown account field $_ in create_account\n";
	}

	unshift @db_fields, "period";
	unshift @db_values, '['.$date.',)';

	my $query = "INSERT INTO account (id, " . join(", ", @db_fields) . ")";
	$query .= " VALUES (NEXTVAL('account_id_seq'), " . join (", ", (('?') x @db_fields)) . ")";

	my $sth = $dbh->prepare($query);
	$sth->execute(@db_values);

	my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});
	return get_account($lim, $account_id, $date);
}

=head3 update_object($lim, $object_info, $account_id, $changes, [$date])

Update an account. $date is the optional date of the changes; if not given,
'today' is assumed. $account_id is the ID of the account to change, $changes
is a hashref of wanted changes. If any of the given changes is impossible, the
full update is aborted and an exception is thrown. $changes must only contain
fields that are either required or optional for an account.

This method returns the updated account, or throws an exception if something
failed.

=cut

sub update_object {
	my ($lim, $object_info, $account_id, $changes, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	$dbh->begin_work;

	try {
		$dbh->do("LOCK TABLE account;");

		my $sth = $dbh->prepare("SELECT *, lower(period) AS old_date, ?::date AS new_date FROM account WHERE id=? AND upper(period) IS NULL AND period @> ?::date");
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
			die "Failed to change account $account_id, even though it existed";
		}

		my @db_fields;
		my @db_values;

		foreach(@{$object_info->{'required_fields'}}) {
			push @db_fields, $_;
			if(!exists($changes->{$_}) || length($changes->{$_}) == 0) {
				push @db_values, $old_account->{$_};
			} else {
				push @db_values, delete $changes->{$_};
			}
		}

		foreach(@{$object_info->{'optional_fields'}}) {
			push @db_fields, $_;
			if(exists($changes->{$_})) {
				push @db_values, delete $changes->{$_};
			} else {
				push @db_values, $old_account->{$_};
			}
		}

		foreach(keys %$changes) {
			die "Unknown account field $_ in update_account\n";
		}

		unshift @db_fields, "period";
		unshift @db_values, '['.$date.',)';

		unshift @db_fields, "id";
		unshift @db_values, $account_id;

		my $query = "INSERT INTO account (" . join(", ", @db_fields) . ")";
		$query .= " VALUES (" . join (", ", (('?') x @db_fields)) . ")";

		$sth = $dbh->prepare($query);
		$sth->execute(@db_values);
		$dbh->commit;
		return get_account($lim, $account_id, $date);
	} catch {
		$dbh->rollback;
		die $_;
	};
}

=head3 delete_object($lim, $object_info, $account_id, [$date])

Delete an account. $date is the optional date of deletion; if not given,
'today' is assumed.

=cut

sub delete_object {
	my ($lim, $object_info, $account_id, $date) = @_;
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
