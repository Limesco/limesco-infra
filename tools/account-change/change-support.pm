#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

=head3 get_object($lim, $object_info, $object_id, [$date])

Retrieve an object. $date is the optional date of interest; if not given,
'today' is assumed. $object_id is the value of the primary key of the row in
which we are interested.

=cut

sub get_object {
	my ($lim, $object_info, $object_id, $date) = @_;
	$date ||= 'today';
	my $table_name = $object_info->{'table_name'};
	my $primary_key = $object_info->{'primary_key'};

	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM $table_name WHERE $primary_key=? AND period @> ?::date");
	$sth->execute($object_id, $date);
	my $object = $sth->fetchrow_hashref;
	if(!$object) {
		die "No such object with ID $object_id at date $date";
	}
	return $object;
}

=head3 create_object($lim, $object_info, $object, [$date])

Create an object. $date is the optional starting date of the object; if not
given, 'today' is assumed. $object must contain all required fields from the
$object_info (such as first_name and street_address for an account), may
contain zero or more of the optional fields (such as company_name), and may not
contain any other fields.

This method returns the newly created object, or throws an exception if
something failed.

=cut

sub create_object {
	my ($lim, $object_info, $object, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	my @db_fields;
	my @db_values;

	foreach(@{$object_info->{'required_fields'}}) {
		if(!exists($object->{$_}) || length($object->{$_}) == 0) {
			die "Required object field $_ is missing in create_object";
		}
		push @db_fields, $_;
		push @db_values, delete $object->{$_};
	}

	foreach(@{$object_info->{'optional_fields'}}) {
		if(exists($object->{$_})) {
			push @db_fields, $_;
			push @db_values, delete $object->{$_};
		}
	}

	foreach(keys %$object) {
		die "Unknown object field $_ in create_object\n";
	}

	unshift @db_fields, "period";
	unshift @db_values, '['.$date.',)';

	my $table_name = $object_info->{'table_name'};
	my $primary_key = $object_info->{'primary_key'};
	my $primary_key_seq = $object_info->{'primary_key_seq'};

	my $query = "INSERT INTO $table_name ($primary_key, " . join(", ", @db_fields) . ")";
	$query .= " VALUES (NEXTVAL('$primary_key_seq'), " . join (", ", (('?') x @db_fields)) . ")";

	my $sth = $dbh->prepare($query);
	$sth->execute(@db_values);

	my $object_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => $primary_key_seq});
	return get_object($lim, $object_info, $object_id, $date);
}

=head3 update_object($lim, $object_info, $object_id, $changes, [$date])

Update an object. $date is the optional date of the changes; if not given,
'today' is assumed. $object_id is the primary key value of the object to
change, $changes is a hashref of wanted changes. If any of the given changes is
impossible, the full update is aborted and an exception is thrown. $changes
must only contain fields that are either required or optional for an object.

This method returns the updated object, or throws an exception if something
failed.

=cut

sub update_object {
	my ($lim, $object_info, $object_id, $changes, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	$dbh->begin_work;

	try {
		my $table_name = $object_info->{'table_name'};
		my $primary_key = $object_info->{'primary_key'};

		$dbh->do("LOCK TABLE $table_name;");

		my $sth = $dbh->prepare("SELECT *, lower(period) AS old_date, ?::date AS new_date FROM $table_name WHERE $primary_key=? AND upper(period) IS NULL AND period @> ?::date");
		$sth->execute($date, $object_id, $date);
		my $old_object = $sth->fetchrow_hashref;
		if(!$old_object) {
			die "Cannout change object $object_id at date $date, doesn't exist or it is already historical";
		}

		# If the new date overwrites the last period, delete the row, otherwise update it
		my $changed_rows;
		if($old_object->{'old_date'} eq $old_object->{'new_date'}) {
			my $sth = $dbh->prepare("DELETE FROM $table_name WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($old_object->{'id'}, $old_object->{'period'});
		} else {
			my $sth = $dbh->prepare("UPDATE $table_name SET period=daterange(lower(period), ?) WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($date, $old_object->{id}, $old_object->{period});
		}
		if(!$changed_rows) {
			die "Failed to change object $object_id, even though it existed";
		}

		my @db_fields;
		my @db_values;

		foreach(@{$object_info->{'required_fields'}}) {
			push @db_fields, $_;
			if(!exists($changes->{$_}) || length($changes->{$_}) == 0) {
				push @db_values, $old_object->{$_};
			} else {
				push @db_values, delete $changes->{$_};
			}
		}

		foreach(@{$object_info->{'optional_fields'}}) {
			push @db_fields, $_;
			if(exists($changes->{$_})) {
				push @db_values, delete $changes->{$_};
			} else {
				push @db_values, $old_object->{$_};
			}
		}

		foreach(keys %$changes) {
			die "Unknown object field $_ in update_object\n";
		}

		unshift @db_fields, "period";
		unshift @db_values, '['.$date.',)';

		unshift @db_fields, $primary_key;
		unshift @db_values, $object_id;

		my $query = "INSERT INTO $table_name (" . join(", ", @db_fields) . ")";
		$query .= " VALUES (" . join (", ", (('?') x @db_fields)) . ")";

		$sth = $dbh->prepare($query);
		$sth->execute(@db_values);
		$dbh->commit;
		return get_object($lim, $object_info, $object_id, $date);
	} catch {
		$dbh->rollback;
		die $_;
	};
}

=head3 delete_object($lim, $object_info, $object_id, [$date])

Delete an object. $date is the optional date of deletion; if not given,
'today' is assumed.

=cut

sub delete_object {
	my ($lim, $object_info, $object_id, $date) = @_;
	$date ||= 'today';
	my $dbh = $lim->get_database_handle();

	$dbh->begin_work;

	try {
		my $table_name = $object_info->{'table_name'};
		my $primary_key = $object_info->{'primary_key'};

		$dbh->do("LOCK TABLE $table_name;");
		my $sth = $dbh->prepare("SELECT $primary_key, period, lower(period) AS old_date, ?::date AS new_date FROM $table_name WHERE $primary_key=? AND upper(period) IS NULL AND period @> ?::date");
		$sth->execute($date, $object_id, $date);
		my $old_object = $sth->fetchrow_hashref;
		if(!$old_object) {
			die "Cannout change object $object_id at date $date, doesn't exist or it is already historical";
		}

		# If the new date overwrites the last period, delete the row, otherwise update it
		my $changed_rows;
		if($old_object->{'old_date'} eq $old_object->{'new_date'}) {
			my $sth = $dbh->prepare("DELETE FROM $table_name WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($old_object->{'id'}, $old_object->{'period'});
		} else {
			my $sth = $dbh->prepare("UPDATE $table_name SET period=daterange(lower(period), ?) WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($date, $old_object->{id}, $old_object->{period});
		}

		if(!$changed_rows) {
			die "Failed to delete object $object_id, even though it existed";
		}

		$dbh->commit;
	} catch {
		$dbh->rollback;
		die $_;
	};
}

1;
