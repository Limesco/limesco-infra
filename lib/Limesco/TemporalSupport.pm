package Limesco::TemporalSupport;
use strict;
use warnings;
use Try::Tiny;
use Exporter 'import';
use Limesco;

our $VERSION = $Limesco::VERSION;
our @EXPORT = qw(get_object list_objects create_object update_object delete_object);

=head1 Limesco::TemporalSupport

This method provides support for retrieving, modifying and deleting temporal
rows. Every method accepts either a Limesco object or a database handle as the
first parameter, which can be useful when you already have a database handle
open. In case you give a database handle, the functions are transaction-aware,
meaning that they will not open new transactions and will definitely die() when
a part of the method failed allowing you to issue a rollback.

=head2 Methods

=head3 get_object($lim | $dbh, $object_info, $object_id, [$date])

Retrieve an object. $date is the optional date of interest; if not given,
'today' is assumed. $object_id is the value of the primary key of the row in
which we are interested.

=cut

sub get_object {
	my ($lim, $object_info, $object_id, $date) = @_;
	$date ||= 'today';
	my $table_name = $object_info->{'table_name'};
	my $primary_key = $object_info->{'primary_key'};

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	my $sth = $dbh->prepare("SELECT * FROM $table_name WHERE $primary_key=? AND period @> ?::date");
	$sth->execute($object_id, $date);
	my $object = $sth->fetchrow_hashref;
	if(!$object) {
		die "No such object with ID $object_id at date $date";
	}
	return $object;
}

=head3 list_objects($lim | $dbh, $object_info, [$date])

Retrieve all objects as active on the given $date. If $date is not given,
'today' is assumed. Objects are returned in the order of their primary key.

=cut

sub list_objects {
	my ($lim, $object_info, $date) = @_;
	$date ||= 'today';
	my $table_name = $object_info->{'table_name'};
	my $primary_key = $object_info->{'primary_key'};

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	my $sth = $dbh->prepare("SELECT * FROM $table_name WHERE period @> ?::date ORDER BY $primary_key ASC");
	$sth->execute($date);
	my @objects;
	while(my $object = $sth->fetchrow_hashref) {
		push @objects, $object;
	}
	return @objects;
}

=head3 create_object($lim | $dbh, $object_info, $object, [$date])

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

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	my $table_name = $object_info->{'table_name'};
	my $primary_key = $object_info->{'primary_key'};
	my $primary_key_seq = $object_info->{'primary_key_seq'};

	my @db_fields;
	my @db_values;
	my $primary_key_value;

	foreach(@{$object_info->{'required_fields'}}) {
		if(!exists($object->{$_}) || length($object->{$_}) == 0) {
			die "Required object field $_ is missing in create_object";
		}
		if($_ eq $primary_key) {
			# If the primary key is in required_fields, it must be given instead of
			# being auto-initialised based on a sequence.
			$primary_key_value = delete $object->{$_};
			next;
		}
		push @db_fields, $_;
		push @db_values, delete $object->{$_};
	}

	foreach(@{$object_info->{'optional_fields'}}) {
		if($_ eq $primary_key) {
			die "Primary key may never be in optional_fields for an object";
		}
		if(exists($object->{$_})) {
			push @db_fields, $_;
			push @db_values, delete $object->{$_};
		}
	}

	foreach(keys %$object) {
		if($_ eq $primary_key) {
			die "Primary key value given to create_object, but if that's allowed it must be one of the required_fields";
		}
		die "Unknown object field $_ in create_object\n";
	}

	unshift @db_fields, "period";
	unshift @db_values, '['.$date.',)';

	my $primary_key_init_string;
	if($primary_key_value) {
		unshift @db_values, $primary_key_value;
		$primary_key_init_string = "?";
	} elsif($primary_key_seq) {
		$primary_key_init_string = "NEXTVAL('$primary_key_seq')";
	} else {
		die "Don't know how to init primary key for this object type: no primary key value is given, no primary key sequence is known. Is the primary key missing in the required fields list?";
	}

	my $query = "INSERT INTO $table_name ($primary_key, " . join(", ", @db_fields) . ")";
	$query .= " VALUES ($primary_key_init_string, " . join (", ", (('?') x @db_fields)) . ")";

	my $sth = $dbh->prepare($query);
	$sth->execute(@db_values);

	my $object_id = $primary_key_value ? $primary_key_value : $dbh->last_insert_id(undef, undef, undef, undef, {sequence => $primary_key_seq});
	return get_object($lim, $object_info, $object_id, $date);
}

=head3 update_object($lim | $dbh, $object_info, $object_id, $changes, [$date])

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

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	$dbh->begin_work if $dbh_is_mine;

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
			$changed_rows = $sth->execute($old_object->{$primary_key}, $old_object->{'period'});
		} else {
			my $sth = $dbh->prepare("UPDATE $table_name SET period=daterange(lower(period), ?) WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($date, $old_object->{$primary_key}, $old_object->{period});
		}
		if(!$changed_rows) {
			die "Failed to change object $object_id, even though it existed";
		}

		my @db_fields;
		my @db_values;

		foreach(@{$object_info->{'required_fields'}}) {
			if($_ eq $primary_key) {
				if(exists($changes->{$_})) {
					die "The primary key of an object may never be listed in the changes of update_object";
				}
				next;
			}
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
		$dbh->commit if $dbh_is_mine;
		return get_object($lim, $object_info, $object_id, $date);
	} catch {
		$dbh->rollback if $dbh_is_mine;
		die $_;
	};
}

=head3 delete_object($lim | $dbh, $object_info, $object_id, [$date])

Delete an object. $date is the optional date of deletion; if not given,
'today' is assumed.

=cut

sub delete_object {
	my ($lim, $object_info, $object_id, $date) = @_;
	$date ||= 'today';

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	$dbh->begin_work if $dbh_is_mine;

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
			$changed_rows = $sth->execute($old_object->{$primary_key}, $old_object->{'period'});
		} else {
			my $sth = $dbh->prepare("UPDATE $table_name SET period=daterange(lower(period), ?) WHERE $primary_key=? AND period=?");
			$changed_rows = $sth->execute($date, $old_object->{$primary_key}, $old_object->{period});
		}

		if(!$changed_rows) {
			die "Failed to delete object $object_id, even though it existed";
		}

		$dbh->commit if $dbh_is_mine;
	} catch {
		$dbh->rollback if $dbh_is_mine;
		die $_;
	};
}

1;
