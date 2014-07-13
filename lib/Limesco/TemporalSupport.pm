package Limesco::TemporalSupport;
use strict;
use warnings;
use Try::Tiny;
use Exporter 'import';
use Limesco;

our $VERSION = $Limesco::VERSION;
our @EXPORT = qw(get_object list_objects create_object update_object delete_object object_changes_between);

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
	my ($lim, $object_info, $object_id, $orig_changes, $date) = @_;
	$date ||= 'today';

	# copy orig_changes to changes so we don't clobber input
	my $changes = {};
	foreach(keys %$orig_changes) {
		$changes->{$_} = $orig_changes->{$_};
	}

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	$dbh->begin_work if $dbh_is_mine;

	try {
		my $table_name = $object_info->{'table_name'};
		my $primary_key = $object_info->{'primary_key'};

		$dbh->do("LOCK TABLE $table_name;");

		my $sth = $dbh->prepare("SELECT *, lower(period) AS old_date, upper(period) AS propagate_date, ?::date AS new_date FROM $table_name WHERE $primary_key=? AND period @> ?::date");
		$sth->execute($date, $object_id, $date);
		my $old_object = $sth->fetchrow_hashref;
		if(!$old_object) {
			die "Cannot change object $object_id at date $date, doesn't exist or is deleted here";
		}

		# If the new date overwrites the last period, delete the row, otherwise update it
		my $changed_rows;
		if($old_object->{'old_date'} && $old_object->{'old_date'} eq $old_object->{'new_date'}) {
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
		my $propagate_date = $old_object->{'propagate_date'};
		my $new_enddate = $propagate_date ? $propagate_date : '';
		unshift @db_values, '['.$date.',' . $new_enddate . ')';

		unshift @db_fields, $primary_key;
		unshift @db_values, $object_id;

		my $query = "INSERT INTO $table_name (" . join(", ", @db_fields) . ")";
		$query .= " VALUES (" . join (", ", (('?') x @db_fields)) . ")";

		$sth = $dbh->prepare($query);
		$sth->execute(@db_values);

		# if we changed a historical record, propagate these changes to future records
		if(defined($propagate_date)) {
			foreach my $key (keys %$orig_changes) {
				# find the first row where this key changed again
				$query = "SELECT lower(period) FROM $table_name WHERE $primary_key=? AND lower(period) >= ? AND $key <> ? ORDER BY period ASC";
				$sth = $dbh->prepare($query);
				$sth->execute($old_object->{$primary_key}, $propagate_date, $old_object->{$key});
				my $propagate_end_date = $sth->fetchrow_arrayref;
				$propagate_end_date = $propagate_end_date->[0] if($propagate_end_date);

				if(!$propagate_end_date || $propagate_end_date ne $propagate_date) {
					# propagate this change to all rows from $propagate_date to $propagate_end_date
					$query = "UPDATE $table_name SET $key=? WHERE $primary_key=? AND period <@ daterange(?::date, ?::date)";
					$sth = $dbh->prepare($query);
					$sth->execute($orig_changes->{$key}, $old_object->{$primary_key}, $propagate_date, $propagate_end_date);
				}
			}
		}

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
		if($old_object->{'old_date'} && $old_object->{'old_date'} eq $old_object->{'new_date'}) {
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

=head3 object_changes_between($lim | $dbh, $object_info, $object_id, [$startdate, [$enddate]])

Retrieve the changes done between two dates, INCLUSIVE. If the same date is
given for $startdate and $enddate, return the change on that date if there was
one.  'undef' can be given instead of either of the two variables to mean
"infinitely in that direction" or instead of both to mean "infinitely". For
example, giving a startdate of undef and an enddate of '2014-03-01' means all
changes to the given object before 2014-03-01, including changes done on
2014-03-01.

=cut

sub object_changes_between {
	my ($lim, $object_info, $object_id, $startdate, $enddate) = @_;

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	$dbh->begin_work if $dbh_is_mine;

	try {
		my $table_name = $object_info->{'table_name'};
		my $primary_key = $object_info->{'primary_key'};

		# Lock the table to ensure internal consistency while this process is running
		$dbh->do("LOCK TABLE $table_name IN EXCLUSIVE MODE;");

		# First, fetch all records valid between the given dates
		my $sth = $dbh->prepare("SELECT *, "
			# add a column to every row that says if the row is supposed to be returned,
			# i.e. if the start date of the row is within [$startdate, $enddate+1)
			."(lower(period) <@ daterange(?::date, (?::date + '1 day'::interval)::date)) "
			."AS temporal_included_in_changes FROM $table_name WHERE $primary_key=? AND "
			# select all records that fall between a slightly wider daterange than given
			# this makes sure if the startdate is the date of the change, we also receive
			# the record right before it (and vice versa for enddate)
			."period && daterange((?::date - '1 day'::interval)::date, (?::date + '1 day'::interval)::date)");
		$sth->execute($startdate, $enddate, $object_id, $startdate, $enddate);

		my @rows_to_return;
		my $previous_row;
		while(my $row = $sth->fetchrow_hashref()) {
			my $included = delete $row->{'temporal_included_in_changes'};
			if($included) {
				if(!defined($previous_row)) {
					# No previous row in the database, so this is a creation row, return it as-is
					push @rows_to_return, $row;
				} else {
					# Compute changes between the previous row and this one; assume keys of both
					# hashes will be the same since they come from the same table
					my %changes;
					for(keys %$row) {
						my $prev = $previous_row->{$_};
						my $new  = $row->{$_};
						if(!defined($prev) && defined($new)) {
							$changes{$_} = $new;
						} elsif(defined($prev) && !defined($new)) {
							$changes{$_} = undef;
						} elsif(defined($prev) && defined($new) && $prev ne $new) {
							$changes{$_} = $new;
						}
					}
					if(%changes) {
						push @rows_to_return, \%changes;
					}
				}
			}
			$previous_row = $row;
		}
		return @rows_to_return;
	} catch {
		$dbh->rollback if $dbh_is_mine;
		die $_;
	};
}

1;
