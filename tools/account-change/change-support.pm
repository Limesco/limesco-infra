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

1;
