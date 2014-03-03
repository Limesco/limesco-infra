#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;

=head1 upgrade.pl

Usage: upgrade.pl [infra-options]

This tool can be used to initialize or upgrade a database schema. It connects
to the database, detects the current schema version, and upgrades it to the
newest version.

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV);
	my $version = get_current_schema_version($lim);
	my $latest_version = get_latest_schema_version();
	if(!$version) {
		if(ask_confirmation("The database seems uninitialized. Proceed to initialize to latest schema version $latest_version?")) {
			initialize_database($lim);
			print "Done initializing database.\n";
		} else {
			print "Cancel initialization.\n";
			exit 1;
		}
	} elsif($version == $latest_version) {
		print "The current database schema version is $version, which is the latest version. No upgrading needed.\n";
	} elsif($version > $latest_version) {
		print "The current database schema version is $version, which is newer than the latest version this tool can understand ($latest_version). Please upgrade your repository.\n";
		exit 1;
	} elsif(ask_confirmation("The current database schema version is $version, which is upgradable to the latest schema version $latest_version. Proceed to upgrade?")) {
		upgrade_database($lim, $version);
		print "Done upgrading database.\n";
	} else {
		print "Cancel upgrade.\n";
		exit 1;
	}
	exit 0;
}

=head2 Methods

=head3 get_current_schema_version($lim, [$current_date])

Returns the current schema version in the database, or undefined if none could be found.

The current schema version is stored in the "meta" table. If that table doesn't
exist, the current schema version is 0. Otherwise, the schema_version column of
the newest row is returned, which must have a period starting before
$current_date and not ending. If any of these preconditions is not true, or
more than one row matches these preconditions, this method throws.

=cut

sub get_current_schema_version {
	my ($lim, $current_date) = @_;
	my $dbh = $lim->get_database_handle();
	if(!$current_date) {
		my (undef, undef, undef, $mday, $mon, $yr) = localtime(time);
		$current_date = sprintf("%04d-%02d-%02d", $yr + 1900, $mon + 1, $mday);
	}

	# Check if the table exists
	my $sth = $dbh->prepare("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='meta')");
	$sth->execute();
	my $result = $sth->fetchrow_arrayref();
	if(!$result->[0]) {
		return 0;
	}

	# It exists, so it must have exactly one valid row
	$sth = $dbh->prepare("SELECT period, schema_version FROM meta WHERE upper_inf(period)");
	$sth->execute();
	$result = $sth->fetchrow_arrayref();
	if(!$result) {
		die "No rows match preconditions for get_current_schema_version";
	}
	if($sth->fetchrow_arrayref()) {
		die "More than one row matches preconditions for get_current_schema_version";
	}

	if(!daterange_in($result->[0], $current_date)) {
		die "Newest meta row is not valid yet, cannot determine current schema version";
	}

	return $result->[1];
}

sub daterange_in {
	my ($daterange, $date) = @_;
	my ($daterange_start, $daterange_end);
	# only allow inclusive start and exclusive end: [date, date) and [date,)
	if($daterange !~ /^\[\s*(\d{4}-\d\d-\d\d)\s*,(?:\s*(\d{4}-\d\d-\d\d)?\)|\))$/) {
		die "Could not parse invalid date range: $daterange";
	}
	$daterange_start = $1;
	$daterange_end = $2;
	if($date lt $daterange_start) {
		return 0;
	}
	if(defined($daterange_end) && $date ge $daterange_end) {
		return 0;
	}
	return 1;
}

=head3 get_latest_schema_version()

Returns the latest schema version supported by this tool.

=cut

sub get_latest_schema_version {
	return 1;
}

=head3 ask_confirmation($question)

Utility method. Asks the question until yes or no was answered, then returns 1 if yes, 0 if no.

=cut

sub ask_confirmation {
	my ($question) = @_;
	while(1) {
		print $question . "\nYes or no? [yn] ";
		my $answer = lc(<STDIN>);
		1 while chomp $answer;
		if($answer eq "y" || $answer eq "yes") {
			return 1;
		} elsif($answer eq "n" || $answer eq "no") {
			return 0;
		} else {
			print "Incomprehensible reply. Try again.\n\n";
		}
	}
}

=head3 initialize_database($lim)

Initializes the database to the schema version returned by get_latest_schema_version().
Assumes no tables already exist in the database.

=cut

sub initialize_database {
	my ($lim) = @_;
	my $dbh = $lim->get_database_handle();
	try {
		$dbh->begin_work();
		# Disable CREATE TABLE notices for this transaction only
		$dbh->do("SET LOCAL client_min_messages='WARNING';");

		$dbh->do("CREATE EXTENSION IF NOT EXISTS btree_gist;");

		$dbh->do("CREATE TABLE meta (
			period DATERANGE,
			schema_version INT,
			EXCLUDE USING gist (period WITH &&)
		);");
		$dbh->do("INSERT INTO meta (period, schema_version) values ('[today,)', ?)", undef, get_latest_schema_version());

		# A domain AS TEXT CONSTRAINT max_length is the fastest to
		# insert, fastest to search through, and fastest to change the
		# length constraint. We place a CONSTRAINT on our database
		# mostly to prevent these text from becoming insanely large.
		# See benchmark: http://www.depesz.com/2010/03/02/charx-vs-varcharx-vs-varchar-vs-text/
		$dbh->do("CREATE DOMAIN shorttext AS TEXT CONSTRAINT max_length CHECK (LENGTH(VALUE) <= 100);");
		$dbh->do("CREATE TYPE accountstate AS ENUM('UNPAID', 'UNCONFIRMED', 'CONFIRMATION_REQUESTED', 'CONFIRMED', 'DEACTIVATED');");

		$dbh->do("CREATE SEQUENCE account_id_seq;");

		$dbh->do("CREATE TABLE account (
			id INTEGER,
			period DATERANGE,
			first_name SHORTTEXT,
			last_name SHORTTEXT,
			street_address SHORTTEXT,
			postal_code SHORTTEXT,
			city SHORTTEXT,
			email SHORTTEXT,
			password_hash SHORTTEXT NULL,
			admin BOOLEAN,
			state ACCOUNTSTATE,
			PRIMARY KEY (id, period),
			EXCLUDE USING gist (id WITH =, period WITH &&)
		);");

		$dbh->do("CREATE TABLE speakupAccount (
			name SHORTTEXT,
			period DATERANGE,
			account_id INTEGER NULL,
			PRIMARY KEY (name, period),
			EXCLUDE USING gist (name WITH =, period WITH &&),
			EXCLUDE USING gist (account_id WITH =, period WITH &&)
		);");

		return $dbh->commit();
	} catch {
		$dbh->rollback();
	}
}

=head3 upgrade_database($lim, $current_version)

Upgrades the database schema to the version returned by get_latest_schema_version(), or throws
an exception if this was impossible.

=cut

sub upgrade_database {
	my ($lim, $current_version) = @_;
	if($current_version <= 0) {
		die "upgrade_database cannot initialize a database\n";
	}
	die "Not implemented";
}

1;
