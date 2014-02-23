#!/usr/bin/perl
use strict;
use warnings;

=head1 upgrade.pl

Usage: upgrade.pl

This tool can be used to initialize or upgrade a database schema. It connects
to the database, detects the current schema version, and upgrades it to the
newest version.

=cut

if(!caller) {
	my $version = get_current_schema_version();
	my $latest_version = get_latest_schema_version();
	if(!$version) {
		if(ask_confirmation("The database seems uninitialized. Proceed to initialize to latest schema version $latest_version?")) {
			initialize_database();
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
		upgrade_database($version, $latest_version);
	} else {
		print "Cancel upgrade.\n";
		exit 1;
	}
	exit 0;
}

=head2 Methods

=head3 get_current_schema_version()

Returns the current schema version in the database, or undefined if none could be found.

=cut

sub get_current_schema_version {}

=head3 get_latest_schema_version()

Returns the latest schema version supported by this tool.

=cut

sub get_latest_schema_version {}

=head3 ask_confirmation($question)

Utility method. Asks the question until yes or no was answered, then returns 1 if yes, 0 if no.

=cut

sub ask_confirmation {}

=head3 initialize_database()

Initializes the database to the schema version returned by get_latest_schema_version().
Assumes no tables already exist in the database.

=cut

sub initialize_database {}

=head3 upgrade_database()

Upgrades the database schema to the version returned by get_latest_schema_version(), or throws
an exception if this was impossible.

=cut

sub upgrade_database {}

1;
