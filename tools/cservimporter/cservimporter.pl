#!/usr/bin/perl
use strict;
use warnings;
use MongoDB;

=head1 cservimporter.pl

Usage: cservimporter.pl host port database

This tool connects to the old CServ Mongo database and imports its contents to
the Liminfra SQL database.

=cut

if(!caller) {
	# We were called directly
	my ($host, $port, $database) = @_;
	if(!$database) {
		die "Usage: $0 host port database\n";
	}

	import_from_cserv_mongo($host, $port, $database);
}

=head2 Methods

=head3 import_from_cserv_mongo ($host, $port, $database)

Connect to the old CServ Mongo database and import its contents to the Liminfra
SQL database.

=cut

sub import_from_cserv_mongo {
	my ($host, $port, $database) = @_;
	my $client = MongoDB::MongoClient->new(host => $host, port => $port);
	my $cservdb = $client->get_database($database);

	import_accounts($cservdb);
	import_sims($cservdb);
	import_cdrs($cservdb);
	import_invoices($cservdb);
}

=head3 import_accounts($database)

Import the accounts from Mongo to Liminfra.

=cut

sub import_accounts {
	my ($database) = @_;
}

=head3 import_sims($database)

Import the sims from Mongo to Liminfra.

=cut

sub import_sims {
	my ($database) = @_;
}

=head3 import_cdrs($database)

Import the CDRs from Mongo to Liminfra.

=cut

sub import_cdrs {
	my ($database) = @_;
}

=head3 import_invoices($database)

Import the invoices from Mongo to Liminfra.

=cut

sub import_invoices {
	my ($database) = @_;
}

1;
