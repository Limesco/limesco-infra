#!/usr/bin/perl
use strict;
use warnings;
use MongoDB;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Carp;

=head1 cservimporter.pl

Usage: cservimporter.pl [options] host port database

This tool connects to the old CServ Mongo database and imports its contents to
the Liminfra SQL database.

=cut

if(!caller) {
	# We were called directly
	my $database = pop @ARGV;
	my $port = pop @ARGV;
	my $host = pop @ARGV;
	if(!$database) {
		die "Usage: $0 [options] host port database\n";
	}

	my $lim = Limesco->new_from_args(\@ARGV);

	import_from_cserv_mongo($lim, $host, $port, $database);
}

=head2 Methods

=head3 import_from_cserv_mongo ($lim, $host, $port, $database)

Connect to the old CServ Mongo database and import its contents to the Liminfra
SQL database.

=cut

sub import_from_cserv_mongo {
	my ($lim, $host, $port, $database) = @_;
	my $client = MongoDB::MongoClient->new(host => $host, port => $port);
	my $cservdb = $client->get_database($database);
	my $dbh = $lim->get_database_handle();

	try {
		$dbh->begin_work;
		$dbh->do("LOCK TABLE account IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE sim IN ACCESS EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE speakupAccount IN ACCESS EXCLUSIVE MODE;");

		my $accounts_map = import_accounts($lim, $cservdb, $dbh);
		import_sims($lim, $cservdb, $dbh, $accounts_map);
		import_invoices($lim, $cservdb, $dbh, $accounts_map);
		import_cdrs($lim, $cservdb, $dbh, $accounts_map);

		$dbh->commit;
		return;
	} catch {
		$dbh->rollback;
	}
}

=head3 verify_table_empty($dbh, $table)

Checks that the given table exists and is empty in the database pointed to by
$dbh. Throws if not.

=cut

sub verify_table_empty {
	my ($dbh, $table) = @_;
	my $sth = $dbh->prepare("SELECT EXISTS (SELECT relname FROM pg_class WHERE relname=?)");
	$sth->execute($table);
	if(!$sth->fetchrow_arrayref()->[0]) {
		croak "Will not continue: table $table does not exist";
	}

	# TODO: apparantly, $table exists so it was safe to use in a CREATE TABLE statement
	# however, it would be nicer here if we didn't have to insert it directly into the query
	$sth = $dbh->prepare("SELECT EXISTS (SELECT 1 FROM $table)");
	$sth->execute();
	if($sth->fetchrow_arrayref()->[0]) {
		croak "Will not continue: table $table is not empty";
	}
}

=head3 import_accounts($lim, $database, $dbh)

Import the accounts from Mongo to Liminfra. The given database is a
MongoDB::Database pointing at CServ's database; the given dbh is a DBI handle
pointing at liminfra's database. The dbh must be in transaction state and
should have an exclusive lock on the 'account' table.

This method returns a hashref containing CServ account ID's as keys and
liminfra account ID's as values.

=cut

sub import_accounts {
	my ($lim, $database, $dbh) = @_;
	my $collection = $database->get_collection("accounts");

	verify_table_empty($dbh, "account");
	# restart the account ID sequence
	$dbh->do("ALTER SEQUENCE account_id_seq RESTART WITH 1");

	my $cursor = $collection->find();
	my %accounts_map;
	while(my $account = $cursor->next) {
		my $id = $account->{'_id'}->to_string();
		my $speakupAccount = $account->{'externalAccount'}{'speakup'};
		my $email = $account->{'email'};
		if(!$email) {
			# invalid account, just add the speakupAccount and continue
			if($speakupAccount) {
				$dbh->do("INSERT INTO speakupAccount (name, period) VALUES (?, '[today,)')", undef, $speakupAccount);
			}
			next;
		}
		my $sth = $dbh->prepare("INSERT INTO account (id, period, first_name, last_name, street_address, postal_code,
			city, email, state) VALUES (NEXTVAL('account_id_seq'), '[today,)', ?, ?, ?, ?, ?, ?, ?)");
		$sth->execute($account->{'fullName'}{'firstName'}, $account->{'fullName'}{'lastName'},
			$account->{'address'}{'streetAddress'}, $account->{'address'}{'postalCode'},
			$account->{'address'}{'locality'}, $email, $account->{'state'});
		my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});
		$accounts_map{$id} = $account_id;
		if($speakupAccount) {
			$dbh->do("INSERT INTO speakupAccount (name, period, account_id) VALUES (?, '[today,)', ?)", undef, $speakupAccount, $account_id);
		}
	}
	return \%accounts_map;
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
