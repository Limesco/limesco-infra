#!/usr/bin/perl
use strict;
use warnings;

use Test::PostgreSQL;
use Test::More;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 26;

require_ok("directdebit.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl') or die $!;
initialize_database($lim);

my @authorizations;

{
	my $authorization;
	try {
		$authorization = generate_directdebit_authorization($lim);
	};
	ok($authorization, "DirectDebit authorization generated");
	push @authorizations, $authorization;
}

for(1..999) {
	my $a;
	try {
		$a = generate_directdebit_authorization($lim);
	};
	push @authorizations, $a;
}

my $failures = 0;
my $duplicates = 0;
foreach my $a (@authorizations) {
	if(!$a) {
		$failures += 1;
		next;
	}
	my @equals = grep { $_ && $_ eq $a } @authorizations;
	$duplicates += @equals - 1;
}

is($failures, 0, "No authorization codes failed to generate");
is($duplicates, 0, "No duplicate authorizations generated");

my $exception;
try {
	add_directdebit_account($lim, 1,
		generate_directdebit_authorization($lim),
		'Limesco B.V.',
		'NL24RABO0169207587', 'RABONL2U',
		'2013-05-04');
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while registering non-existing account for direct debit");

$dbh->do("INSERT INTO account (id, period, first_name, last_name, street_address,
	postal_code, city, email, state) values (NEXTVAL('account_id_seq'), '(,)', 'Test', 'User',
	'', '', '', '', 'CONFIRMED');");
my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});
my $authorization = generate_directdebit_authorization($lim);

$exception = undef;
try {
	add_directdebit_account($lim, $account_id,
		$authorization, 'Limesco B.V.',
		'NL24RABO0169207587', 'RABONL2U',
		'04-05-2013'); # <--- wrong date format
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown with wrong date");

$exception = undef;
try {
	add_directdebit_account($lim, $account_id,
		$authorization, 'Limesco B.V.',
		'NL24RABO0169207587', 'RABONL2U',
		'2013-04-05');
} catch {
	$exception = $_;
};

is($exception, undef, "Directdebit account add succeeded.");

# try to use the same authorization again, non-overlapping daterange
$exception = undef;
try {
	add_directdebit_account($lim, $account_id,
		$authorization, 'Limesco B.V.',
		'NL24RABO0169207587', 'RABONL2U',
		'2013-04-01', '2013-04-05');
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while using the same authorization twice");

# try to add another authorization, overlapping daterange
$exception = undef;
try {
	add_directdebit_account($lim, 1,
		generate_directdebit_authorization($lim),
		'Limesco B.V.',
		'NL24RABO0169207587', 'RABONL2U',
		'2013-04-05');
} catch {
	$exception = $_;
};

# selecteer facturen voor dit machtingskenmerk
# authorization -> account_id, datum
# kies alle facturen van die account >= datum
my @invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 0, "No invoices in directdebit authorization");

# invoice with a different account ID
my $invoice_sth = $dbh->prepare("INSERT INTO invoice (id, account_id, date, creation_time, rounded_without_taxes,
	rounded_with_taxes) VALUES (?, ?, ?, ?, ?, ?);");
$invoice_sth->execute("13C000010", $account_id + 1, '2013-04-05', '2014-04-05 00:00:00', '123.45', '150.00');
@invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 0, "No invoices in directdebit authorization");

# invoice with a date before authorization date
$invoice_sth->execute("13C000020", $account_id, '2013-04-03', '2013-04-03 00:00:00', '123.45', '150.00');
@invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 0, "No invoices in directdebit authorization");

# invoice with a date before authorization date, creationdate after
$invoice_sth->execute("13C000030", $account_id, '2013-04-03', '2013-04-05 00:00:00', '123.45', '150.00');
@invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 0, "No invoices in directdebit authorization");

# invoice with a valid date and account
$invoice_sth->execute("13C000040", $account_id, '2013-04-05', '2013-04-05 00:00:00', '234.56', '250.00');
$invoice_sth->execute("14C000010", $account_id, '2014-05-06', '2013-04-05 00:00:00', '345.67', '400.00');
@invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 2, "Two invoices in directdebit authorization");
is($invoices[0]{'id'}, "13C000040", "Correct invoice 1 in directdebit authorization selected");
is($invoices[1]{'id'}, "14C000010", "Correct invoice 2 in directdebit authorization selected");

# No file created without transactions in database
$exception = undef;
try {
	my $file = create_directdebit_file($lim, "FRST");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a file without any transactions in DB");

# Create transactions from the invoices
my $transaction1 = create_directdebit_transaction($lim, $authorization, $invoices[0]);
is($transaction1->{'authorization_id'}, $authorization, "correct authorization ID in transaction");
is_deeply($transaction1, get_directdebit_transaction($lim, $transaction1->{'id'}), "Retrieving transaction returns exactly the same object as returning it");

my $transaction2 = create_directdebit_transaction($lim, $authorization, $invoices[1]);
isnt($transaction1->{'id'}, $transaction2->{'id'}, "Different transaction ID's");

# No recurring transactions yet
$exception = undef;
try {
	my $file = create_directdebit_file($lim, "RCUR");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a recurring files with only first-time transactions");

# Both transactions added to file?
my $file = create_directdebit_file($lim, "FRST");
is(get_directdebit_transaction($lim, $transaction1->{'id'})->{'directdebit_file_id'}, $file->{'id'}, "Transaction 1 added to file");
is(get_directdebit_transaction($lim, $transaction2->{'id'})->{'directdebit_file_id'}, $file->{'id'}, "Transaction 2 added to file");
is_deeply($file, get_directdebit_file($lim, $file->{'id'}), "Returned file is exactly the same as retrieved file");

# Transaction not claimable twice
$exception = undef;
try {
	my $file = create_directdebit_file($lim, "FRST");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a file without any new transactions");

is(get_directdebit_transaction($lim, $transaction1->{'id'})->{'directdebit_file_id'}, $file->{'id'}, "Transaction 1 not changed");
is(get_directdebit_transaction($lim, $transaction2->{'id'})->{'directdebit_file_id'}, $file->{'id'}, "Transaction 2 not changed");

# TODO: succeeded files: transactions should be marked succeeded
# TODO: failed transactions (pre and post settlement rejects)
# the code should try creating new transactions for these invoices; these transactions
# should belong to RCUR files in the case of a post settlement reject and FRST files in
# the case of a pre-settlement reject, unless an earlier transaction was succesful
# TODO: failed files should lead to pre-settlement reject on all transactions
# (also test these situations with existing succeeded or failed transactions and files
#  in the database to see if these are also handled correctly)

# TODO: implement exporting a directdebit file to XML and check if it matches the
# expected XML contents

$dbh->disconnect();