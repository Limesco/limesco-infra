#!/usr/bin/perl
use strict;
use warnings;

use Test::PostgreSQL;
use Test::More;
use Test::XML::Simple;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 69;

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
	postal_code, city, email) values (NEXTVAL('account_id_seq'), '(,)', 'Test', 'User',
	'', '', '', '');");
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
		'NL24RABO0169307587', 'RABONL2U',
		'2013-04-05');
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown when IBAN number incorrect");

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

ok($exception, "Exception thrown when adding overlapping date ranges");

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
my $file = create_directdebit_file($lim, "FRST", "2015-01-02");
is($file->{'processing_date'}, "2015-01-02", "Processing date is set correctly");
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

mark_directdebit_transaction($lim, $transaction1->{'id'}, "PRESETTLEMENTREJECT");
is(get_directdebit_transaction($lim, $transaction1->{'id'})->{'status'}, "PRESETTLEMENTREJECT", "Transaction 1 marked pre-settlement reject");
mark_directdebit_transaction($lim, $transaction2->{'id'}, "POSTSETTLEMENTREJECT");
is(get_directdebit_transaction($lim, $transaction2->{'id'})->{'status'}, "POSTSETTLEMENTREJECT", "Transaction 2 marked post-settlement reject");

# A post-settlement and pre-settlement reject means one transaction in this
# authorization did come through to the bank, so next transactions must be RCUR

# No transactions claimable for a new file now
$exception = undef;
try {
	my $file = create_directdebit_file($lim, "FRST");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a FRST file after two rejects");

$exception = undef;
try {
	my $file = create_directdebit_file($lim, "RCUR");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a RCUR file after two rejects");

# Select new invoices for new transactions
@invoices = select_directdebit_invoices($lim, $authorization);
is(scalar @invoices, 2, "Two invoices in directdebit authorization after two rejects");
is($invoices[0]{'id'}, "13C000040", "Correct invoice 1 in directdebit authorization selected");
is($invoices[1]{'id'}, "14C000010", "Correct invoice 2 in directdebit authorization selected");

# Create transactions from the invoices
my $transaction3 = create_directdebit_transaction($lim, $authorization, $invoices[0]);
my $transaction4 = create_directdebit_transaction($lim, $authorization, $invoices[1]);
isnt($transaction1->{'id'}, $transaction3->{'id'}, "Different transaction ID's");
isnt($transaction2->{'id'}, $transaction3->{'id'}, "Different transaction ID's");
isnt($transaction1->{'id'}, $transaction4->{'id'}, "Different transaction ID's");
isnt($transaction2->{'id'}, $transaction4->{'id'}, "Different transaction ID's");
isnt($transaction3->{'id'}, $transaction4->{'id'}, "Different transaction ID's");

$exception = undef;
try {
	my $file = create_directdebit_file($lim, "FRST");
} catch {
	$exception = $_;
};

ok($exception, "Exception thrown while creating a FRST file after a post-settlement reject");

$exception = undef;
my $rcur_file;
try {
	$rcur_file = create_directdebit_file($lim, "RCUR", "2016-03-04");
} catch {
	$rcur_file = {};
	$exception = $_;
};

ok(!defined($exception), "No exception thrown while creating a RCUR file after a post-settlement reject");

# Both transactions added to the new file?
is(get_directdebit_transaction($lim, $transaction3->{'id'})->{'directdebit_file_id'}, $rcur_file->{'id'}, "Transaction 3 added to file");
is(get_directdebit_transaction($lim, $transaction4->{'id'})->{'directdebit_file_id'}, $rcur_file->{'id'}, "Transaction 4 added to file");

# Can we mark a single transaction and then mark the rest of the file?
mark_directdebit_transaction($lim, $transaction3->{'id'}, "POSTSETTLEMENTREJECT");
is(get_directdebit_transaction($lim, $transaction3->{'id'})->{'status'}, "POSTSETTLEMENTREJECT", "Transaction 3 marked post-settlement reject");
mark_directdebit_file($lim, $rcur_file->{'id'}, "SUCCESS");
is(get_directdebit_transaction($lim, $transaction3->{'id'})->{'status'}, "POSTSETTLEMENTREJECT", "Transaction 3 still marked post-settlement reject");
is(get_directdebit_transaction($lim, $transaction4->{'id'})->{'status'}, "SUCCESS", "Transaction 4 marked successful");

my $xml = export_directdebit_file($lim, $rcur_file->{'id'});
xml_valid($xml, "Valid XML exported");

# hack to test if the XML has the namespace
like($xml, qr%<Document\s+xmlns="urn:iso:std:iso:20022:tech:xsd:pain\.008\.001\.02"\s+xmlns:xsi="http://www\.w3\.org/2001/XMLSchema-instance">%, "XML contains valid namespace");
# now, remove it so we can access /Document in a sane way in libxml
$xml =~ s/<Document\s+xmlns="[^"]+"\s+xmlns:xsi="[^"]+">/<Document>/g;
$xml = XML::LibXML->new->parse_string($xml);

xml_node($xml, '/Document', 'Document root exists');
xml_is($xml, '/Document/CstmrDrctDbtInitn/GrpHdr/NbOfTxs', 2, "Two transactions in this file");
xml_is($xml, '/Document/CstmrDrctDbtInitn/GrpHdr/CtrlSum', '650.00', "Currency amount is OK");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/NbOfTxs', 2, "Two transactions in this file (other header)");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/CtrlSum', '650.00', "Currency amount is OK (other header)");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/PmtTpInf/SeqTp', 'RCUR', "File is marked 'recurrent transfer'");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/ReqdColltnDt', '2016-03-04', "Correct processing date is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/PmtId/InstrId', '13C000040', "Correct invoice ID is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/PmtId/EndToEndId', '13C000040', "Correct invoice ID is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/InstdAmt', '250.00', "Correct amount is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/DrctDbtTx/MndtRltdInf/MndtId', $authorization, "Correct authorization is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/DrctDbtTx/MndtRltdInf/DtOfSgntr', "2013-04-05", "Correct signature date is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/DbtrAgt/FinInstnId/BIC', "RABONL2U", "Correct BIC code is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/Dbtr/Nm', "Limesco B.V.", "Correct account name is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/Dbtr/CtryOfRes', "NL", "Correct country is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[1]/DbtrAcct/Id/IBAN', "NL24RABO0169207587", "Correct IBAN number is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[2]/PmtId/InstrId', '14C000010', "Correct invoice ID is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[2]/PmtId/EndToEndId', '14C000010', "Correct invoice ID is used");
xml_is($xml, '/Document/CstmrDrctDbtInitn/PmtInf/DrctDbtTxInf[2]/InstdAmt', '400.00', "Correct amount is used");

# TODO: mark a file as failed -> should lead to a pre-settlement reject on all transactions;
# an exception must be thrown if any transaction is already marked before this
# TODO: more authorizations so FRST and RCUR files are combined
# TODO: select_directdebit_invoices may only return invoices that are not already in a SUCCESS or NEW transaction
# TODO: get_active_directdebit_authorizations

$dbh->disconnect();
