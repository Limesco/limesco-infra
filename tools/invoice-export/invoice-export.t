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
use File::Temp qw(tempdir);

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 33;

require_ok("invoice-export.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

$dbh->do("INSERT INTO account (id, period, first_name, last_name,
	street_address, postal_code, city, email, contribution)
	VALUES (NEXTVAL('account_id_seq'), '(,)', 'First Name',
	'Last Name', 'Street Address 123', '9876 BA', 'City Name',
	'test\@test.org', 2);");

my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});

is_deeply([list_invoices($lim, $account_id)], [], "No invoices for account");

my $invoice_id = '14C000001';

$dbh->do("INSERT INTO invoice (id, account_id, date, creation_time, rounded_without_taxes,
	rounded_with_taxes) VALUES (?, ?, '2014-01-01', '2014-01-01 05:06:07', '10.61',
	'13.37');", undef, $invoice_id, $account_id);

my $sth = $dbh->prepare("INSERT INTO invoice_itemline (type, invoice_id, description, taxrate,
	rounded_total, base_amount, item_price, item_count, number_of_calls, number_of_seconds,
	price_per_call, price_per_minute, service) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

$sth->execute('NORMAL', $invoice_id, 'Invoice Itemline Description', 0.26,
	13.37, undef, 10.61, 1, undef, undef, undef, undef, "VOICE");

is_deeply([list_invoices($lim, $account_id)], [{
	id => $invoice_id,
	account_id => $account_id,
	date => '2014-01-01',
	currency => 'EUR',
	creation_time => '2014-01-01 05:06:07',
	rounded_without_taxes => 10.61,
	rounded_with_taxes => 13.37,
	item_lines => [{
		id => 1,
		type => 'NORMAL',
		invoice_id => $invoice_id,
		queued_for_account_id => undef,
		description => 'Invoice Itemline Description',
		taxrate => '0.26000000',
		rounded_total => 13.37,
		base_amount => undef,
		item_price => '10.61000000',
		item_count => 1,
		number_of_calls => undef,
		number_of_seconds => undef,
		price_per_call => undef,
		price_per_minute => undef,
		service => "VOICE",
	}],
}], "One invoice for account");

my $dir = tempdir(CLEANUP => 1);
my $template_name = "$dir/template.tex";
open my $fh, '>', $template_name or die $!;
print $fh <<'EOF';
ID: \beginperl $invoice{'id'} \endperl
Account ID: \beginperl $invoice{'account_id'} \endperl
Date: \beginperl $invoice{'date'} \endperl
Rounded without taxes: \beginperl $invoice{'rounded_without_taxes'} \endperl
Rounded with taxes: \beginperl $invoice{'rounded_with_taxes'} \endperl

Account ID: \beginperl $account{'id'} \endperl
Account firstname: \beginperl $account{'first_name'} \endperl
Account lastname: \beginperl $account{'last_name'} \endperl
Account streetaddress: \beginperl $account{'street_address'} \endperl
Account postalcode: \beginperl $account{'postal_code'} \endperl
Account city: \beginperl $account{'city'} \endperl
Account email: \beginperl $account{'email'} \endperl

\beginperl
	my $i = 0;

	foreach my $itemline (@{$invoice{'item_lines'}}) {
		$i++;
		$OUT .= "Itemline $i\n";
		$OUT .= "Type: " . $itemline->{'type'} . "\n";
		$OUT .= "Invoice ID: " . $itemline->{'invoice_id'} . "\n";
		$OUT .= "Description: " . $itemline->{'description'} . "\n";
		$OUT .= "Taxrate: " . $itemline->{'taxrate'} . "\n";
		$OUT .= "Rounded total: " . $itemline->{'rounded_total'} . "\n";
		$OUT .= "Base amount: " . $itemline->{'base_amount'} . "\n";
		$OUT .= "Item price: " . $itemline->{'item_price'} . "\n";
		$OUT .= "Item count: " . $itemline->{'item_count'} . "\n";
		$OUT .= "Number of calls: " . $itemline->{'number_of_calls'} . "\n";
		$OUT .= "Number of seconds: " . $itemline->{'number_of_seconds'} . "\n";
		$OUT .= "Price per call: " . $itemline->{'price_per_call'} . "\n";
		$OUT .= "Price per minute: " . $itemline->{'price_per_minute'} . "\n";
	}
\endperl
End of input
EOF
close $fh;

my $invoice = get_invoice($lim, $invoice_id);
my $input_tex = generate_invoice_tex($lim, $invoice, $template_name);

my @lines = (
	"ID: $invoice_id",
	"Account ID: $account_id",
	"Date: 2014-01-01",
	"Rounded without taxes: 10.61",
	"Rounded with taxes: 13.37",
	"",
	"Account ID: $account_id",
	"Account firstname: First Name",
	"Account lastname: Last Name",
	"Account streetaddress: Street Address 123",
	"Account postalcode: 9876 BA",
	"Account city: City Name",
	'Account email: test@test.org',
	"",
	"Itemline 1",
	"Type: NORMAL",
	"Invoice ID: $invoice_id",
	"Description: Invoice Itemline Description",
	"Taxrate: 0.26000000",
	"Rounded total: 13.37",
	"Base amount: ",
	"Item price: 10.61000000",
	"Item count: 1",
	"Number of calls: ",
	"Number of seconds: ",
	"Price per call: ",
	"Price per minute: ",
	"",
	"End of input"
);

my $line_num = 0;
foreach(@lines) {
	my $newline_position = index($input_tex, "\n");
	my $line = substr($input_tex, 0, $newline_position);
	$input_tex = substr($input_tex, $newline_position + 1);
	is($line, $_, "Line " . (++$line_num) . " is equal");
}
is($input_tex, "", "End of input reached");

# TODO: make accounts with multiple periods, check if the right one is selected
# TODO: multiple itemlines, check sorting
# TODO: test pdf generation by writing a simple valid .tex file, no dynamic contents

$dbh->disconnect();
