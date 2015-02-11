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
use DateTime;

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 30;

require_ok("invoice-generate.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

sub dt {
	return DateTime->new(year => $_[0], month => $_[1], day => $_[2]);
}

my $sth = $dbh->prepare("INSERT INTO invoice (id, account_id, date, creation_time, rounded_without_taxes, rounded_with_taxes)"
	." VALUES (?, 10, ?, 'now', 0, 0)");

#### find_next_invoice_id

is(find_next_invoice_id($dbh, dt("2012","01","01")), "12C000001", "Invoice ID generated correctly");
$sth->execute("12C000001", "2012-01-01");
is(find_next_invoice_id($dbh, dt("2012","01","01")), "12C000002", "Invoice ID generated correctly");
$sth->execute("12C000002", "2012-01-01");
is(find_next_invoice_id($dbh, dt("2012","01","01")), "12C000003", "Invoice ID generated correctly");
$sth->execute("12C000003", "2012-01-01");
is(find_next_invoice_id($dbh, dt("2012","06","07")), "12C000004", "Invoice ID generated correctly");
$sth->execute("12C000004", "2012-06-07");
is(find_next_invoice_id($dbh, dt("2013","01","01")), "13C000001", "Invoice ID generated correctly");
$sth->execute("13C000001", "2013-01-01");
is(find_next_invoice_id($dbh, dt("2013","01","02")), "13C000002", "Invoice ID generated correctly");
$sth->execute("13C000002", "2013-01-02");
is(find_next_invoice_id($dbh, dt("2013","01","03")), "13C000003", "Invoice ID generated correctly");
$sth->execute("13C000003", "2013-01-03");
is(find_next_invoice_id($dbh, dt("2013","01","01")), "13C000004", "Invoice ID generated correctly");
$sth->execute("13C000004", "2013-01-01");

my $exception;
try {
	find_next_invoice_id($dbh, dt("2012","12","01"));
} catch {
	$exception = $_ || 1;
};
ok($exception, "Exception thrown when generating invoice IDs in the wrong order");

#### get_all_active_account_ids

require('../account-change/account-change.pl');

my $account = create_account($lim, {
	first_name => "First Name",
	last_name => "Last Name",
	street_address => "Street Address 22",
	postal_code => "Postal Code",
	city => "City",
	email => 'test@limesco.nl',
	contribution => 2,
}, '2014-08-01');

is_deeply([get_all_active_account_ids($lim, '2014-07-29')], [], "No active accounts yet");
is_deeply([get_all_active_account_ids($lim, '2014-08-01')], [$account->{'id'}], "One active account");

delete_account($lim, $account->{'id'}, '2014-08-30');

is_deeply([get_all_active_account_ids($lim, '2014-08-01')], [$account->{'id'}], "One active account");
is_deeply([get_all_active_account_ids($lim, '2014-08-30')], [], "No active accounts anymore");

### {first,last}_day_of_{next,this}_month

is(first_day_of_next_month(dt("2014", "02", "20"))->ymd, "2014-03-01", "2014-02-20 -> 2014-03-01");
is(first_day_of_next_month(dt("2014", "02", "01"))->ymd, "2014-03-01", "2014-02-01 -> 2014-03-01");
is(first_day_of_next_month(dt("2014", "12", "30"))->ymd, "2015-01-01", "2014-12-30 -> 2015-01-01");

is(last_day_of_this_month(dt("2014", "02", "20"))->ymd, "2014-02-28", "2014-02-20 -> 2014-02-28");
is(last_day_of_this_month(dt("2014", "02", "28"))->ymd, "2014-02-28", "2014-02-28 -> 2014-02-28");
is(last_day_of_this_month(dt("2014", "12", "01"))->ymd, "2014-12-31", "2014-12-01 -> 2014-12-31");

### Nothing to invoice: generate_invoice returns undef

my $invoice;
undef $exception;
try {
	$invoice = generate_invoice($lim, $account->{'id'}, dt('2014', '08', '15'));
} catch {
	$exception = $_ || 1;
};

diag($exception) if $exception;
ok(!$exception, "No exception thrown during generation of invoice");
ok(!defined($invoice), "Invoice was empty");

### add_queued_itemline

undef $exception;
try {
	add_queued_itemline($lim, {
		type => "NORMAL",
		queued_for_account_id => $account->{'id'},
		description => "Test queued itemline",
		taxrate => 0.50,
		rounded_total => 1.50,
		item_price => 1.50,
		item_count => 1,
	});
} catch {
	$exception = $_ || 1;
};

diag($exception) if $exception;
ok(!$exception, "No exception thrown while adding queued itemline");

undef $invoice;
undef $exception;
try {
	$invoice = generate_invoice($lim, $account->{'id'}, dt('2014', '08', '15'));
} catch {
	$exception = $_ || 1;
};

diag($exception) if $exception;
ok(!$exception, "No exception thrown during generation of invoice");
is($invoice, "14C000001", "Invoice was correctly generated");

undef $exception;
try {
	my $sth = $dbh->prepare("SELECT * FROM invoice WHERE id='14C000001'");
	$sth->execute;
	$invoice = $sth->fetchrow_hashref;
} catch {
	$exception = $_ || 1;
};

ok(!$exception, "Invoice could be retrieved");
is_deeply($invoice, {
	id => "14C000001",
	account_id => $account->{'id'},
	currency => "EUR",
	date => "2014-08-15",
	creation_time => $invoice->{'creation_time'},
	rounded_without_taxes => '1.50',
	rounded_with_taxes => '2.25',
}, "Invoice created correctly");

undef $exception;
my $itemline;
my $taxline;
try {
	my $sth = $dbh->prepare("SELECT * FROM invoice_itemline WHERE invoice_id='14C000001' ORDER BY type");
	$sth->execute;
	$itemline = $sth->fetchrow_hashref;
	$taxline = $sth->fetchrow_hashref;
	if($sth->fetchrow_hashref) {
		die "More than two itemlines on the invoice";
	}
} catch {
	$exception = $_ || 1;
};

ok(!$exception, "No exception thrown while retrieving invoice itemlines");
is_deeply($itemline, {
	id => $itemline->{'id'},
	type => "NORMAL",
	queued_for_account_id => undef,
	invoice_id => '14C000001',
	description => "Test queued itemline",
	taxrate => '0.50000000',
	rounded_total => '1.50',
	base_amount => undef,
	item_price => '1.50000000',
	item_count => 1,
	number_of_calls => undef,
	number_of_seconds => undef,
	price_per_call => undef,
	price_per_minute => undef,
	service => undef,
}, "Itemline is OK");

is_deeply($taxline, {
	id => $taxline->{'id'},
	type => "TAX",
	queued_for_account_id => undef,
	invoice_id => '14C000001',
	description => "Tax",
	taxrate => '0.50000000',
	rounded_total => '0.75',
	base_amount => '1.50000000',
	item_price => '0.75000000',
	item_count => 1,
	number_of_calls => undef,
	number_of_seconds => undef,
	price_per_call => undef,
	price_per_minute => undef,
	service => undef,
}, "Tax line is OK");

$dbh->disconnect();
