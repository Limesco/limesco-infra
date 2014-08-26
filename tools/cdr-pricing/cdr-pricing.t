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
plan tests => 13;

require_ok("cdr-pricing.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

# Insert a single pricing
$dbh->do("INSERT INTO pricing (period, description, hidden, service, call_connectivity_type, source, destination, direction, connected,
		cost_per_line, cost_per_unit, price_per_line, price_per_unit, legreason) VALUES ('(,)', 'Test pricing', 'f', 'VOICE', '{}', '{First Test Source}', '{}',
		'{}', '{}', 0, 0, 0, 0, '{}')");
my $null_pricing_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "pricing_id_seq"});
my $insert_cdr = $dbh->prepare("INSERT INTO cdr (service, call_id, \"from\", \"to\", speakup_account, time, pricing_id, pricing_info,
	computed_cost, computed_price, units, connected, source, destination, direction, leg, reason)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

# empty table: no unpricables
my $unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 0, "No unpriced CDRs in empty database");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-01 01:00:00", $null_pricing_id, "{}", 0.01, 0.01, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 0, "No unpriced CDRs in database with priced CDR");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-02 01:00:00", undef, undef, undef, undef, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 1, "One unpriced CDR in database with priced and unpriced CDR");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-03 01:00:00", undef, undef, undef, undef, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 2, "Two unpriced CDRs in database with priced and unpriced CDRs");

$dbh->do("TRUNCATE cdr RESTART IDENTITY;");

$insert_cdr->execute("VOICE", "quux", "31600000000", "0", "suaccount", "2012-01-02 01:00:00", undef, undef, undef, undef, 5, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
my $cdr_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "cdr_id_seq"});
for_every_unpriced_cdr($lim, sub {
	my (undef, $cdr) = @_;
	is_deeply($cdr, {
		id => $cdr_id,
		service => "VOICE",
		call_id => "quux",
		from => "31600000000",
		to => "0",
		speakup_account => "suaccount",
		time => "2012-01-02 01:00:00",
		pricing_id => undef,
		pricing_id_two => undef,
		pricing_info => undef,
		computed_cost => undef,
		computed_price => undef,
		invoice_id => undef,
		units => 5,
		connected => "1",
		source => "Test Source",
		destination => "Test Destination",
		direction => "OUT",
		leg => undef,
		reason => undef,
	}, "Retrieved CDR is the same as entered CDR");
});

# No pricings: CDR is unpricable
my $unpriced_cdr_tried = 0;
my $unpriced_cdr_priced = 0;
my $exception;
try {
	for_every_unpriced_cdr($lim, sub {
		my (undef, $cdr) = @_;
		$unpriced_cdr_tried++;
		price_cdr($lim, $cdr);
		$unpriced_cdr_priced++;
	});
} catch {
	$exception = $_;
};

is($unpriced_cdr_tried, 1, "Unpriced CDR was tried for pricing");
is($unpriced_cdr_priced, 0, "Unpriced CDR was unpricable");
like($exception, qr/\bunpricable\b/, "Unpricable CDR error contains 'unpricable'");

my $insert_pricing = $dbh->prepare("INSERT INTO pricing (period, description, service, hidden,
	call_connectivity_type, source, destination, direction, connected, cost_per_line, cost_per_unit, price_per_line, price_per_unit, legreason)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')");
$insert_pricing->execute("(,)", "Some Description", "VOICE", "f", ["OOTB"], ["Test Source"], ["Test Destination"], ["OUT"], ["t"], 1, 10, 2, 20);
my $pricing_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "pricing_id_seq"});

# without an externalAccount mapping, and without a SIM, this CDR can't be mapped to the right call_connectivity_type
# so this CDR should still be unpricable
$unpriced_cdr_priced = 0;
try {
	for_every_unpriced_cdr($lim, sub {
		price_cdr($lim, $_[1]);
		$unpriced_cdr_priced++;
	});
};
is($unpriced_cdr_priced, 0, "Unpriced CDR still unpricable");

# Add an account, SIM and externalAccount mapping so the CDR is pricable
$dbh->do("INSERT INTO account (id, period, first_name, last_name,
	street_address, postal_code, city, email)
	VALUES (NEXTVAL('account_id_seq'), '(,)', 'First Name',
	'Last Name', 'Street Address 123', '9876 BA', 'City Name',
	'test\@test.org');");
my $account_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "account_id_seq"});
my $sim_iccid = '123456789012345678';
$dbh->do("INSERT INTO sim (iccid, period, state, puk, owner_account_id,
	data_type, exempt_from_cost_contribution, call_connectivity_type)
	VALUES (?, '(,)', 'ACTIVATED', '123456', ?,
	'APN_NODATA', 'f', 'OOTB');", undef, $sim_iccid, $account_id);
$dbh->do("INSERT INTO phonenumber (phonenumber, period, sim_iccid)
	VALUES ('31600000000', '(,)', ?)", undef, $sim_iccid);
$dbh->do("INSERT INTO speakup_account (name, period, account_id) VALUES
	('suaccount', '(,)', ?)", undef, $account_id);

$unpriced_cdr_priced = 0;
my $cdr;
try {
	for_every_unpriced_cdr($lim, sub {
		$cdr = $_[1];
		price_cdr($lim, $cdr);
		$unpriced_cdr_priced++;
	});
} catch {
	diag("Unexpected exception: $_");
};

is($unpriced_cdr_priced, 1, "Unpriced CDR now priced");
ok($cdr->{'pricing_info'}, "Unpriced CDR has pricing information");
# delete the freeform 'info' parameter, so we can is_deeply
delete $cdr->{'pricing_info'};
is_deeply($cdr, {
	id => $cdr_id,
	service => "VOICE",
	call_id => "quux",
	from => "31600000000",
	to => "0",
	speakup_account => "suaccount",
	time => "2012-01-02 01:00:00",
	pricing_id => $pricing_id,
	pricing_id_two => undef,
	computed_cost => 51, # 1 per line, 10 per unit, 5 units
	computed_price => 102, # 2 per line, 20 per unit, 5 units
	invoice_id => undef,
	units => 5,
	connected => "1",
	source => "Test Source",
	destination => "Test Destination",
	direction => "OUT",
	leg => undef,
	reason => undef,
}, "Priced CDR is exactly as expected");

$dbh->disconnect();
