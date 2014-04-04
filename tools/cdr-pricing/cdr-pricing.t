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
plan tests => 9;

require_ok("cdr-pricing.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

my $insert_cdr = $dbh->prepare("INSERT INTO cdr (service, call_id, \"from\", \"to\", speakup_account, time, pricing_info,
	computed_cost, computed_price, units, connected, source, destination, direction, leg, reason)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

# empty table: no unpricables
my $unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 0, "No unpriced CDRs in empty database");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-01 01:00:00", "{}", 0.01, 0.01, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 0, "No unpriced CDRs in database with priced CDR");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-02 01:00:00", undef, undef, undef, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 1, "One unpriced CDR in database with priced and unpriced CDR");

$insert_cdr->execute("VOICE", "foo", "31600000000", "0", "suaccount", "2012-01-03 01:00:00", undef, undef, undef, 1, "t", "Test Source",
	"Test Destination", "OUT", undef, undef);
$unpricable = 0;
for_every_unpriced_cdr($lim, sub { ++$unpricable });
is($unpricable, 2, "Two unpriced CDRs in database with priced and unpriced CDRs");

$dbh->do("TRUNCATE cdr RESTART IDENTITY;");

$insert_cdr->execute("VOICE", "quux", "31600000000", "0", "suaccount", "2012-01-02 01:00:00", undef, undef, undef, 5, "t", "Test Source",
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

$dbh->disconnect();
