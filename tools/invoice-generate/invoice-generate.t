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
plan tests => 10;

require_ok("invoice-generate.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

sub dt {
	return DateTime->new(year => $_[0], month => $_[1], day => $_[2]);
}

my $sth = $dbh->prepare("INSERT INTO invoice (id, account_id, date, creation_time, rounded_without_taxes, rounded_with_taxes)"
	." VALUES (?, 1, ?, 'now', 0, 0)");

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

$dbh->disconnect();
