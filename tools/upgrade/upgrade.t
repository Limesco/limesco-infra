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
plan tests => 31;

require_ok("upgrade.pl");
ok(get_latest_schema_version(), "Latest schema version is a true value");
ok( daterange_in("[2014-02-10, 2014-02-14)", "2014-02-12"), "12 feb is in range");
ok( daterange_in("[2014-02-10, 2014-02-14)", "2014-02-10"), "10 feb is in range");
ok(!daterange_in("[2014-02-10, 2014-02-14)", "2014-02-09"), "9 feb is before range");
ok(!daterange_in("[2014-02-10, 2014-02-14)", "2014-02-15"), "15 feb is after range");
ok(!daterange_in("[2014-02-10, 2014-02-14)", "2014-02-14"), "14 feb is after range");
ok( daterange_in("[2014-02-10,)", "2014-02-15"), "15 feb is in infinite range");
ok( daterange_in("[2014-02-10,)", "2014-02-10"), "10 feb is in infinite range");
ok(!daterange_in("[2014-02-10,)", "2014-02-09"), "9 feb is before infinite range");
ok(!daterange_in("[2014-02-10, 2014-02-14)", "2013-02-12"), "2013 is before range");
ok(!daterange_in("[2014-02-10, 2014-02-14)", "2014-01-12"), "january is before range");
ok( daterange_in("[2014-02-10,)", "2015-01-09"), "2015 is in infinite range");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

# table is empty
my $current_schema_version;
try {
	$current_schema_version = get_current_schema_version($lim, '2014-02-12');
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
ok(defined $current_schema_version && $current_schema_version == 0, "Database is uninitialized");

$dbh->do("SET LOCAL client_min_messages='WARNING';");
# leave out exclusion constraints to ease testing
$dbh->do("CREATE TABLE meta (period DATERANGE, schema_version INT);");

# table exists but is empty -> throws
try {
	get_current_schema_version($lim, '2014-02-12');
	fail("throws");
} catch {
	pass("throws");
};

# table exists and has one current row -> returns that schema_version
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-10,)', 4);");
undef $current_schema_version;
try {
	$current_schema_version = get_current_schema_version($lim, '2014-02-12');
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
ok(defined $current_schema_version && $current_schema_version == 4, "Current schema version returned");

# table exists and has one current row and one history row -> returns that schema_version
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-04, 2014-02-10)', 3);");
undef $current_schema_version;
try {
	$current_schema_version = get_current_schema_version($lim, '2014-02-12');
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
ok(defined $current_schema_version && $current_schema_version == 4, "Current schema version returned");

# table exists and has two current rows -> throws
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-11,)', 5);");
try {
	get_current_schema_version($lim, '2014-02-12');
	fail("throws");
} catch {
	pass("throws");
};

# table exists and has one current row and one future row -> throws
$dbh->do("DELETE FROM meta;");
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-10, 2014-02-14)', 4)");
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-14,)', 5)");
try {
	get_current_schema_version($lim, '2014-02-12');
	fail("throws");
} catch {
	pass("throws");
};

# table exists but has only rows in the past -> throws
$dbh->do("DELETE FROM meta;");
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-04, 2014-02-10)', 3)");
try {
	get_current_schema_version($lim, '2014-02-12');
	fail("throws");
} catch {
	pass("throws");
};

# table exists but has only rows in the future -> throws
$dbh->do("DELETE FROM meta;");
$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[2014-02-14,)', 5)");
try {
	get_current_schema_version($lim, '2014-02-12');
	fail("throws");
} catch {
	pass("throws");
};

# initialize_database
$dbh->do("DROP TABLE meta");
try {
	initialize_database($lim);
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
undef $current_schema_version;
try {
	$current_schema_version = get_current_schema_version($lim);
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
ok(defined $current_schema_version && $current_schema_version > 0, "current schema version is set");
ok($current_schema_version == get_latest_schema_version(), "schema is initialized to latest version");

try {
	update_schema_version($lim, $dbh, 4);
	pass("update didn't throw");
} catch {
	diag($_);
	fail("update didn't throw");
};

undef $current_schema_version;
try {
	$current_schema_version = get_current_schema_version($lim);
	pass("didn't throw");
} catch {
	fail("didn't throw");
};
ok(defined $current_schema_version && $current_schema_version == 4, "current schema version is set");

$dbh->disconnect();
