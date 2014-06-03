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
use POSIX 'strftime';

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 28;

require_ok("sim-change.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl') or die $!;
initialize_database($lim);

# Create a stock SIM
my $sim;
my $exception;
try {
	$sim = create_sim($lim, {
		iccid => "89310105029090284383",
		puk => "12345678",
		state => "STOCK",
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating SIM");
ok($sim, "SIM was returned correctly");
is_deeply($sim, {
	iccid => "89310105029090284383",
	period => '[2014-02-03,)',
	state => "STOCK",
	puk => "12345678",
	owner_account_id => undef,
	data_type => undef,
	exempt_from_cost_contribution => undef,
	porting_state => undef,
	activation_invoice_id => undef,
	last_monthly_fees_invoice_id => undef,
	last_monthly_fees_month => undef,
	call_connectivity_type => undef,
	sip_realm => undef,
	sip_username => undef,
	sip_authentication_username => undef,
	sip_password => undef,
	sip_uri => undef,
	sip_expiry => undef,
	sip_trunk_password => undef,
}, "SIM was fully created");

undef $exception;
my $returned_sim;
try {
	$returned_sim = get_sim($lim, $sim->{'iccid'});
} catch {
	$exception = $_ || 1;
};
diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while retrieving SIM");
ok($returned_sim, "SIM was retrieved correctly");
is_deeply($sim, $returned_sim, "Retrieved SIM matches created SIM");

# No starting date given: period must start today
undef $sim;
undef $exception;
try {
	$sim = create_sim($lim, {
		iccid => "89310105029090284384",
		puk => "12345678",
		state => "STOCK",
	});
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating SIM with starting date today");
ok($sim, "SIM was created correctly");
is($sim->{'period'}, strftime("[%F,)", localtime(time)), "Starting date is today");

# SIM without state: throw an exception
undef $exception;
undef $sim;
try {
	$sim = create_sim($lim, {
		iccid => "89310105029090284385",
		puk => "12345678",
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(defined($exception), "Exception thrown while creating SIM without state");

undef $exception;
undef $returned_sim;
try {
	$returned_sim = get_sim($lim, "89310105029090284385");
} catch {
	$exception = $_ || 1;
};

ok(!defined($returned_sim), "No SIM returned with new ICCID");
ok(defined($exception), "Exception thrown while retrieving new SIM");

# Activated SIM with all required fields
undef $exception;
undef $sim;
try {
	$sim = create_sim($lim, {
		iccid => "89310105029090284386",
		puk => "12345678",
		state => "ACTIVATED",
		owner_account_id => 2,
		data_type => "APN_NODATA",
		exempt_from_cost_contribution => 0,
		porting_state => "NO_PORT",
		call_connectivity_type => "DIY",
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(defined($sim), "Activated SIM created with all required fields");
ok(!defined($exception), "No exception thrown while creating activated SIM with all required fields");

# Activated SIM without owner_account_id
undef $exception;
undef $sim;
try {
	$sim = create_sim($lim, {
		iccid => "89310105029090284387",
		puk => "12345678",
		state => "ACTIVATED",
		data_type => "APN_NODATA",
		exempt_from_cost_contribution => 0,
		porting_state => "NO_PORT",
		call_connectivity_type => "DIY",
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(!defined($sim), "No SIM created when state is 'ACTIVATED' and owner_account_id is missing");
ok(defined($exception), "Exception thrown while creating activated SIM without owner_account_id");

# SIM with unknown extra fields: throw an exception
undef $exception;
undef $sim;
try {
	$sim = create_sim($lim, {
		meaningless_field => "My Meaningless Field",
		iccid => "89310105029090284388",
		puk => "12345678",
		state => "ACTIVATED",
		owner_account_id => 2,
		data_type => "APN_NODATA",
		exempt_from_cost_contribution => 0,
		porting_state => "NO_PORT",
		call_connectivity_type => "DIY",
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(!defined($sim), "No SIM created when unknown field is given");
ok(defined($exception), "Exception thrown when SIM was created with unknown field in it");

# Retrieving, modifying and deleting is thoroughly tested in account-change,
# which uses the same base methods as sim-change, so we only have to test a
# little bit.

undef $exception;
undef $returned_sim;
try {
	$returned_sim = get_sim($lim, "89310105029090284383");
} catch {
	$exception = $_ || 1;
};

ok(defined($returned_sim), "Retrieved first created SIM correctly");
ok(!defined($exception), "No exception thrown while retrieving first SIM");

### Setting a STOCK SIM to activated ###
$exception = undef;
try {
	update_sim($lim, "89310105029090284383", {
		state => "ACTIVATED",
		owner_account_id => 2,
		data_type => "APN_NODATA",
		exempt_from_cost_contribution => 0,
		porting_state => "NO_PORT",
		call_connectivity_type => "DIY",
	}, '2014-03-10');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while setting SIM to activated");
is_deeply(get_sim($lim, "89310105029090284383"), {
	iccid => "89310105029090284383",
	period => '[2014-03-10,)',
	state => "ACTIVATED",
	puk => "12345678",
	owner_account_id => 2,
	data_type => "APN_NODATA",
	exempt_from_cost_contribution => 0,
	porting_state => "NO_PORT",
	call_connectivity_type => "DIY",
	activation_invoice_id => undef,
	last_monthly_fees_invoice_id => undef,
	last_monthly_fees_month => undef,
	sip_realm => undef,
	sip_username => undef,
	sip_authentication_username => undef,
	sip_password => undef,
	sip_uri => undef,
	sip_expiry => undef,
	sip_trunk_password => undef,
}, "SIM correctly activated");

is_deeply(get_sim($lim, "89310105029090284383", '2014-03-09'), {
	iccid => "89310105029090284383",
	period => '[2014-02-03,2014-03-10)',
	state => "STOCK",
	puk => "12345678",
	owner_account_id => undef,
	data_type => undef,
	exempt_from_cost_contribution => undef,
	porting_state => undef,
	activation_invoice_id => undef,
	last_monthly_fees_invoice_id => undef,
	last_monthly_fees_month => undef,
	call_connectivity_type => undef,
	sip_realm => undef,
	sip_username => undef,
	sip_authentication_username => undef,
	sip_password => undef,
	sip_uri => undef,
	sip_expiry => undef,
	sip_trunk_password => undef,
}, "Old SIM period was updated, no other changes");

# Try to delete SIM
$exception = undef;
try {
	delete_sim($lim, "89310105029090284383", '2014-03-12');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown when trying to delete SIM in latest record");

undef $exception;
undef $sim;
try {
	$sim = get_sim($lim, "89310105029090284383");
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown while trying to fetch SIM in latest record");
ok(!defined($sim), "SIM undefined");

is_deeply(get_sim($lim, "89310105029090284383", '2014-03-11'), {
	iccid => "89310105029090284383",
	period => '[2014-03-10,2014-03-12)',
	state => "ACTIVATED",
	puk => "12345678",
	owner_account_id => 2,
	data_type => "APN_NODATA",
	exempt_from_cost_contribution => 0,
	porting_state => "NO_PORT",
	call_connectivity_type => "DIY",
	activation_invoice_id => undef,
	last_monthly_fees_invoice_id => undef,
	last_monthly_fees_month => undef,
	sip_realm => undef,
	sip_username => undef,
	sip_authentication_username => undef,
	sip_password => undef,
	sip_uri => undef,
	sip_expiry => undef,
	sip_trunk_password => undef,
}, "SIM still exists before deletion");

$dbh->disconnect;
