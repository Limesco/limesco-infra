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
plan tests => 86;

require_ok("account-change.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl') or die $!;
initialize_database($lim);

# Create a full account
my $account;
my $exception;
try {
	$account = create_account($lim, {
		company_name => "Test Company Name",
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 0,
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating account");
ok($account, "Account was returned correctly");
my $accountid1 = $account->{'id'};
is_deeply($account, {
	id => $accountid1,
	period => '[2014-02-03,)',
	company_name => "Test Company Name",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => '0',
}, "Account was fully created");

undef $exception;
my $returned_account;
try {
	$returned_account = get_account($lim, $account->{'id'});
} catch {
	$exception = $_ || 1;
};
diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while retrieving account");
ok($returned_account, "Account was retrieved correctly");
is_deeply($account, $returned_account, "Retrieved account matches created account");

# No starting date given: period must start today
undef $account;
undef $exception;
try {
	$account = create_account($lim, {
		company_name => "Test Company Name",
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 0,
	});
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating account with starting date today");
ok($account, "Account was created correctly");
my $accountid2 = $account->{'id'};
my $period2 = strftime("[%F,)", localtime(time));
is($account->{'period'}, $period2, "Starting date is today");

# Account without company name: just have it unset
undef $exception;
try {
	$account = create_account($lim, {
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 1,
	}, '2014-03-03');
} catch {
	$exception = $_ || 1;
};
diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating account without company_name");
my $accountid3 = $account->{'id'};
isnt($account->{'id'}, $returned_account->{'id'}, "ID of new account is different");
is_deeply($account, {
	id => $accountid3,
	period => '[2014-03-03,)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => '1',
}, "Account without company name was fully created");

# Account without password and admin bit
undef $exception;
try {
	$account = create_account($lim, {
		company_name => undef,
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
	}, '2014-03-03');
} catch {
	$exception = $_ || 1;
};
diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while creating account without optional bits");
my $accountid4 = $account->{'id'};
is_deeply($account, {
	id => $accountid4,
	period => '[2014-03-03,)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}, "Account without optional bits was fully created");

# This must be the last account that is succesfully created in this test
my $account_id = $accountid4;

# Account without name information: throw an exception
undef $exception;
undef $account;
try {
	$account = create_account($lim, {
		company_name => "Test Company Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 0,
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(defined($exception), "Exception thrown while creating account without name information");

undef $exception;
undef $returned_account;
try {
	$returned_account = get_account($lim, $account_id + 1);
} catch {
	$exception = $_ || 1;
};

ok(!defined($returned_account), "No account returned with new account ID");
ok(defined($exception), "Exception thrown while retrieving new account ID");

# Account with empty address information: throw an exception
undef $exception;
undef $account;
try {
	$account = create_account($lim, {
		company_name => "Test Company Name",
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 0,
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(!defined($account), "No account created when street address is empty");
ok(defined($exception), "Exception thrown when account was created with empty street address");

# Account with unknown extra fields: throw an exception
undef $exception;
undef $account;
try {
	$account = create_account($lim, {
		meaningless_field => "My Meaningless Field",
		company_name => "Test Company Name",
		first_name => "Test First Name",
		last_name => "Test Last Name",
		street_address => "Test Street Address",
		postal_code => "Test Postal Code",
		city => "Test City",
		email => 'testemail@limesco.nl',
		password_hash => "",
		admin => 0,
	}, '2014-02-03');
} catch {
	$exception = $_ || 1;
};

ok(!defined($account), "No account created when unknown field is given");
ok(defined($exception), "Exception thrown when account was created with unknown field in it");

# Retrieving all accounts
my @accounts = list_accounts($lim);
is_deeply(\@accounts,
[{
	id => $accountid1,
	period => '[2014-02-03,)',
	company_name => "Test Company Name",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => '0',
}, {
	id => $accountid2,
	period => $period2,
	company_name => "Test Company Name",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => 0,
}, {
	id => $accountid3,
	period => '[2014-03-03,)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => '1',
}, {
	id => $accountid4,
	period => '[2014-03-03,)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}], "list_accounts returns all four accounts just fine");
@accounts = list_accounts($lim, "2014-02-03");
is_deeply(\@accounts,
[{
	id => $accountid1,
	period => '[2014-02-03,)',
	company_name => "Test Company Name",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => '0',
}], "list_accounts returns only oldest account when date is given");

# Retrieving accounts with date
$account = get_account($lim, $account_id);
undef $exception;
try {
	$returned_account = get_account($lim, $account->{'id'}, '2014-03-04');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown when retrieving account with date");
is_deeply($returned_account, $account, "Retrieved account at 2014-03-04 is still the same");

undef $exception;
try {
	$returned_account = get_account($lim, $account->{'id'}, '2014-03-03');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown when retrieving account with date");
is_deeply($returned_account, $account, "Retrieved account at 2014-03-03 is still the same");

undef $exception;
undef $returned_account;
try {
	$returned_account = get_account($lim, $account->{'id'}, '2014-03-02');
} catch {
	$exception = $_ || 1;
};

ok(!defined($returned_account), "Retrieved account at 2014-03-02 does not exist");
ok($exception, "Exception thrown when retrieving account with date");

### Updating an account ###

# Double-check that the account didn't change
is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-03,)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}, "Account is still completely the same");

# Partial property changes
$exception = undef;
try {
	update_account($lim, $account_id, {
		company_name => "My Test Company",
	}, '2014-03-10');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while changing company name");
is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-10,)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}, "Company name changed");

is_deeply(get_account($lim, $account_id, '2014-03-09'), {
	id => $account_id,
	period => '[2014-03-03,2014-03-10)',
	company_name => undef,
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}, "Old account period was updated, no other changes");

# See if the updates are OK
is_deeply([account_changes_between($lim, $account_id, '2014-03-01', '2014-03-02')],
	[], "No account changes before it was created");
is_deeply([account_changes_between($lim, $account_id, '2014-03-02', '2014-03-03')],
	[get_account($lim, $account_id, '2014-03-09')], "Account creation is returned when asking for changes over account creation date");
my $account_change = {
	period => '[2014-03-10,)',
	company_name => 'My Test Company',
};
is_deeply([account_changes_between($lim, $account_id, '2014-03-10', '2014-03-10')],
	[$account_change], "Account modifications are returned when asking for changes over account modification date");
is_deeply([account_changes_between($lim, $account_id, '2014-03-02', '2014-03-10')],
	[get_account($lim, $account_id, '2014-03-09'), $account_change],
	"Account creation and modification are both returned when asking for changes over both dates");
is_deeply([account_changes_between($lim, $account_id, '2014-03-11', '2014-03-12')],
	[], "No account changes after it was modified");

# 'undef' as either of the two fields means infinitely
is_deeply([account_changes_between($lim, $account_id, undef, '2014-03-02')],
	[], "No account changes before it was created (infinitely)");
is_deeply([account_changes_between($lim, $account_id, '2014-03-11', undef)],
	[], "No account changes after it was modified (infinitely)");
is_deeply([account_changes_between($lim, $account_id, undef, '2014-03-03')],
	[get_account($lim, $account_id, '2014-03-09')], "Account creation is returned (infinitely)");
is_deeply([account_changes_between($lim, $account_id, '2014-03-10', undef)],
	[$account_change], "Account modifications are returned (infinitely)");
is_deeply([account_changes_between($lim, $account_id, undef, undef)],
	[get_account($lim, $account_id, '2014-03-09'), $account_change], "All changes are returned (infinitely)");

# Two properties at a time
$exception = undef;
try {
	update_account($lim, $account_id, {
		email => 'testmailtwo@limesco.nl',
		admin => 1,
	}, '2014-03-12');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while changing two properties at once");
is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-12,)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testmailtwo@limesco.nl',
	password_hash => undef,
	admin => 1,
}, "Two properties changed");

# Another property at the same date
$exception = undef;
try {
	update_account($lim, $account_id, {
		admin => 0,
		city => "Another City",
	}, '2014-03-12');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while changing property with the same date");
is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-12,)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Another City",
	email => 'testmailtwo@limesco.nl',
	password_hash => undef,
	admin => 0,
}, "Two properties changed");

# account_changes_between should act fine now too
# (admin was updated back to 0, so it's not seen as a change)
is_deeply([account_changes_between($lim, $account_id, '2014-03-11', '2014-03-12')],
	[{
		email => 'testmailtwo@limesco.nl',
		city => 'Another City',
		period => '[2014-03-12,)',
	}], "Account modifications are returned correctly");

# Properties that can't be changed
$exception = undef;
try {
	update_account($lim, $account_id, {
		id => 5,
		company_name => "My Test Company 2",
	}, '2014-03-14');
} catch {
	$exception = $_ || 1;
};
ok($exception, "Exception thrown when changing id of account");

$exception = undef;
try {
	update_account($lim, $account_id, {
		period => '[2014-03-16,)',
		company_name => "My Test Company 3",
	}, '2014-03-15');
} catch {
	$exception = $_ || 1;
};
ok($exception, "Exception thrown when changing period of account");

$exception = undef;
try {
	update_account($lim, $account_id, {
		nonexistant_property => 'foo',
		company_name => "My Test Company 4",
	}, '2014-03-16');
} catch {
	$exception = $_ || 1;
};
ok($exception, "Exception thrown when changing nonexistant property of account");

is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-12,)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Another City",
	email => 'testmailtwo@limesco.nl',
	password_hash => undef,
	admin => 0,
}, "No properties changed");

# Make a change in latest record
$exception = undef;
try {
	update_account($lim, $account_id, {
		street_address => "My New Street Address",
	}, '2014-03-12');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown while adding street name to latest changes");
is_deeply(get_account($lim, $account_id), {
	id => $account_id,
	period => '[2014-03-12,)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "My New Street Address",
	postal_code => "Test Postal Code",
	city => "Another City",
	email => 'testmailtwo@limesco.nl',
	password_hash => undef,
	admin => 0,
}, "Street name changed");

# Try to delete an account in history
$exception = undef;
try {
	delete_account($lim, $account_id, '2014-01-01');
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown when trying to delete account in history");

$exception = undef;
try {
	delete_account($lim, $account_id, '2014-03-11');
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown when trying to delete account in history");

$exception = undef;
try {
	delete_account($lim, $account_id, '2014-03-12');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!defined($exception), "No exception thrown when trying to delete account in latest record");

undef $exception;
undef $account;
try {
	$account = get_account($lim, $account_id);
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown while trying to fetch account in latest record");
ok(!defined($account), "Account undefined");

is_deeply(get_account($lim, $account_id, '2014-03-11'), {
	id => $account_id,
	period => '[2014-03-10,2014-03-12)',
	company_name => "My Test Company",
	first_name => "Test First Name",
	last_name => "Test Last Name",
	street_address => "Test Street Address",
	postal_code => "Test Postal Code",
	city => "Test City",
	email => 'testemail@limesco.nl',
	password_hash => undef,
	admin => '0',
}, "Account still exists before deletion");

undef $exception;
undef $account;
try {
	$account = get_account($lim, $account_id, '2014-03-12');
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown while trying to fetch account on date 2014-03-12");
ok(!defined($account), "Account undefined");

## Test historical changes using new objects ##
undef $account_id;
$account = create_account($lim, {
	company_name => "Company Before Change",
	first_name => "First Name Before Change",
	last_name => "Last Name",
	street_address => "Street Address",
	postal_code => "Postal Code",
	city => "City",
	email => 'testemail@limesco.nl',
	password_hash => "",
	admin => 0,
}, '2014-06-01');

update_account($lim, $account->{'id'}, {
	company_name => "Company After Change",
}, '2014-06-05');

# now, the [2014-06-01, 2014-06-05) record is historical.
# if we change the company_name, it should change until 2014-06-05.

undef $exception;
try {
	update_account($lim, $account->{'id'}, {
		company_name => "Company During Change",
	}, '2014-06-03');
} catch {
	$exception = $_ || 1;
};

ok(!$exception, "Update succeeded");
is(get_account($lim, $account->{'id'}, '2014-06-01')->{'company_name'},
	"Company Before Change", "Original company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-03')->{'company_name'},
	"Company During Change", "Updated company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'company_name'},
	"Company After Change", "Final company name is OK");

# Now, check if historical changes are propagated correctly

update_account($lim, $account->{'id'}, {
	company_name => "Company After Third Change",
}, '2014-06-09');
update_account($lim, $account->{'id'}, {
	company_name => "Company After Second Change",
}, '2014-06-07');

undef $exception;
try {
	update_account($lim, $account->{'id'}, {
		first_name => "First Name After Change",
	}, '2014-06-02');
} catch {
	$exception = $_ || 1;
};

ok(!$exception, "Update succeeded");
is(get_account($lim, $account->{'id'}, '2014-06-01')->{'company_name'},
	"Company Before Change", "Original company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-02')->{'company_name'},
	"Company Before Change", "Original company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-03')->{'company_name'},
	"Company During Change", "Updated company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'company_name'},
	"Company After Change", "Final company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-07')->{'company_name'},
	"Company After Second Change", "Second company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-09')->{'company_name'},
	"Company After Third Change", "Third company name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-01')->{'first_name'},
	"First Name Before Change", "Original name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-02')->{'first_name'},
	"First Name After Change", "Updated name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-03')->{'first_name'},
	"First Name After Change", "Propagated name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'first_name'},
	"First Name After Change", "Final name stays OK");
is(get_account($lim, $account->{'id'}, '2014-06-07')->{'first_name'},
	"First Name After Change", "Final name stays OK");
is(get_account($lim, $account->{'id'}, '2014-06-09')->{'first_name'},
	"First Name After Change", "Final name stays OK");
is(get_account($lim, $account->{'id'}, '2014-06-01')->{'last_name'},
	"Last Name", "Last name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-02')->{'last_name'},
	"Last Name", "Last name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-03')->{'last_name'},
	"Last Name", "Last name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'last_name'},
	"Last Name", "Last name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'last_name'},
	"Last Name", "Last name is OK");
is(get_account($lim, $account->{'id'}, '2014-06-05')->{'last_name'},
	"Last Name", "Last name is OK");

$dbh->disconnect;
