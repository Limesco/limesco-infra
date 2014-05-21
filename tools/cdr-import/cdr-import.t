#!/usr/bin/perl
use strict;
use warnings;

use Test::PostgreSQL;
use Test::HTTP::Server;
use Test::More;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use DateTime;
use Try::Tiny;
use HTTP::Request;
use URI qw();

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
my $httpd = Test::HTTP::Server->new() or plan skip_all => "HTTP server failed to start";
plan tests => 30;

require_ok("cdr-import.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

## Dates to retrieve ##
my $cdri_sth = $dbh->prepare("INSERT INTO cdrimports (cdr_date, import_time, error) VALUES (?, ?, ?);");

# 2013-03-01: already imported fine (no need to update)
$cdri_sth->execute('2013-03-01', '2013-03-05 00:00:00', undef);
# 2013-03-02: last updated within 48h (should update again today)
$cdri_sth->execute('2013-03-02', '2013-03-02 23:30:00', undef);
# 2013-03-03: >48h, but an error accured
$cdri_sth->execute('2013-03-03', '2013-03-06 01:00:00', 'Something happened');
# 2013-03-04: imported fine
$cdri_sth->execute('2013-03-04', '2013-03-06 01:00:00', undef);
# 2013-03-05: no mention

my @dates = get_cdr_import_dates($lim, '2013-03-06');
is_deeply(\@dates, ['2013-03-02', '2013-03-03', '2013-03-05', '2013-03-06'], "Dates to retrieve computed fine");

## Logging in ##
sub Test::HTTP::Server::Request::login {
	my $response = $_[0];
	my $request_data = $response->{'head'} . $response->{'body'};
	my $request = HTTP::Request->parse($request_data);

	my $error = sub {
		$response->out_response("500 $_[0]");
		$response->out_headers(
			'Content-Type' => 'text/plain',
		);
		$response->out_body($_[0]);
	};

	if($request->method ne "POST") {
		$error->("method is not POST");
		return;
	}
	if($request->uri ne "/login") {
		$error->("URI is not /login");
		return;
	}
	if($request->content_type ne 'application/x-www-form-urlencoded') {
		$error->("content-type is not correct");
		return;
	}

	my %fields = URI->new('?'.$request->content)->query_form;
	my $user = $fields{'username'};
	my $pass = $fields{'password'};

	if($pass ne 'abc$@%&?def') {
		$error->("password is incorrect");
		return;
	}

	if($user !~ /logintest2?\@limesco.nl/) {
		$error->("username is incorrect");
		return;
	}

	if($user eq 'logintest@limesco.nl') {
		$response->out_response("302 Moved temporarily");
		$response->out_headers(
			'Set-Cookie' => 'PHPSESSID=foobarbaz',
			'Content-Type' => 'text/plain',
		);
		$response->out_body("");
	} else {
		$response->out_response("200 OK");
		$response->out_headers(
			'Content-Type' => 'text/plain',
		);
		$response->out_body("");
	}
	return;
}

my $exception;
my $token;
try {
	$token = get_speakup_login_token($lim, $httpd->uri, 'logintest@limesco.nl', 'abc$@%&?def');
} catch {
	$exception = $_ || 1;
};

diag($exception) if $exception;
ok(!$exception, "No exceptions thrown while logging in");
is($token, "foobarbaz", "Login token is correct");

undef $exception;
my $token2;
try {
	$token2 = get_speakup_login_token($lim, $httpd->uri, 'logintest2@limesco.nl', 'abc$@%&?def');
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown when logging in with wrong information");
ok(!defined($token2), "No token defined when failing to log in");

## Retrieving CDRs by day ##
sub Test::HTTP::Server::Request::partner {
	my $response = $_[0];
	my $request_data = $response->{'head'} . $response->{'body'};
	my $request = HTTP::Request->parse($request_data);

	my $error = sub {
		$response->out_response("500 $_[0]");
		$response->out_headers(
			'Content-Type' => 'text/plain',
		);
		$response->out_body($_[0]);
	};

	if($request->method ne "POST") {
		$error->("method is not POST");
		return;
	}

	my %fields = URI->new('?'.$request->content)->query_form;
	my $startdate = $fields{'startdate'};
	if($startdate !~ m#^(\d\d)/03/2013$#) {
		$error->("startdate is incorrect: $startdate");
		return;
	}
	# TODO: check all fields here

	$startdate = $1;

	$response->{out_headers} = {
		'Content-Type' => 'text/csv',
	};
	my $body = "cid,time,from,to,account,duration,ppc,ppm,costs,destination,direction,sip\n";
	if($startdate eq "02") {
		$body .= <<'CSV';
abc-def@cid1,2013-03-02 01:00:00,123456,Destination,testaccount,6,0.123,0.412,0.8765,Netherlands - Other - Test Destination,out,200: OK
CSV
	} elsif($startdate eq "03") {
		$body .= <<'CSV';
abc-def@cid1,2013-03-03 01:00:00,123456,Destination,testaccount,6,0.123,0.412,0.8765,Netherlands - Other - Test Destination,out,200: OK
CSV
	} elsif($startdate eq "04") {
		$body .= <<'CSV';
abc-def@sms,2013-03-04 01:02:03,123456,316********,testaccount,6,0.123,0.412,0.8765,,out,200: OK
abc-def@data,2013-03-04 01:02:03,123456,APN,testaccount,6,0.123,0.412,0.8765,Netherlands - Other - Test Destination,out,404: Not Found
CSV
	} elsif($startdate eq "05") {
		# no CDRs
	} elsif($startdate eq "06") {
		$error->("this date is supposed to fail");
	} else {
		$error->("startdate has a wrong day value");
		return;
	}
	return $body;
}

undef $exception;
my @cdrs;
try {
	retrieve_speakup_cdrs($lim, $httpd->uri, $token, '2013-03-04', sub {
		push @cdrs, $_[0];
	});
} catch {
	$exception = $_ || 1;
};

diag($exception) if $exception;
ok(!$exception, "No exception thrown while retrieving CDRs");
is(scalar @cdrs, 2, "Two CDRs returned");
is_deeply($cdrs[0], {
	cid => 'abc-def@sms',
	time => '2013-03-04 01:02:03',
	from => '123456',
	to => '316********',
	account => 'testaccount',
	duration => 6,
	ppc => 0.123,
	ppm => 0.412,
	costs => 0.8765,
	destination => "",
	direction => "out",
	sip => "200: OK"
}, "First CDR is ok");
is_deeply($cdrs[1], {
	cid => 'abc-def@data',
	time => '2013-03-04 01:02:03',
	from => '123456',
	to => 'APN',
	account => 'testaccount',
	duration => 6,
	ppc => 0.123,
	ppm => 0.412,
	costs => 0.8765,
	destination => "Netherlands - Other - Test Destination",
	direction => "out",
	sip => "404: Not Found"
}, "Second CDR is ok");

undef $exception;
@cdrs = ();
try {
	retrieve_speakup_cdrs($lim, $httpd->uri, $token, '2013-03-05', sub {
		push @cdrs, $_[0];
	});
} catch {
	$exception = $_ || 1;
};

ok(!$exception, "No exception thrown while retrieving CDRs on empty day");
is(scalar @cdrs, 0, "No CDRs returned");

undef $exception;
@cdrs = ();
try {
	retrieve_speakup_cdrs($lim, $httpd->uri, $token, '2013-03-06', sub {
		push @cdrs, $_[0];
	});
} catch {
	$exception = $_ || 1;
};

ok($exception, "Exception thrown while retrieving CDRs on error day");
is(scalar @cdrs, 0, "No CDRs returned");

## Importing CDRs by day ##
undef $exception;
try {
	import_speakup_cdrs_by_day($lim, $httpd->uri, $token, '2013-03-04');
} catch {
	$exception = $_ || 1;
};

diag($exception) if($exception);
ok(!$exception, "No exception thrown while importing CDRs");
my $cdr_sth = $dbh->prepare("SELECT * FROM cdr ORDER BY time");
$cdr_sth->execute();
@cdrs = ();
while(my $cdr = $cdr_sth->fetchrow_hashref()) {
	push @cdrs, $cdr;
}
is(scalar @cdrs, 2, "Two CDRs inserted");
delete $cdrs[0]{'id'};
delete $cdrs[1]{'id'};
is_deeply($cdrs[0], {
	service => "SMS",
	call_id => 'abc-def@sms',
	from => '123456',
	to => '316********',
	speakup_account => 'testaccount',
	time => '2013-03-04 01:02:03',
	pricing_id => undef,
	pricing_info => undef,
	computed_cost => undef,
	computed_price => undef,
	invoice_id => undef,
	units => 6,
	connected => undef,
	source => undef,
	destination => undef,
	direction => "OUT",
	leg => undef,
	reason => undef,
}, "First CDR is ok");
is_deeply($cdrs[1], {
	service => "DATA",
	call_id => 'abc-def@data',
	from => '123456',
	to => 'APN',
	speakup_account => 'testaccount',
	time => '2013-03-04 01:02:03',
	pricing_id => undef,
	pricing_info => undef,
	computed_cost => undef,
	computed_price => undef,
	invoice_id => undef,
	units => 6,
	connected => undef,
	source => undef,
	destination => "Netherlands - Other - Test Destination",,
	direction => "OUT",
	leg => undef,
	reason => undef,
}, "Second CDR is ok");

my $cdrg_sth = $dbh->prepare("SELECT * FROM cdrimports WHERE cdr_date=?");
$cdrg_sth->execute('2013-03-04');
my $cdrg = $cdrg_sth->fetchrow_hashref;
diag($cdrg->{'error'}) if($cdrg->{'error'});
is($cdrg->{'error'}, undef, "No error while importing");
my $old_0304_import_time = $cdrg->{'import_time'};
ok(around_now($cdrg->{'import_time'}), "CDR importing date changed to recently");

## Importing all CDRs ##
sleep 2; # make sure some time passed since last test, so the importing time is different
import_speakup_cdrs($lim, $httpd->uri, $token, '2013-03-06');
# Imports 2013-03-02, 2013-03-03, 2013-03-05, 2013-03-06
# 2013-03-02 has one CDR
# 2013-03-03 has one CDR whose callId conflicts with ^
# 2013-03-05 has no CDRs
# 2013-03-06 gives an error

# Check if the cdrimports table was correctly updated
$cdrg_sth->execute('2013-03-02');
$cdrg = $cdrg_sth->fetchrow_hashref;
is($cdrg->{'error'}, undef, "No error while importing 03-02");
ok(around_now($cdrg->{'import_time'}), "CDR importing date 03-02 changed to recently");
$cdrg_sth->execute('2013-03-03');
$cdrg = $cdrg_sth->fetchrow_hashref;
is($cdrg->{'error'}, undef, "No error while importing 03-03");
ok(around_now($cdrg->{'import_time'}), "CDR importing date 03-03 changed to recently");
$cdrg_sth->execute('2013-03-04');
$cdrg = $cdrg_sth->fetchrow_hashref;
is($cdrg->{'error'}, undef, "No error while importing 03-04");
is($old_0304_import_time, $cdrg->{'import_time'}, "03-04 import time hasn't changed");
$cdrg_sth->execute('2013-03-05');
$cdrg = $cdrg_sth->fetchrow_hashref;
is($cdrg->{'error'}, undef, "No error while importing 03-05");
ok(around_now($cdrg->{'import_time'}), "CDR importing date 03-05 changed to recently");
$cdrg_sth->execute('2013-03-06');
$cdrg = $cdrg_sth->fetchrow_hashref;
ok($cdrg->{'error'}, "Error thrown while importing 03-06");
ok(around_now($cdrg->{'import_time'}), "CDR importing date 03-06 changed to recently");

$cdrg_sth = undef;
$dbh->disconnect();

sub around_now {
	my $date = $_[0];
	# unset date: not around now
	return 0 if(!$date);

	my ($year, $mon, $day, $hour, $min, $sec) = $date =~ /^(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\.\d+$/;
	die "Failed to parse date: $date" if(!$sec);
	my $datetime = DateTime->new(
		year => $year,
		month => $mon,
		day => $day,
		hour => $hour,
		minute => $min,
		second => $sec,
	);
	my $diff = DateTime->now() - $datetime;
	return $diff->delta_minutes() < 5;
}

