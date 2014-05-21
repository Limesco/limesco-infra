#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use DateTime;
use LWP::UserAgent;
use HTTP::Cookies;
use Text::CSV;

=head1 cdr-import.pl

Usage: cdr-import.pl [infra-options]

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--account") {
			#$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});
}

=head2 Methods

=head3 get_speakup_login_token($lim, $uri_base, $username, $password)

=cut

sub get_speakup_login_token {
	my ($lim, $uri_base, $username, $password) = @_;
	$uri_base =~ s#/$##;

	my $cookie_jar = HTTP::Cookies->new;

	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->agent("Liminfra/0.0 ");
	$ua->cookie_jar($cookie_jar);
	my $response = $ua->post($uri_base . '/login', {
		username => $username,
		password => $password,
	});
	# Failed login gives response code 200
	if($response->code != 302) {
		die "SpeakUp login token request failed: " . $response->status_line;
	}
	my $login_cookie;
	$cookie_jar->scan(sub {
		my (undef, $key, $val) = @_;
		if($key eq "PHPSESSID") {
			$login_cookie = $val;
		} else {
			warn "$key=$val\n";
		}
	});
	if(!$login_cookie) {
		die "SpeakUp login token request failed: no session cookie found\n";
	}
	return $login_cookie;
}

=head3 get_cdr_import_dates($lim, $date_today)

=cut

sub get_cdr_import_dates {
	my ($lim, $date_today) = @_;

	{
		my ($t_year, $t_month, $t_day) = $date_today =~ /^(\d{4})-(\d\d)-(\d\d)$/;
		if(!$t_day) {
			die "Don't understand date syntax of today: $date_today";
		}
		$date_today = DateTime->new(year => $t_year, month => $t_month, day => $t_day);
	}

	my $dbh = $lim->get_database_handle();

	# Re-import all CDR dates where an error occured or the day was last refreshed
	# less than 48 hours after the day began

	my @days_to_update;

	my $sth = $dbh->prepare("SELECT cdr_date FROM cdrimports
		WHERE error IS NOT NULL
		OR (import_time <= cdr_date + '24 hours'::interval)");
	$sth->execute();
	while(my $row = $sth->fetchrow_arrayref) {
		push @days_to_update, $row->[0];
	}

	# Add all dates which are between last mentioned date and given 'today' date
	$sth = $dbh->prepare("SELECT cdr_date + '1 day'::interval AS startdate
		FROM cdrimports ORDER BY cdr_date DESC LIMIT 1");
	$sth->execute();
	my $date = $sth->fetchrow_arrayref;
	if(!$date) {
		warn "No dates in the cdrimports table yet, will start fetching today.\n";
		warn "If you want to start on another date, add a row to the cdrimports table\n";
		warn "with 'error' set to anything non-NULL, and I will start fetching there.\n";
		return ($date_today);
	}

	my ($year, $month, $day) = $date->[0] =~ /^(\d{4})-(\d\d)-(\d\d) /;
	$date = DateTime->new(year => $year, month => $month, day => $day);
	until($date > $date_today) {
		push @days_to_update, $date->ymd;
		$date->add(days => 1);
	}

	return @days_to_update;
}

=head3 retrieve_speakup_cdrs($lim, $uri_base, $token, $date, $callback)

=cut

sub retrieve_speakup_cdrs {
	my ($lim, $uri_base, $token, $date, $callback) = @_;

	my $uri = URI->new($uri_base);
	$uri_base =~ s#/$##;
	$date =~ s#^(\d{4})-(\d\d)-(\d\d)$#$3/$2/$1#;

	my $cookie_jar = HTTP::Cookies->new;
	$cookie_jar->set_cookie(1, "PHPSESSID", $token, $uri->path, $uri->host, $uri->port);

	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->agent("Liminfra/0.0 ");
	$ua->cookie_jar($cookie_jar);
	my $response = $ua->post($uri_base . '/partner/cdr', {
		period => 'day',
		startdate => $date,
		direction => 'both',
		deliverystatus => 'both',
		displaysip => 'yes',
		displayppc => 'yes',
		ratetype => 'normal',
		outputData => 'detailed',
		outputFormat => 'csv',
		extensionId => '',
		subscriptionAccountId => '',
		customerId => '',
		extensionNumber => '',
		submit => '',
	});
	if($response->code != 200) {
		die "SpeakUp CDR request for $date failed: " . $response->status_line;
	}
	if($response->content_type ne "text/csv") {
		die "SpeakUp CDR request for $date failed: content-type is not text/csv";
	}
	my $csv = Text::CSV->new({binary => 1})
		or die "Cannot use CSV: ".Text::CSV->error_diag();
	
	my $i = 0;
	my @column_names;
	for(split /^/, ${$response->content_ref}) {
		++$i;
		if(!$csv->parse($_)) {
			die "Failed to parse line $i of response";
		}
		if(!@column_names) {
			@column_names = $csv->fields;
			next;
		}

		my @columns = $csv->fields;
		my $cdr = {};
		for (0 .. $#column_names) {
			$cdr->{$column_names[$_]} = $columns[$_];
		}

		try {
			$callback->($cdr);
		} catch {
			die "Callback failed during parsing of line $i of response: $_";
		};
	}
}

=head3 import_speakup_cdrs_by_day($lim, $uri_base, $token, $date)

=cut

sub import_speakup_cdrs_by_day {
	my ($lim, $uri_base, $token, $date) = @_;
}

=head3 import_speakup_cdrs($lim, $uri_base, $token, $date_today)

=cut

sub import_speakup_cdrs {
	my ($lim, $uri_base, $token, $date_today) = @_;
}

1;
