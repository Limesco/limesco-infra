#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use LWP::UserAgent;
use HTTP::Cookies;

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
	return ();
}

=head3 retrieve_speakup_cdrs($lim, $uri_base, $date, $callback)

=cut

sub retrieve_speakup_cdrs {
	my ($lim, $uri_base, $date, $callback) = @_;
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
