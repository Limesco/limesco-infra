#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

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
