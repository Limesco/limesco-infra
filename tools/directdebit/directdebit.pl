#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Digest::MD5 qw(md5);
use Sys::Hostname;

=head1 directdebit.pl

Usage: directdebit.pl [infra-options]

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV);
	print generate_directdebit_authorization($lim) . "\n";
}

=head2 Methods

=head3 generate_directdebit_authorization($lim)

=cut

sub generate_directdebit_authorization {
	# This code was heavily inspired by BSON ObjectID generation from bson-ruby
	use bytes;
	my $time = time();
	my $machine_id = (unpack("N", md5(hostname)))[0];
	my $process_id = $$;
	my $random = int(rand(1 << 32));
	my $binary = pack("N NX lXX NX", $time, $machine_id, $process_id, $random);
	my $str = "";
	for(0..length($binary)) {
		$str .= sprintf("%02x", ord(substr($binary, $_, 1)));
	}
	return $str;
}

=head3 add_directdebit_account($lim, $period, $authorization, $iban, $bic, $date)

=cut

sub add_directdebit_account {
	my ($lim, $period, $authorization, $iban, $bic, $date) = @_;
	return;
}

=head3 select_directdebit_invoices($lim, $authorization)

=cut

sub select_directdebit_invoices {
	my ($lim, $authorization) = @_;
	return;
}

=head3 create_directdebit_transaction($lim, $authorization, $invoice)

=cut

sub create_directdebit_transaction {
	my ($lim, $authorization, $invoice) = @_;
	return;
}

=head3 get_directdebit_transaction($lim, $transaction_id)

=cut

sub get_directdebit_transaction {
	my ($lim, $transaction_id) = @_;
	return;
}

=head3 create_directdebit_file($lim, $filetype)

FRST / RCUR

=cut

sub create_directdebit_file {
	my ($lim, $filetype) = @_;
	return;
}

=head3 get_directdebit_file($lim, $id)

=cut

sub get_directdebit_file {
	my ($lim, $id) = @_;
	return;
}

1;
