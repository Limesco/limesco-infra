#!/usr/bin/perl
use strict;
use warnings;
use Business::IBAN;
use Try::Tiny;

use lib '../../lib';
use lib '../lib';
use lib 'lib';

require 'nl-bic-list.pl';
our $bicmapping;

=head1 bic-convert.pl

Usage: bic-convert.pl <IBAN>

This tool converts an IBAN to a BIC, using a predefined list of BICs, sourced
from http://www.betaalvereniging.nl/europees-betalen/sepa-documentatie/bic-afleiden-uit-iban/

=cut

if (!caller) {
	my $iban = shift;

	if (!$iban) {
		die "No IBAN given";
	}
	
	print "IBAN: $iban\n";
	print "BIC: ". iban_to_bic($iban) ."\n";
}

=head2 Methods

=head3 iban_to_bic($iban)

Retrieves BIC from an IBAN or dies when an incorrect IBAN is given.

=cut

sub iban_to_bic {
	my $iban = shift;

	if (!iban_check($iban)) {
		die;
	}
	my $bank = substr $iban, 4, 4;
	return $bicmapping->{$bank};
}

=head3 iban_check($iban)

Wrapper for Business::IBAN->valid($iban)

=cut

sub iban_check {
	my $iban = shift;
	my $valid = 0;

	my $iban_module = Business::IBAN->new();
	if (!$iban_module->valid($iban)) {
		warn "IBAN is invalid: $iban.\n";
	} else {
		$valid = 1;
	}

	return $valid;
}

1;
