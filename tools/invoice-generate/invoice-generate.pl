#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;

=head1 invoice-generate.pl

Usage: invoice-generate.pl [infra-options] --account <accountID> --date <date>

This tool is used to generate an invoice for an account with a given date.

=cut

if(!caller) {
	my $account_id;
	my $date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--account") {
			$account_id = $args->[++$$iref];
		} elsif($arg eq "--date") {
			$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if(!$account_id) {
		die "--account option is required\n";
	}
	if(!$date) {
		die "--date option is required\n";
	}

	my $invoice = generate_invoice($lim, $account_id, $date);

	print "Invoice generated for account $account_id: " . $invoice->{'id'} . "\n";
}

=head2 Methods

=head3 generate_invoice($lim, $account_id, $date)

Create an invoice in the database for account $account_id, with invoice date $date.

=cut

sub generate_invoice {
	my ($lim, $account_id, $date) = @_;
	my $dbh = $lim->get_database_handle();

	# Retrieve CDR info of last month
	# Retrieve monthly costs of this month
	# Generate item lines
	# Generate invoice itself
	# Insert it all into the database in a transaction

	my $sth = $dbh->prepare("SELECT * FROM account WHERE id=? AND period && ?");
	$sth->execute($account_id, $date);
	my $account = $sth->fetchrow_hashref() or die "Account doesn't exist: '$account_id'\n";
}

1;
