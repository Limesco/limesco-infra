#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Data::Dumper;
use IPC::Run qw(run);

=head1 gnucash-export.pl

Usage: gnucash-export.pl [infra-options]
                         [--format <format>]
                         [--write-to <filename>]
                         [--type <invoice|direct-debit>]
                         [--date <+date|date>]

This tool can be used to export GnuCash-compatible QIF files for invoices or
direct-debit data.

--format   <format>                QIF or plain, defaults to plain (table-like)
--write-to <filename>              Write output to <filename>, defaults to stdout
--type     <invoice|direct-debit>  Invoices or direct-debit, defaults to invoice
--date     <+date|date>            Show <date> or prepend with + for <date> and
                                   further, defaults to current month. Expects
                                   the following format: YYYY-MM

=cut

if(!caller) {
	my $format = "plain";
	my $filename = "";
	my $type = "invoice";
	my $date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--format") {
			$format = $args->[++$$iref];
		} elsif($arg eq "--write-to") {
			$filename = $args->[++$$iref];
		} elsif($arg eq "--type") {
			$type = $args->[++$$iref];
		} elsif($arg eq "--date") {
			$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if (!$date) {
		$date = sprintf "%d-%02d-01", map { $$_[5]+1900, $$_[4]+1 } [localtime];
	}
	if ($date !~ /^[+]?\d\d\d\d-\d\d(-\d\d)?$/) {
		print "Date format not recognized. Expected YYYY-MM or YYYY-MM-DD, got $date\n";
	}
	if ($date !~ /-\d\d-\d\d$/) {
		# Append first day of the month
		$date .= "-01";
	}
	my @invoices = get_all_invoices($lim, $date);
	print_invoices(@invoices, $format);
}

=head2 Methods

=head3 get_all_invoices($lim, $date)

Retrieve all invoices of $date (or further.

=cut

sub get_all_invoices {
	my ($lim, $date) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth;
	my $query;
	my $params;
	my $numresults;
	my $invoices = [];
	
	if ($date =~ '^\+') {
		$date = substr $date, 1;
		$query = "SELECT * FROM invoice WHERE date >= ?";
		$params = 1;
	} else {
		$query = q(SELECT * FROM invoice WHERE date BETWEEN ? AND (?::date + '1 month'::interval));
		$params = 2;
	}
	print "$query\n";

	$sth = $dbh->prepare($query);
	$numresults = ($params == 1) ? $sth->execute($date) : $sth->execute($date, $date);

	if ($numresults > 0) {
		while (my $row = $sth->fetchrow_hashref()) {
			my $account = get_account($lim, $row->{account_id});
			$row->{full_name} = $account->{last_name}.", ".$account->{first_name};
			$row->{full_name} .= " (".$account->{company_name}.")" if $account->{company_name};
			push @{$invoices}, $row;
		}
	} else {
		print "Empty result set.\n";
	}
	return $invoices;
}

=head3 get_account($lim, $account_id)

Retrieve current information about account $account_id.

=cut

sub get_account {
	my ($lim, $account_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM account WHERE id=? AND period && '[today,today]';");
	$sth->execute($account_id);
	return $sth->fetchrow_hashref() or die "Account doesn't exist: '$account_id'\n";
}

=head3 print_table(@invoices, $format)

Prints all the requested invoices in the specified format (plain, QIF)

=cut

sub print_invoices {
	my ($invoices, $format) = @_;
	print "!Type:Oth A\n" if ($format eq "qif");

	for ( 0 .. keys $invoices ) {
		my $invoice = $invoices->[$_];
		last if (!$invoice);
		my $invoice_vat = $invoice->{rounded_without_taxes} * 0.21 if ($invoice->{rounded_with_taxes});
		my $invoice_inc = $invoice->{rounded_without_taxes} + $invoice_vat if ($invoice->{rounded_with_taxes});
		my $invoice_diff = $invoice->{rounded_with_taxes} - $invoice_inc if ($invoice->{rounded_with_taxes});

		printf("%s\t%s\t%6.2f\t%6.2f\t%6.2f\t%6.2f\t%6.2f\n",
			$invoice->{id},
			$invoice->{date},
			$invoice->{rounded_without_taxes},
			$invoice_vat,
			$invoice_inc,
			$invoice->{rounded_with_taxes},
			$invoice_diff) if ($invoice->{rounded_with_taxes} and $format eq "plain");

		if ($format eq "qif") {
			printf("!Account\nN%s\nTOth A\n^\n", $invoice->{full_name});
			print "!Type:Oth A\n";

			if (abs($invoice_diff) > 0) {
				printf("D%s\nT%s\nMFactuur %s\nSKlanten\n\$%s\nSBTW-hoog\n\$%.2f\nSImbalance\n\$%.2f\n^\n",
					$invoice->{date},
					$invoice->{rounded_with_taxes},
					$invoice->{id},
					$invoice->{rounded_without_taxes},
					$invoice_vat,
					$invoice_diff);
			} else {
				printf("D%s\nT%s\nMFactuur %s\nSKlanten\n\$%s\nSBTW-hoog\n\$%.2f\n^\n",
					$invoice->{date},
					$invoice->{rounded_with_taxes},
					$invoice->{id},
					$invoice->{rounded_without_taxes},
					$invoice_vat);
			}
		}
	}
}
