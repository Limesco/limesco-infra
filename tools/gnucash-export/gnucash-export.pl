#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Data::Dumper;
#use IPC::Run qw(run);

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
	if ($type eq "invoice") {
		my @invoices = get_all_invoices($lim, $date);
		print_invoices(@invoices, $format);
	} else {
		my @dd = get_all_directdebit($lim, $date);
		print_directdebit(@dd, $format);
	}
}

=head2 Methods

=head3 Generic methods

=head4 get_account($lim, $account_id)

Retrieve current information about account $account_id.

=cut

sub get_account {
	my ($lim, $account_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM account WHERE id=? AND period && '[today,today]';");
	$sth->execute($account_id);
	return $sth->fetchrow_hashref() or die "Account doesn't exist: '$account_id'\n";
}

=head3 Invoice related methods

=head4 get_all_invoices($lim, $date)

Retrieve all invoices of the specified date (interval).

=cut

sub get_all_invoices {
	my ($lim, $date) = @_;
	my $dbh = $lim->get_database_handle();
	my $query;
	my $params;

	if ($date =~ '^\+') {
		$date = substr $date, 1;
		$query = "SELECT * FROM invoice WHERE date >= ?";
		$params = 1;
	} else {
		$query = q(SELECT * FROM invoice WHERE date BETWEEN ? AND (?::date + '1 month'::interval));
		$params = 2;
	}

	my $sth = $dbh->prepare($query);
	my $numresults = ($params == 1) ? $sth->execute($date) : $sth->execute($date, $date);
	my $invoices = [];

	if ($numresults > 0) {
		while (my $row = $sth->fetchrow_hashref()) {
			my $account = get_account($lim, $row->{account_id});
			$row->{full_name} = $account->{last_name}.", ".$account->{first_name};
			$row->{full_name} .= " (".$account->{company_name}.")" if $account->{company_name};
			push @{$invoices}, $row;
		}
	}
	return $invoices if ($numresults > 0);
	return "Empty result set.";
}

=head4 print_invoices(@invoices, $format)

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

			if (sprintf("%.2f", $invoice_diff) !~ /-?0.00/) {
				warn "invoice diff: $invoice_diff vs ".sprintf("%.2f", $invoice_diff)."\n";
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

=head3 Direct debit related methods

=head4 get_all_directdebit($lim, $date)

Retrieve all direct debit information of the specified $date (interval).

=cut

sub get_all_directdebit {
	my ($lim, $date) = @_;
	my $dbh = $lim->get_database_handle();
	my $query;
	my $params;
	my $where_clause;

	if ($date =~ '^\+') {
		$date = substr $date, 1;
		$where_clause = ">= ?";
		$params = 1;
	} else {
		$where_clause = q(BETWEEN ? AND (?::date + '1 month'::interval));
		$params = 2;
	}
	$query = "SELECT df.id, df.processing_date, dt.invoice_id, invoice.rounded_without_taxes,
			invoice.rounded_with_taxes, account.first_name, account.last_name,
			account.company_name FROM directdebit_file df
			LEFT OUTER JOIN directdebit_transaction dt ON (df.id = dt.directdebit_file_id)
			LEFT OUTER JOIN invoice ON (invoice.id = dt.invoice_id)
			LEFT OUTER JOIN account ON (account.id = invoice.account_id)
			WHERE df.creation_time ".$where_clause." AND account.period @> invoice.date";
	# `account.period @> invoice.date` is necessary to prevent duplicate entries for an
	# account with multiple periods

	my $sth = $dbh->prepare($query);
	my $numresults = ($params == 1) ? $sth->execute($date) : $sth->execute($date, $date);
	my $directdebit = [];

	my $activation_query = "SELECT invoice_id, item_price, description FROM invoice_itemline WHERE description = 'Activatie SIM-kaart' AND invoice_id = ?";
	my $act_sth = $dbh->prepare($activation_query);

	if ($numresults > 0) {
		while (my $row = $sth->fetchrow_hashref()) {
		# XXX: hack-alert. Check for activation item lines and add them to the result set.
			if ($act_sth->execute($row->{invoice_id}) > 0) {
				$row->{activation_costs} = [];
				while (my $act_row = $act_sth->fetchrow_hashref()) {
					push $row->{activation_costs}, $act_row;
				}
			}
			$row->{full_name} = $row->{last_name}.", ".$row->{first_name};
			$row->{full_name} .= " (".$row->{company_name}.")" if ($row->{company_name});
			push @{$directdebit}, $row;
		}
	}
	return $directdebit if ($numresults > 0);
	return "Empty result set.";
}

=head4 print_directdebit(@invoices, $format)

Prints all the requested direct debit data in the specified format (plain, QIF)

=cut

sub print_directdebit {
	my ($invoices, $format) = @_;
	print "!Type:Oth A\n" if ($format eq "qif");

	for ( 0 .. keys $invoices ) {
		my $invoice = $invoices->[$_];
		last if (!$invoice);

		my $num_actcosts = 0;
		if ($invoice->{activation_costs}) {
			for ( 0 .. keys $invoice->{activation_costs} ) {
				my $act_data = $invoice->{activation_costs}[$_];
				if ($act_data) {
					if ($act_data->{item_price} != 34.71070000) {
						die "print_directdebit(): invalid item_price for activation costs. Check data for invoice_id $invoice->{invoice_id}.";
					}
					$num_actcosts++;
				}
			}
		}

		# Correct invoice amounts for activation costs
		$invoice->{rounded_with_taxes} -= $num_actcosts*42 if ($num_actcosts > 0);
		$invoice->{rounded_without_taxes} -= $num_actcosts*34.7107 if ($num_actcosts > 0);

		printf("%s\t%s\t%6.2f\t%6.2f\t%d\t%s\n",
			$invoice->{invoice_id},
			$invoice->{processing_date},
			$invoice->{rounded_without_taxes},
			$invoice->{rounded_with_taxes},
			$num_actcosts,
			$invoice->{full_name}) if ($invoice->{rounded_with_taxes} and $format eq "plain");

		if ($format eq "qif") {
			if ($invoice->{rounded_with_taxes}) {
				printf("!Account\nN%s\nTOth A\n^\n", $invoice->{full_name});
				print "!Type:Oth A\n";

				printf("D%s\nT-%.2f\nMIncasso voor factuur %s\nSAutomatisch incasso\n\$-%.2f\n^\n",
					$invoice->{processing_date},
					$invoice->{rounded_with_taxes},
					$invoice->{invoice_id},
					$invoice->{rounded_with_taxes});
			}
		}
	}
}

1;
