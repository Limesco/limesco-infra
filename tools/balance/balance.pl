#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;

Limesco::Balance->import(qw(get_payments_and_invoices sprintf_money));

=head1 balance.pl

Usage: balance.pl [infra-options] [--account account-like] [--date date]

Display payments, invoices and balance for a given account, or all accounts if
none was given. If a date is given, payments and invoices until that date are
shown, producing a balance on that date.

=cut

if(!caller) {
	my $account;
	my $date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--account") {
			$account = $args->[++$$iref];
		} elsif($arg eq "--date") {
			$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if($account) {
		if($account =~ /^\d+$/) {
			$account = $lim->get_account($account);
		} else {
			$account = $lim->get_account_like($account);
		}
		my @p_and_i = get_payments_and_invoices($lim, $account->{'id'}, $date);
		foreach(@p_and_i) {
			my $descr;
			my $amount;
			if($_->{'objecttype'} eq "PAYMENT") {
				$amount = $_->{'amount'};
				$descr = lc($_->{'type'}) . " " . $_->{'origin'};
			} else {
				$amount = -$_->{'rounded_with_taxes'};
				$descr = $_->{'id'};
			}
			printf("%s %s %40s  %s -> %s\n", lc($_->{'objecttype'}), $_->{'date'}, $descr,
				sprintf_money($amount), sprintf_money($_->{'balance'}));
		}
	} else {
		my $dbh = $lim->get_database_handle();
		my $sth = $dbh->prepare("SELECT DISTINCT(id) FROM account ORDER BY id ASC");
		$sth->execute();
		my @accounts;
		while(my $account_id = $sth->fetchrow_hashref) {
			$account_id = $account_id->{'id'};
			my $asth = $dbh->prepare("SELECT id, first_name, last_name, company_name FROM account WHERE id=? AND
				period=(SELECT period FROM account WHERE id=? ORDER BY lower(period) DESC LIMIT 1)");
			$asth->execute($account_id, $account_id);
			push @accounts, $asth->fetchrow_hashref;
		}
		for my $account (@accounts) {
			my @p_and_i = get_payments_and_invoices($lim, $account->{'id'}, $date);
			my $balance = @p_and_i ? (pop @p_and_i)->{'balance'} : 0;
			my $name = sprintf("%s %s", $account->{'first_name'}, $account->{'last_name'});
			if($account->{'company_name'}) {
				$name = $account->{'company_name'} . " ($name)";
			}
			printf("%3d %45s -> %s\n", $account->{'id'}, $name, sprintf_money($balance));
		}
	}
}

package Limesco::Balance;
use strict;
use warnings;
no warnings 'redefine';
use Exporter::Easy (
	OK => [qw(sprintf_money list_payments get_payment get_payments_and_invoices)],
);
use JSON;
use Try::Tiny;

BEGIN {
	do '../invoice-export/invoice-export.pl' or die $! unless $INC{"../invoice-export/invoice-export.pl"};
	Limesco::InvoiceExport->import("list_invoices");
}

=head2 Methods

=head3 sprintf_money($amount)

=cut

sub sprintf_money {
	my ($amount) = @_;
	return sprintf("%7s", sprintf("%.2f", $amount));
}

=head3 list_payments($lim, [$account_id])

Return a list of all payments for a given account ID. If no account ID is
given, return a list of all payments.

=cut

sub list_payments {
	my ($lim, $account_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $where_clause = $account_id ? "WHERE account_id=?" : "";
	my $sth = $dbh->prepare("SELECT * FROM payment $where_clause ORDER BY id ASC");
	$sth->execute($account_id ? ($account_id) : ());
	my @payments;
	while(my $payment = $sth->fetchrow_hashref) {
		push @payments, $payment;
	}
	return @payments;
}

=head3 get_payment($lim, $payment_id)

Return information on a specific payment.

=cut

sub get_payment {
	my ($lim, $payment_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM payment WHERE id=?");
	$sth->execute($payment_id);
	my $payment = $sth->fetchrow_hashref() or die "Payment doesn't exist: '$payment_id'\n";
	return $payment;
}

=head3 get_payments_and_invoices($lim, $account_id, [$date])

Get a list of invoices and payments for an account ID, sorted by date. For each
object, a field 'objecttype' is added whose value is PAYMENT or INVOICE, and a field
'balance' is added which displays the cumulative balance after this object. If the
optional date variable is given (YYYY-MM-DD), only objects until the given date
are returned.

=cut

sub get_payments_and_invoices {
	my ($lim, $account_id, $date) = @_;
	my @payments = list_payments($lim, $account_id);
	foreach(@payments) {
		$_->{'objecttype'} = "PAYMENT";
	}
	my @invoices = list_invoices($lim, $account_id);
	foreach(@invoices) {
		$_->{'objecttype'} = "INVOICE";
	}
	my @payments_and_invoices = sort { $a->{'date'} cmp $b->{'date'} }
		grep { !$date || $date ge $_->{'date'} } (@payments, @invoices);
	my $balance = 0;
	foreach(@payments_and_invoices) {
		$balance += $_->{'objecttype'} eq "PAYMENT" ? $_->{'amount'} : -$_->{'rounded_with_taxes'};
		$_->{'balance'} = $balance;
	}
	return @payments_and_invoices;
}

1;
