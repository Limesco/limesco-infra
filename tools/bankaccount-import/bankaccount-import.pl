#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::MT940;
use Try::Tiny;
use DateTime;
use Term::Menu;
use Data::Dumper;

=head1 bankaccount-import.pl

Usage:
  bankaccount-import.pl [infra-options] --mt940 filename
  bankaccount-import.pl [infra-options] --create-payment

If --mt940 is given, import bank account transactions from an mt940 file. All
transactions not corresponding to the configured bank account are ignored. the
start date and balance must correspond to the last end date and end balance
stored in the database.

During import, Payments will be created for all transactions containing an
invoice number. If a transaction is missed, it can be created interactively
using --create-payment.

=cut

if(!caller) {
	my $mt940;
	my $create;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--mt940") {
			$mt940 = $args->[++$$iref];
		} elsif($arg eq "--create-payment") {
			$create = 1;
		} else {
			return 0;
		}
	});

	if($create) {
		create_payment_interactively($lim);
	} elsif($mt940) {
		import_mt940_file($lim, $mt940);
	} else {
		die "Use --help to get usage information.\n";
	}
}

=head2 Methods

=cut

sub parse_mt940_balance {
	my ($year, $month, $day, $balance) = $_[0] =~ /^C(\d\d)(\d\d)(\d\d)EUR([\d]+,\d\d)$/;
	if(!$day) {
		die "Did not understand balance value: " . $_[0] . "\n";
	}
	$balance =~ s/,/./;
	$balance += 0; # convert to number
	my $date = DateTime->new(year => "20$year", month => $month, day => $day);
	return ($date, $balance);
}

=head3 import_mt940_file($lim, $filename)

Import an MT940 file to the Payments table. The start date and balance for the
first statement in the file must correspond to the end date and balance for the
last statement in the bankaccountimports table, if any. The bankaccountimports
table will be updated with the loaded statements.

Payments will be created for any transactions containing an invoice ID in the
description.

=cut

sub import_mt940_file {
	my ($lim, $filename) = @_;
	my $dbh = $lim->get_database_handle;
	$dbh->begin_work;

	try {
		$dbh->do("LOCK TABLE bankaccountimports");

		my ($current_date, $current_balance);
		{
			my $sth = $dbh->prepare("SELECT enddate, endbalance FROM bankaccountimports ORDER BY enddate DESC LIMIT 1");
			$sth->execute;
			my $row = $sth->fetchrow_hashref;
			if($row && $row->{'enddate'} =~ /^(\d{4})-(\d\d)-(\d\d)$/) {
				$current_date = DateTime->new(
					year => $1,
					month => $2,
					day => $3,
				);
				$current_balance = $row->{'endbalance'};
			}
		}

		my ($first_date, $first_balance);

		# An MT940 file consists of statements for a specific day and
		# account. In the weekends, no statements are written. Each
		# statement begins with begin and end balances, including "date
		# of balance". Here, we check that every begin balance
		# corresponds with the last end balance, including the dates.

		my @dates = Limesco::MT940->parse_from_file($filename);
		my @accounts = ("NL24RABO0169207587 EUR");
		my $transactions = 0;
		my $payments = 0;
		foreach(@dates) {
			my $account = $_->{'account'};
			if(!grep { $_ eq $account || $_ eq "$account EUR" } @accounts) {
				# Skip transactions to unknown accounts
				next;
			}

			my ($start_date, $start_balance) = parse_mt940_balance($_->{'start_balance'});
			my ($end_date, $end_balance) = parse_mt940_balance($_->{'end_balance'});

			if(defined($current_date) && $current_date->ymd ne $start_date->ymd) {
				die "Expected transactions entry with begin date " . $current_date->ymd . ", but got one with " . $start_date->ymd . "\n";
			}
			if(defined($current_balance) && $current_balance != $start_balance) {
				die "Expected transactions entry on begin date " . $current_date->ymd . " to have start balance $current_balance, but it has $start_balance\n";
			}

			foreach(@{$_->{'transactions'}}) {
				$transactions++;
				my $description = $_->{'description'};
				my $transaction = $_->{'transaction'};
				my ($year, $month, $day, $direction, $amount, $peer) =
					$transaction =~ /^(\d\d)(\d\d)(\d\d)([DC])0(\d+,\d\d)N\d{3}\w+\s*(\w\w\d\d.+)?$/;
				if(!$direction) {
					warn "Could not parse transaction description: $transaction\n";
				}
				$amount =~ s/,/./;
				$amount += 0;
				if($direction eq 'D') {
					$amount = -$amount;
				}
				my @invoices = $description =~ /\b(\d\dC\d{6})\b/;
				if(!@invoices) {
					next;
				}
				my $account_id;
				foreach(@invoices) {
					my $account_from_invoice = $dbh->prepare("SELECT account_id FROM invoice WHERE id=?");
					$account_from_invoice->execute($_);
					my $this_account_id = $account_from_invoice->fetchrow_hashref();
					if(!$this_account_id) {
						warn "Invoice ID $_ referenced but not found, ignoring\n";
						next;
					}
					$this_account_id = $this_account_id->{'account_id'};
					if(defined($account_id) && $this_account_id != $account_id) {
						warn "Transaction pays for multiple accounts; that's unsupported, ignoring it\n";
						undef $account_id;
						last;
					}
					$account_id = $this_account_id;
				}
				if($account_id) {
					$payments++;
					create_payment($dbh, {
						account_id => $account_id,
						type => 'BANK_TRANSFER',
						amount => $amount,
						date => new DateTime(year => "20$year", month => $month, day => $day)->ymd,
						origin => $peer,
						description => $description,
					});
				}
			}

			if(!defined($first_date)) {
				$first_date = $start_date;
				$first_balance = $start_balance;
			}
			$current_date = $end_date;
			$current_balance = $end_balance;
		}

		if(!$first_date) {
			die "No correct date records found. Maybe the correct account isn't added to the configuration file?\n";
		}

		my $sth = $dbh->prepare("INSERT INTO bankaccountimports (startdate, startbalance, enddate, endbalance, transactions, payments) VALUES (?, ?, ?, ?, ?, ?)");
		$sth->execute($first_date->ymd, $first_balance, $current_date->ymd, $current_balance, $transactions, $payments);

		$dbh->commit;
	} catch {
		my $exception = $_ || "Unknown error";
		$dbh->rollback;
		die $exception;
	};
}

=head3 create_payment($lim / $dbh, $payment)

Insert the given payment into the database.

=cut

sub create_payment {
	my ($lim, $payment) = @_;
	my $dbh = ref($lim) eq "Limesco" ? $lim->get_database_handle() : $lim;
	my $sth = $dbh->prepare("INSERT INTO payment (account_id, type, amount, date, origin, description) VALUES (?, ?, ?, ?, ?, ?)");
	$sth->execute(map { $payment->{$_} } qw/account_id type amount date origin description/);
}

=head3 create_payment_interactively($lim)

Ask a few questions to interactively create a payment.

=cut

sub create_payment_interactively {
	my ($lim) = @_;
	my $payment = {};
	$|++;
	my $menu = Term::Menu->new();
	until($payment->{'account_id'}) {
		my $accountlike = $menu->question("Add payment for which account? ");
		1 while chomp $accountlike;
		try {
			if($accountlike =~ /^\d+$/) {
				my $account = $lim->get_account($accountlike);
				$payment->{'account_id'} = $account->{'id'} if($account);
			} else {
				my $account = $lim->get_account_like($accountlike);
				$payment->{'account_id'} = $account->{'id'} if($account);
			}
		} catch {
			warn $_;
		};
	}

	until($payment->{'type'}) {
		$payment->{'type'} = $menu->menu(
			BANK_TRANSACTION => ["Bank transaction", 1],
			TARGETPAY => ["TargetPay (iDeal)", 2],
			DIRECTDEBIT => ["DirectDebit", 3],
			BITKASSA => ["BitKassa (Bitcoin)", 4],
			ADMINISTRATIVE => ["Administrative (other)", 5],
		);
	}

	until($payment->{'amount'}) {
		$payment->{'amount'} = $menu->question("Amount to pay (negative for amounts paid to them)? ");
		$payment->{'amount'} += 0;
	}

	until($payment->{'date'}) {
		my $dt = DateTime->now();
		$payment->{'date'} = $menu->question("Date of payment? [" . $dt->ymd . "] ");
		1 while chomp $payment->{'date'};
		$payment->{'date'} ||= $dt->ymd;
		delete $payment->{'date'} if $payment->{'date'} !~ /^\d{4}-\d\d-\d\d$/;
	}

	$payment->{'description'} = $menu->question("Description? ");
	1 while chomp $payment->{'description'};

	my $origin_question = $payment->{'type'} eq "BANK_TRANSACTION" ? "Origin bank account number" : "Origin (optional)";
	$payment->{'origin'} = $menu->question("$origin_question? ");
	1 while chomp $payment->{'origin'};

	print "Are you sure you want to create the following payment?\n";
	print Dumper($payment);
	if($menu->question("Are you sure? [yN] ") =~ /^y$/i) {
		create_payment($lim, $payment);
	}
}

1;
