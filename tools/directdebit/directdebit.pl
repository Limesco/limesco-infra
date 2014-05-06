#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Digest::MD5 qw(md5);
use Sys::Hostname;
use DateTime;

=head1 directdebit.pl

Usage: directdebit.pl [infra-options]

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV);
	print generate_directdebit_authorization($lim) . "\n";
}

=head2 Methods

=head3 generate_directdebit_authorization($lim)

Generate a unique direct debit authorization number. This number is supposed to
be a unique identifier for an authorization, which belongs to a specific account,
has a starting date and an optional ending date (or 'infinity') and notes IBAN and
BIC numbers.

=cut

sub generate_directdebit_authorization {
	my ($lim) = @_;
	# This code was heavily inspired by BSON ObjectID generation from bson-ruby
	use bytes;
	my $time = time();
	my $machine_id = (unpack("N", md5(hostname)))[0];
	my $process_id = $$;
	my $random = int(rand(1 << 32));
	my $ac = \($lim->{directdebit}{authorization_counter});
	$$ac = int(rand((1 << 8) - 1)) if(!$$ac || $$ac >= (1 << 8) - 1);
	my $counter = ++($$ac);
	my $binary = pack("N NX lXX n C", $time, $machine_id, $process_id, $random, $counter);
	my $str = "";
	for(0..length($binary)-1) {
		$str .= sprintf("%02x", ord(substr($binary, $_, 1)));
	}
	return $str;
}

=head3 add_directdebit_account($lim, $account_id, $authorization, $account_name, $iban, $bic, $date, [$enddate])

Add a directdebit authorization to the database for given account ID.
Authorization must be unique (preferably generated by
generate_directdebit_authorization), i.e. not given to a succeeding call to
add_directdebit_account before. $account_name is the bank account name and must
match the account name at the bank for the given IBAN number. $date is the date
of the authorization signature and signifies the start of this directdebit
authorization, it must match YYYY-MM-DD. $enddate is the optional end of the
authorization, if given it must also match YYYY-MM-DD as the first day where
the authorization is NOT active anymore (if not given, infinity is assumed).

=cut

sub add_directdebit_account {
	my ($lim, $account_id, $authorization, $account_name, $iban, $bic, $date, $enddate) = @_;

	if($date !~ /^\d{4}-\d\d-\d\d$/ || ($enddate && $enddate !~ /^\d{4}-\d\d-\d\d$/)) {
		die "Invalid date format";
	}

	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM account WHERE id=?");
	$sth->execute($account_id);
	my $account = $sth->fetchrow_hashref;
	if(!$account) {
		die "No such account";
	}

	$enddate ||= "";
	my $period = "[$date,$enddate)";
	$sth = $dbh->prepare("INSERT INTO account_directdebit_info (authorization_id, account_id,
		period, bank_account_name, iban, bic, signature_date) VALUES (?, ?, ?, ?, ?, ?, ?);");
	$sth->execute($authorization, $account_id, $period, $account_name, $iban, $bic, $date);
}

=head3 select_directdebit_invoices($lim, $authorization)

Select all invoices for the given authorization code. Note: does not check if
an invoice is already paid for, all invoices within a given authorization are
selected and returned.

=cut

sub select_directdebit_invoices {
	my ($lim, $authorization) = @_;
	my $dbh = $lim->get_database_handle();

	# Find all invoices whose date is within the period of this authorization, belonging to this account
	my $sth = $dbh->prepare("SELECT * FROM invoice WHERE"
		." account_id = (SELECT account_id FROM account_directdebit_info WHERE authorization_id=?) AND"
		." date <@ (SELECT period FROM account_directdebit_info WHERE authorization_id=?);");
	$sth->execute($authorization, $authorization);
	my @invoices;
	while(my $invoice = $sth->fetchrow_hashref) {
		push @invoices, $invoice;
	}
	return sort { $a->{'id'} cmp $b->{'id'} } @invoices;
}

=head3 create_directdebit_transaction($lim, $authorization, $invoice)

Create a transaction for a given invoice. This allows the invoice to be bundled
in a directdebit file to be sent to the bank. Returns the transaction.

=cut

sub create_directdebit_transaction {
	my ($lim, $authorization, $invoice) = @_;
	my $dbh = $lim->get_database_handle();

	# Status must be "NEW" until the transaction is claimed in a DD file
	my $status = 'NEW';

	my $sth = $dbh->prepare("INSERT INTO directdebit_transaction (invoice_id, authorization_id, status)
		VALUES (?, ?, ?)");
	$sth->execute($invoice->{'id'}, $authorization, $status);

	my $dd_trans_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "directdebit_transaction_id_seq"});
	return {
		id => $dd_trans_id,
		invoice_id => $invoice->{'id'},
		authorization_id => $authorization,
		directdebit_file_id => undef,
		status => $status,
	};
}

=head3 mark_directdebit_transaction($lim, $transaction_id, $status)

Change the status of a given directdebit transaction, i.e. mark it SUCCESS,
PRESETTLEMENTREJECT or POSTSETTLEMENTREJECT.

=cut

sub mark_directdebit_transaction {
	my ($lim, $transaction_id, $status) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("UPDATE directdebit_transaction SET status=? WHERE id=?");
	$sth->execute($status, $transaction_id);
}

=head3 get_directdebit_transaction($lim, $transaction_id)

Return a directdebit transaction created earlier using create_directdebit_transaction.

=cut

sub get_directdebit_transaction {
	my ($lim, $transaction_id) = @_;
	my $dbh = $lim->get_database_handle();

	my $sth = $dbh->prepare("SELECT * FROM directdebit_transaction WHERE id=?");
	$sth->execute($transaction_id);
	my $transaction = $sth->fetchrow_hashref;
	if(!$transaction) {
		die "No such DirectDebit transaction $transaction_id";
	}
	return $transaction;
}

=head3 create_directdebit_file($lim, $filetype, [$processing_date])

Bundle transactions into a directdebit file. A file must only have first-time
or recurring transactions; the type of file you want is indicated by the
$filetype parameter which must either be equal to 'FRST' for a first-time
transaction file or 'RCUR' to include only recurring transactions. If you have
transactions of both types, generate both files.

The processing date is optional; if not given it will be today + 14 days. In
European Direct Debit rules, it must be at least six working days in the future
for 'FRST' type files and at least three working days in the future for 'RCUR'
type files. This method does not check that the processing date you give is
correct according to this rule.

This method throws an exception when no transactions were found of this type.

=cut

sub create_directdebit_file {
	my ($lim, $filetype, $processing_date) = @_;

	if($filetype ne 'RCUR' && $filetype ne 'FRST') {
		die "Not implemented for file type $filetype, only RCUR and FRST are implemented";
	}

	my $dbh = $lim->get_database_handle();
	$dbh->begin_work;
	try {
		$dbh->do("LOCK TABLE directdebit_file;");
		$dbh->do("LOCK TABLE directdebit_transaction;");

		if(!$processing_date) {
			$processing_date = DateTime->now->add(days => 14)->ymd();
		}

		# Create the file
		$dbh->do("INSERT INTO directdebit_file (creation_date, processing_date, type) VALUES ('today', ?, ?)", undef, $processing_date, $filetype);
		my $dd_file_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "directdebit_file_id_seq"});

		# Prepare retrieving transaction information
		my $previous_transaction_count_sth = $dbh->prepare("SELECT COUNT(id) FROM directdebit_transaction WHERE status='SUCCESS' OR status='POSTSETTLEMENTREJECT' AND authorization_id=?");
		# Prepare claiming the transaction for our file
		my $claim_transaction_sth = $dbh->prepare("UPDATE directdebit_transaction SET directdebit_file_id=? WHERE id=?");

		# Take all unclaimed transactions
		my $sth = $dbh->prepare("SELECT * FROM directdebit_transaction WHERE directdebit_file_id IS NULL");
		$sth->execute();

		my $transactions_claimed = 0;
		while(my $transaction = $sth->fetchrow_hashref) {
			# Is it a FRST- or RCUR-type transaction?
			$previous_transaction_count_sth->execute($transaction->{'authorization_id'});
			my $count = $previous_transaction_count_sth->fetchrow_arrayref->[0];

			my $transactiontype = $count > 0 ? "RCUR" : "FRST";
			if($filetype eq $transactiontype) {
				# Claim it
				$claim_transaction_sth->execute($dd_file_id, $transaction->{'id'});
				++$transactions_claimed;
			}
		}

		if(!$transactions_claimed) {
			die "No transactions to include in this file";
		}

		$dbh->commit;
		return get_directdebit_file($lim, $dd_file_id);
	} catch {
		$dbh->rollback;
		die $_;
	};
}

=head3 mark_directdebit_file($lim, $file_id, $status)

Change the status of a given directdebit file, i.e. mark it SUCCESS, or
PRESETTLEMENTREJECT. Marking a whole file POSTSETTLEMENTREJECT is impossible.
If a file is marked SUCCESS, existing marks for transactions will not be
changed. If a file is marked PRESETTLEMENTREJECT, all transactions in the file
must still be NEW.

The logic behind this is that a file is checked by the local bank, while
individual transactions are checked by the local bank and by the remote bank. A
file can either completely succeed or completely fail; it can only completely
fail at the local bank, in which case the transactions will not be sent to the
remote bank i.e. a post-settlement reject is impossible. If a file fails, all
transactions will be lost as if they never existed, which has the same
semantics as a pre-settlement reject for all transactions. However, if a file
succeeded, it is possible that some of the transactions in it have been
rejected (a partial success).

=cut

sub mark_directdebit_file {
	my ($lim, $file_id, $status) = @_;

	if($status ne "SUCCESS" && $status ne "PRESETTLEMENTREJECT") {
		die "Status in mark_directdebit_file must be SUCCESS or PRESETTLEMENTREJECT";
	}

	my $dbh = $lim->get_database_handle();
	$dbh->begin_work;
	$dbh->do("LOCK TABLE directdebit_transaction;");

	if($status eq "PRESETTLEMENTREJECT") {
		my $sth = $dbh->prepare("SELECT COUNT(id) FROM directdebit_transaction WHERE directdebit_file_id=? AND status <> 'NEW'");
		$sth->execute($file_id);
		my $count = $sth->fetchrow_arrayref->[0];
		if($count != 0) {
			die "A file cannot be marked PRESETTLEMENTREJECT if it already has marked transactions";
		}
	}

	my $sth = $dbh->prepare("UPDATE directdebit_transaction SET status=? WHERE directdebit_file_id=? AND status='NEW'");
	$sth->execute($status, $file_id);
	$dbh->commit;
}

=head3 get_directdebit_file($lim, $id)

Retrieve directdebit file information from the database.

=cut

sub get_directdebit_file {
	my ($lim, $id) = @_;
	my $dbh = $lim->get_database_handle();

	my $sth = $dbh->prepare("SELECT * FROM directdebit_file WHERE id=?");
	$sth->execute($id);
	my $file = $sth->fetchrow_hashref;
	if(!$file) {
		die "No such DirectDebit file $id";
	}
	return $file;
}

1;
