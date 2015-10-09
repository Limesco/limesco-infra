#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::TemporalSupport;
use Digest::MD5 qw(md5);
use Sys::Hostname;
use DateTime;
use Try::Tiny;
use Business::IBAN;
use Encode;
use v5.14; # Unicode string features
use open qw( :encoding(UTF-8) :std);

do '../balance/balance.pl' or die $! unless $INC{'../balance/balance.pl'};

=head1 directdebit.pl

Usage: directdebit.pl <--collect | --generate | --authorize | --export id> [--processing-date yyyy-mm-dd] [infra-options]

If --collect is given, this tool creates directdebit files. It takes all
invoices for which a valid directdebit authorization exists and which have not
been collected through directdebit yet, creates transactions for them and files
for those transactions. The files are written as LDD-date-FRST.xml and
LDD-date-RCUR.xml.  If no transactions existed for a type, no file is written.
--processing-date is required, it is the date at which the direct debit order
will be processed (the date the funds will be transferred).

If --generate is given, generates a directdebit authentication id and exits.

If --authorize is given, interactively asks a few authorization questions, then
adds an authorization from signed form into the database.

If --export is given, writes an XML file for the given directdebit file or
message ID.

=cut

if(!caller) {
	my $generate;
	my $collect;
	my $export;
	my $authorize;

	my $processing_date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--generate") {
			$generate = 1;
		} elsif($arg eq "--collect") {
			$collect = 1;
		} elsif($arg eq "--processing-date") {
			$processing_date = $args->[++$$iref];
		} elsif($arg eq "--export") {
			$export = $args->[++$$iref];
		} elsif($arg eq "--authorize") {
			$authorize = 1;
		} else {
			return 0;
		}
	});

	if($generate) {
		print generate_directdebit_authorization($lim) . "\n";
		exit(0);
	}

	if($collect) {
		if(!$processing_date) {
			die "Missing parameter --processing-date\n";
		}

		my @auths = get_active_directdebit_authorizations($lim);
		foreach my $authorization (@auths) {
			my @p_and_i = get_payments_and_invoices($lim, $authorization->{'account_id'});
			if(!@p_and_i) {
				next;
			}

			my $balance = $p_and_i[$#p_and_i]{'balance'};
			if($balance >= 0) {
				next;
			}

			# As invoice ID, take the last invoice open
			my $invoice_id;
			for(reverse @p_and_i) {
				if($_->{'objecttype'} eq "INVOICE") {
					$invoice_id = $_->{'id'};
					last;
				}
			}

			try {
				if($invoice_id) {
					create_directdebit_transaction($lim, $authorization->{'authorization_id'}, $invoice_id, -$balance);
				} else {
					die "No invoices found\n";
				}
			} catch {
				warn "Could not create directdebit transaction for account ID " . $authorization->{'account_id'} . ": $_\n";
			}
		}
		try {
			my $file = create_directdebit_file($lim, "FRST", $processing_date);
			my $filename = $file->{'message_id'} . ".xml";
			my $xml = export_directdebit_file($lim, $file->{'id'});
			open my $fh, '>', $filename or die $!;
			print $fh $xml;
			close $fh;
			print "FRST file written to $filename.\n";
		} catch {
			print "Failed to create FRST file: $_";
		};
		try {
			my $file = create_directdebit_file($lim, "RCUR", $processing_date);
			my $filename = $file->{'message_id'} . ".xml";
			my $xml = export_directdebit_file($lim, $file->{'id'});
			open my $fh, '>', $filename or die $!;
			print $fh $xml;
			close $fh;
			print "RCUR file written to $filename.\n";
		} catch {
			print "Failed to create RCUR file: $_";
		};
		exit(0);
	}

	if($authorize) {
		my $ask_question = sub {
			print $_[0] . "\n";
			my $answer = <STDIN>;
			$answer = decode_utf8($answer);
			1 while chomp $answer;
			return $answer;
		};
		my $account_id = $ask_question->("Add authorization for what account name/ID?");
		# If it's not just numbers, find an account with a similar name
		$account_id = $lim->get_account_like($account_id)->{'id'} if $account_id !~ /^\d+$/;
		my $auth_id = $ask_question->("Authorization ID?");
		my $bank_account_name = $ask_question->("Bank account name?");
		my $iban = $ask_question->("IBAN?");
		my $bic = $ask_question->("BIC?");
		my $date = $ask_question->("Signature date (YYYY-MM-DD)?");
		add_directdebit_account($lim, $account_id, $auth_id, $bank_account_name, $iban, $bic, $date);
		print "Successfully added authorization $auth_id.\n";
		exit(0);
	}

	if($export) {
		my $file;
		if($export =~ /^LDD/) {
			$file = get_directdebit_file($lim, $export);
		} else {
			$file = get_directdebit_file($lim, $export);
		}
		my $filename = $file->{'message_id'} . ".xml";
		my $xml = export_directdebit_file($lim, $file->{'id'});
		open my $fh, '>', $filename or die $!;
		print $fh $xml;
		close $fh;
		print "Directdebit file written to $filename.\n";
		exit(0);
	}

	print "One of the --collect, --generate, --authorize or --export options is required.\n";
	exit(1);
}

=head2 Methods

=cut

sub _directdebit_object_info {
	return {
		required_fields => [qw(authorization_id account_id bank_account_name iban bic signature_date)],
		optional_fields => [],
		table_name => "account_directdebit_info",
		primary_key => "authorization_id",
	};
}

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

=head3 add_directdebit_account($lim, $account_id, $authorization, $account_name, $iban, $bic, $date)

Add a directdebit authorization to the database for given account ID.
Authorization must be unique (preferably generated by
generate_directdebit_authorization), i.e. not given to a succeeding call to
add_directdebit_account before. $account_name is the bank account name and must
match the account name at the bank for the given IBAN number. The check numbers
in $iban are checked by the Business::IBAN module. $date is the date of the
authorization signature and signifies the start of this directdebit
authorization, it must match YYYY-MM-DD.

=cut

sub add_directdebit_account {
	my ($lim, $account_id, $authorization, $account_name, $iban, $bic, $date) = @_;

	if($date !~ /^\d{4}-\d\d-\d\d$/) {
		die "Invalid date format";
	}

	{
		my $iban_module = Business::IBAN->new();
		if(!$iban_module->valid($iban)) {
			die "IBAN number is invalid: $iban (auth_id $authorization, bank account name $account_name)";
		}
	}

	my $dbh = $lim->get_database_handle();
	my $account = $lim->get_account($account_id);

	return create_object($lim, _directdebit_object_info(), {
		authorization_id => $authorization,
		account_id => $account_id,
		bank_account_name => $account_name,
		iban => $iban,
		bic => $bic,
		signature_date => $date,
	}, $date);
}

=head3 delete_directdebit_account($lim, $authorization, $enddate)

End a directdebit authorization on the given date. Once ended, an authorization
cannot be re-activated.

=cut

sub delete_directdebit_account {
	my ($lim, $authorization, $enddate) = @_;
	delete_object($lim, _directdebit_object_info(), $authorization, $enddate);
}

=head3 get_all_directdebit_authorizations($lim, [$accountid])

Returns a list of all directdebit authorizations. When an $accountid is given,
limit to those on a given account.

=cut

sub get_all_directdebit_authorizations {
	my ($lim, $accountid) = @_;
	my $dbh = $lim->get_database_handle();

	my $query = "SELECT * FROM account_directdebit_info";
	if($accountid) {
		$query .= " WHERE account_id=?";
	}
	my $sth = $dbh->prepare($query);
	$sth->execute($accountid ? ($accountid) : ());
	my @authorizations;
	while(my $row = $sth->fetchrow_hashref) {
		push @authorizations, $row;
	}
	return @authorizations;
}

=head3 get_active_directdebit_authorizations($lim, [$accountid])

Returns a list of all active directdebit authorizations. When an $accountid is given,
limit to those on a given account.

=cut

sub get_active_directdebit_authorizations {
	my ($lim, $accountid) = @_;
	my $dbh = $lim->get_database_handle();

	# Find all invoices whose date is within the period of this authorization, belonging to this account
	my $query = "SELECT * FROM account_directdebit_info WHERE period @> 'now'::date";
	if($accountid) {
		$query .= " AND account_id=?";
	}
	my $sth = $dbh->prepare($query);
	$sth->execute($accountid ? ($accountid) : ());
	my @authorizations;
	while(my $row = $sth->fetchrow_hashref) {
		push @authorizations, $row;
	}
	return @authorizations;
}

=head3 create_directdebit_transaction($lim, $authorization, $invoice_id, $amount)

Create a transaction for a given invoice ID and amount. The invoice ID is only
used for the description; the debited amount comes from the parameters. Returns
the transaction. Throws an exception if the given amount is below zero.

=cut

sub create_directdebit_transaction {
	my ($lim, $authorization, $invoice_id, $amount) = @_;
	my $dbh = $lim->get_database_handle();

	$amount = sprintf("%.2f", $amount);
	if($amount <= 0) {
		die "Cannot create directdebit transaction for amounts below zero.";
	}

	if(ref($invoice_id)) {
		$invoice_id = $invoice_id->{'id'};
	}

	# Status must be "NEW" until the transaction is claimed in a DD file
	my $status = 'NEW';

	my $sth = $dbh->prepare("INSERT INTO directdebit_transaction (invoice_id, authorization_id, status, amount)
		VALUES (?, ?, ?, ?)");
	$sth->execute($invoice_id, $authorization, $status, $amount);

	my $dd_trans_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "directdebit_transaction_id_seq"});
	return {
		id => $dd_trans_id,
		invoice_id => $invoice_id,
		authorization_id => $authorization,
		directdebit_file_id => undef,
		status => $status,
		amount => $amount,
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
		$dbh->do("INSERT INTO directdebit_file (message_id, creation_time, processing_date, type) VALUES (CONCAT('LDD-', 'today'::date, '-', ?::text), 'now', ?, ?)", undef, $filetype, $processing_date, $filetype);
		my $dd_file_id = $dbh->last_insert_id(undef, undef, undef, undef, {sequence => "directdebit_file_id_seq"});

		# Prepare retrieving transaction information
		my $previous_transaction_count_sth = $dbh->prepare("SELECT COUNT(id) FROM directdebit_transaction WHERE (status='SUCCESS' OR status='POSTSETTLEMENTREJECT') AND authorization_id=?");
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

=head3 mark_directdebit_file($lim | $dbh, $file_id, $status)

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

If a $lim object is given, a new transaction will be started. Otherwise, it is
assumed that the given handle already has a running transaction.

=cut

sub mark_directdebit_file {
	my ($lim, $file_id, $status) = @_;

	if($status ne "SUCCESS" && $status ne "PRESETTLEMENTREJECT") {
		die "Status in mark_directdebit_file must be SUCCESS or PRESETTLEMENTREJECT";
	}

	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;
	if($dbh_is_mine) {
		$dbh->begin_work;
		$dbh->do("LOCK TABLE directdebit_transaction;");
	}

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
	$dbh->commit if $dbh_is_mine;
}

=head3 get_directdebit_file($lim | $dbh, $id)

Retrieve directdebit file information from the database.

=cut

sub get_directdebit_file {
	my ($lim, $id) = @_;
	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;

	my $field = $id =~ /^LDD/ ? 'message_id' : 'id';

	my $sth = $dbh->prepare("SELECT * FROM directdebit_file WHERE $field=?");
	$sth->execute($id);
	my $file = $sth->fetchrow_hashref;
	if(!$file) {
		die "No such DirectDebit file $id";
	}

	$file->{'transactions'} ||= [];

	$sth = $dbh->prepare("SELECT * FROM directdebit_transaction WHERE directdebit_file_id=?");
	$sth->execute($file->{'id'});
	while(my $transaction = $sth->fetchrow_hashref) {
		push @{$file->{'transactions'}}, $transaction;
	}

	return $file;
}

=head3 export_directdebit_file($lim, $id)

Export a directdebit file to XML. Returns a string containing the XML contents.

=cut

sub export_directdebit_file {
	my ($lim, $file_id) = @_;
	my $dbh = $lim->get_database_handle();

	my $file = get_directdebit_file($lim, $file_id);
	my @transactions;
	{
		my $transactions_sth = $dbh->prepare("SELECT * FROM directdebit_transaction
			FULL JOIN account_directdebit_info ON directdebit_transaction.authorization_id = account_directdebit_info.authorization_id
			WHERE directdebit_file_id=? ORDER BY invoice_id");
		$transactions_sth->execute($file_id);
		while(my $t = $transactions_sth->fetchrow_hashref) {
			push @transactions, $t;
		}
	}

	if(@transactions == 0) {
		die "No transactions in directdebit file, this is impossible";
	}

	$file->{'creation_time'} =~ s/ /T/;

	my $sum = 0;
	foreach(@transactions) {
		$sum += $_->{'amount'};
	}
	$sum = sprintf("%.2f", $sum);

	my @export_xml;
	push @export_xml,
		'<?xml version="1.0" encoding="utf-8"?>',
		'<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02"',
		' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">',
		' <CstmrDrctDbtInitn>',
		'  <GrpHdr>',
		'   <MsgId>' . $file->{'message_id'} . '</MsgId>',
		'   <CreDtTm>' . $file->{'creation_time'} . '</CreDtTm>',
		'   <NbOfTxs>' . scalar @transactions . '</NbOfTxs>',
		'   <CtrlSum>' . $sum . '</CtrlSum>',
		'   <InitgPty>',
		'    <Nm>Limesco</Nm>',
		'   </InitgPty>',
		'  </GrpHdr>',
		'  <PmtInf>',
		'   <PmtInfId>' . $file->{'message_id'} . '</PmtInfId>',
		'   <PmtMtd>DD</PmtMtd>',
		'   <BtchBookg>true</BtchBookg>',
		'   <NbOfTxs>' . scalar @transactions . '</NbOfTxs>',
		'   <CtrlSum>' . $sum . '</CtrlSum>',
		'   <PmtTpInf>',
		'    <SvcLvl>',
		'     <Cd>SEPA</Cd>',
		'    </SvcLvl>',
		'    <LclInstrm>',
		'     <Cd>CORE</Cd>',
		'    </LclInstrm>',
		'    <SeqTp>' . $file->{'type'} . '</SeqTp>',
		'   </PmtTpInf>',
		'   <ReqdColltnDt>' . $file->{'processing_date'} . '</ReqdColltnDt>',
		'   <Cdtr>',
		# TODO: this should be taken from a configuration file
		'    <Nm>Limesco B.V.</Nm>',
		'    <PstlAdr>',
		'     <Ctry>NL</Ctry>',
		'     <AdrLine>Toernooiveld 100</AdrLine>',
		'     <AdrLine>6525EC Nijmegen</AdrLine>',
		'    </PstlAdr>',
		'   </Cdtr>',
		'   <CdtrAcct>',
		'    <Id>',
		'     <IBAN>NL24RABO0169207587</IBAN>',
		'    </Id>',
		'    <Ccy>EUR</Ccy>',
		'   </CdtrAcct>',
		'   <CdtrAgt>',
		'    <FinInstnId>',
		'     <BIC>RABONL2U</BIC>',
		'    </FinInstnId>',
		'   </CdtrAgt>',
		'   <ChrgBr>SLEV</ChrgBr>',
		'   <CdtrSchmeId>',
		'    <Id>',
		'     <PrvtId>',
		'      <Othr>',
		'       <Id>NL16LIM552587780000</Id>',
		'       <SchmeNm>',
		'        <Prtry>SEPA</Prtry>',
		'       </SchmeNm>',
		'      </Othr>',
		'     </PrvtId>',
		'    </Id>',
		'   </CdtrSchmeId>';
	foreach(@transactions) {
		push @export_xml,
			'   <DrctDbtTxInf>',
			'    <PmtId>',
			'     <InstrId>' . $_->{'invoice_id'} . '</InstrId>',
			'     <EndToEndId>' . $_->{'invoice_id'} . '</EndToEndId>',
			'    </PmtId>',
			'    <InstdAmt Ccy="EUR">' . $_->{'amount'} . '</InstdAmt>',
			'    <DrctDbtTx>',
			'     <MndtRltdInf>',
			'      <MndtId>' . $_->{'authorization_id'} . '</MndtId>',
			'      <DtOfSgntr>' . $_->{'signature_date'} . '</DtOfSgntr>',
			'     </MndtRltdInf>',
			'    </DrctDbtTx>',
			'    <DbtrAgt>',
			'     <FinInstnId>',
			'      <BIC>' . $_->{'bic'} . '</BIC>',
			'     </FinInstnId>',
			'    </DbtrAgt>',
			'    <Dbtr>',
			'     <Nm>' . $_->{'bank_account_name'} . '</Nm>',
			'     <CtryOfRes>' . substr($_->{'iban'}, 0, 2) . '</CtryOfRes>',
			'    </Dbtr>',
			'    <DbtrAcct>',
			'     <Id>',
			'      <IBAN>' . $_->{'iban'} . '</IBAN>',
			'     </Id>',
			'    </DbtrAcct>',
			'   </DrctDbtTxInf>';
	}
	push @export_xml,
		'  </PmtInf>',
		' </CstmrDrctDbtInitn>',
		'</Document>';
	return join "\n", @export_xml;
}

1;
