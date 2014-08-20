#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Data::Dumper;
use XML::Twig;

require "./transaction_codes.pl";
our $trx_codes;

=head1 swift-import.pl

Usage: swift-import.pl [infra-options]
                       [--format <format>]
                       [--type <type>]
                       --filename <filename>

This tool can be used to view or import a bank statement (CAMT.053 XML format).

  --format   <format>    Defaults to plain (table-like)
  --type     <type>      'raw' for Perl datastructures in Dumper-format
  --filename <filename>  Which file to read

=cut

my @bankstatements;

if(!caller) {
	my $debug = 1;
	my $format = "plain";
	my $filename = "";
	my $type = "raw";
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--format") {
			$format = $args->[++$$iref];
		} elsif($arg eq "--type") {
			$type = $args->[++$$iref];
		} elsif($arg eq "--filename") {
			$filename = $args->[++$$iref];
		} else {
			return 0;
		}
	});
	
	die "No filename given.\n" if (!$filename);

	print Dumper(@bankstatements) if ($type eq "raw");

}

=head2 Methods

=head3 Import-related functions

=head4 import_bankstatement ($filename)

Calls functions related to importing the bank statement

=cut

sub import_bankstatement {
	my $filename = shift;

	die "Given filename not readable.\n" if (! -r $filename );
	die "Given filename has zero size.\n" if ( -z $filename );

	my $swift = new XML::Twig(
		twig_handlers =>
		{ BkToCstmrStmt => \&handle_bankstatement }
	);
	$swift->parsefile($filename);
}

=head4 handle_bankstatement ($twig, $element)

Twig handler for each bank statement. Adds every BkToCstmrStmt to the global
bankstatements array.

=cut

sub handle_bankstatement {
	my ($twig, $bankstmt) = @_;
	my $bank_statement;
	my $group_header = $bankstmt->first_child('GrpHdr');
	$bank_statement->{message_id} = get_fc_text($group_header, 'MsgId');
	$bank_statement->{creation_time} = get_fc_text($group_header, 'CreDtTm');
	$bank_statement->{statements} = [];

	push @bankstatements, $bank_statement;

	my @statements = $bankstmt->get_xpath('Stmt');
	foreach my $stm (@statements) {
		push $bankstatements[$#bankstatements]->{statements}, process_statement ($stm);
	}
}

=head4 process_statement ($stmt)

Reads a <Stmt> element and extracts information.

=cut

sub process_statement {
	my $statement = shift;
	my $stmt;
	$stmt->{id} = get_fc_text($statement, 'Id');
	$stmt->{creation_time} = get_fc_text($statement, 'CreDtTm');
	$stmt->{account}->{iban} = get_fc_text(get_fc(get_fc($statement, 'Acct'), 'Id'), 'IBAN');
	$stmt->{account}->{currency} = get_fc_text(get_fc($statement, 'Acct'), 'Ccy');
	$stmt->{balance}->{open} = process_balance($statement, 'open');
	$stmt->{balance}->{close} = process_balance($statement, 'close');
	$stmt->{balance}->{diff} = process_balance($statement, 'diff');
	$stmt->{balance}->{open_date} = process_balance($statement, 'open_date');
	$stmt->{balance}->{close_date} = process_balance($statement, 'close_date');
	$stmt->{balance}->{open_currency} = process_balance($statement, 'open_currency');
	$stmt->{balance}->{close_currency} = process_balance($statement, 'close_currency');
	$stmt->{entries} = process_entries($statement);


	return $stmt;
}

=head4 get_child_text ($element, $child_name)

Get the text content of the first child element $child_name

=cut

sub get_fc_text {
	return get_fc(@_)->text;
}

=head4 get_fc ($element, $child_name)

Gets first child $child_name from Twig $element

=cut

sub get_fc {
	my ($elem, $cn) = @_;
	return $elem->first_child($cn);
}

=head4 process_balance ($element, $type)

Get the opening and closing balace of a statement and calculate the difference

=cut

sub process_balance {
	my ($elem, $type) = @_;

	my @balances = $elem->get_xpath('Bal');
	#print Dumper(@balances);

	my $balance_open;
	my $date_open;
	my $currency_open;
	my $balance_close;
	my $date_close;
	my $currency_close;

	foreach my $balance (@balances) {
		my $type = get_fc_text(get_fc(get_fc($balance, 'Tp'), 'CdOrPrtry'), 'Cd');
		my $amount = get_fc_text($balance, 'Amt');
		my $sign = (get_fc_text($balance, 'CdtDbtInd') eq "CRDT") ? 1 : -1;
		$amount *= $sign;
		my $date = get_fc_text(get_fc($balance, 'Dt'), 'Dt');

		$balance_open = sprintf("%.2f", $amount) if ($type eq 'OPBD');
		$date_open = $date if ($type eq 'OPBD');
		$currency_open = get_fc($balance, 'Amt')->att('Ccy') if ($type eq 'OPBD');
		$balance_close = sprintf("%.2f", $amount) if ($type eq 'CLBD');
		$date_close = $date if ($type eq 'CLBD');
		$currency_close = get_fc($balance, 'Amt')->att('Ccy') if ($type eq 'CLBD');
	}

	die "swift-import: process_balance failed! Open/Closing balance: '$balance_open'/'$balance_close'\n" if ((!$balance_open or !$balance_close) and ($date_open eq $date_close));

	return $balance_open if ($type eq 'open');
	return $balance_close if ($type eq 'close');
	return $date_open if ($type eq 'open_date');
	return $date_close if ($type eq 'close_date');
	return $currency_open if ($type eq 'open_currency');
	return $currency_close if ($type eq 'close_currency');
	return sprintf("%.2f", $balance_close - $balance_open) if ($type eq 'diff');
}

=head4 process_entries ($element)

Process all entries in the statement and return a readable format

=cut

sub process_entries {
	my ($elem, $type) = @_;
	my @entries = $elem->get_xpath('Ntry');

	my @results;

	foreach my $entry (@entries) {
		my $res;
		my $amount = get_fc_text($entry, 'Amt');
		my $sign = (get_fc_text($entry, 'CdtDbtInd') eq "CRDT") ? 1 : -1;
		$res->{amount} = sprintf("%.2f", $amount*$sign);
		$res->{booking_date} = get_fc_text(get_fc($entry, 'BookgDt'), 'Dt');
		$res->{value_date} = get_fc_text(get_fc($entry, 'ValDt'), 'Dt');
		$res->{transaction_code} = get_fc_text(get_fc(get_fc($entry, 'BkTxCd'), 'Prtry'), 'Cd');
		$res->{transaction_code_hr} = $trx_codes->{$res->{transaction_code}};
		$res->{reversal_indicator} = get_fc_text($entry, 'RvslInd');

		my $related_parties = get_fc(get_fc(get_fc($entry, 'NtryDtls'), 'TxDtls'), 'RltdPties');
		if ($related_parties) {
			if (get_fc($related_parties, 'Dbtr')) {
				$res->{name} = get_fc_text(get_fc($related_parties, 'Dbtr'), 'Nm');
				$res->{iban} = get_fc_text(get_fc(get_fc($related_parties, 'DbtrAcct'), 'Id'), 'IBAN');
			} else {
				$res->{name} = get_fc_text(get_fc($related_parties, 'Cdtr'), 'Nm');
			}
		}

		my $remittance_information = get_fc(get_fc(get_fc($entry, 'NtryDtls'), 'TxDtls'),'RmtInf');
		$res->{description} = get_fc_text($remittance_information, 'Ustrd') if ($remittance_information);

		push @results, $res;
	}

	my $results_ref = \@results;

	return $results_ref;
}

=head3 Export-related functions

=head4 export_to_database

Exports all information to the database from @bankstatements

=cut

sub export_to_database {
	my $num_statements = @bankstatements;

	die "No statements available to be exported.\n" if ($num_statements == 0);
}

1;
