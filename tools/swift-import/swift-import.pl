#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Data::Dumper;
use XML::Twig;

=head1 swift-import.pl

Usage: swift-import.pl [infra-options]

=cut

my @bankstatements;

if(!caller) {
	my $debug = 1;
	my $format = "plain";
	my $filename = "";
	my $type = "invoice";
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

	my $swift = new XML::Twig(
		twig_handlers =>
		{ BkToCstmrStmt => \&handle_bankstatement }
	);
	$swift->parsefile($filename);
	
	print Dumper(@bankstatements);

}

=head2 Methods

=head3 Twig-related methods

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

	my @statements = $bankstmt->get_xpath('Stmt');
	foreach my $stm (@statements) {
		process_statement ($stm);
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

	push @bankstatements, $stmt;
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

1;
