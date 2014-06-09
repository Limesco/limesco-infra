#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use DateTime;

my $invoice_id;
my $lim = Limesco->new_from_args(\@ARGV, sub {
	my ($args, $iref) = @_;
	my $arg = $args->[$$iref];
	return 0 if($invoice_id);
	$invoice_id = $arg;
	return 1;
});
if(!$invoice_id) {
	die <<"EOF";
Usage: $0 invoiceid\n
Given an invoice ID, writes the number of CDRs included in that invoice grouped
by the day the CDR was generated.
EOF
}
my $dbh = $lim->get_database_handle();
my $sth = $dbh->prepare("SELECT time::date, COUNT(id) FROM cdr WHERE invoice_id=? GROUP BY time::date ORDER BY time::date;");
$sth->execute($invoice_id);
while(my $row = $sth->fetchrow_arrayref) {
	print $row->[0] . " " . $row->[1] . "\n";
}
