#!/usr/bin/perl
use strict;
use warnings;

use Test::PostgreSQL;
use Test::More;
use Test::Exception;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use File::Temp qw(tempfile);

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 5;

require_ok("swift-import.pl");

my $lim = Limesco->new_for_test($pgsql->dsn);
my $dbh = $lim->get_database_handle();

require('../upgrade/upgrade.pl');
initialize_database($lim);

dies_ok( sub { export_to_database() }, "Exporting to database without statements? No, Mr. Bond. I expect you to die." );
dies_ok( sub { import_bankstatement("i_do_not_exist.file") }, "Careful with that non-existing file, Eugene." );

my $tmpfh = File::Temp->new( DIR => "./" );

print $tmpfh <<"EOF";
<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02">
<BkToCstmrStmt>
<GrpHdr>
<MsgId>CAMT053RIB000000012345</MsgId>
<CreDtTm>2014-01-01T00:00:07+00:00</CreDtTm>
</GrpHdr>
<Stmt>
<Id>010EURNL24RABO0169207587</Id>
<CreDtTm>2014-01-01T00:00:07+00:00</CreDtTm>
<Acct>
<Id>
<IBAN>NL24RABO0169207587</IBAN>
</Id>
<Ccy>EUR</Ccy>
</Acct>
<Bal>
<Tp>
<CdOrPrtry>
<Cd>OPBD</Cd>
</CdOrPrtry>
</Tp>
<Amt Ccy="EUR">1337.00</Amt>
<CdtDbtInd>CRDT</CdtDbtInd>
<Dt>
<Dt>2014-01-01</Dt>
</Dt>
</Bal>
<Bal>
<Tp>
<CdOrPrtry>
<Cd>CLBD</Cd>
</CdOrPrtry>
</Tp>
<Amt Ccy="EUR">1337.00</Amt>
<CdtDbtInd>CRDT</CdtDbtInd>
<Dt>
<Dt>2014-04-01</Dt>
</Dt>
</Bal>
<TxsSummry>
<TtlNtries>
<NbOfNtries>0</NbOfNtries>
<Sum>0.00</Sum>
<TtlNetNtryAmt>0.00</TtlNetNtryAmt>
<CdtDbtInd>CRDT</CdtDbtInd>
</TtlNtries>
<TtlCdtNtries>
<NbOfNtries>0</NbOfNtries>
<Sum>0.00</Sum>
</TtlCdtNtries>
<TtlDbtNtries>
<NbOfNtries>0</NbOfNtries>
<Sum>0.00</Sum>
</TtlDbtNtries>
</TxsSummry>
</Stmt>
</BkToCstmrStmt>
</Document>
EOF

$tmpfh->flush();

ok( ! -z $tmpfh, "Tempfile is not empty." );

lives_ok ( sub { import_bankstatement( $tmpfh->filename ) } , "Import sample bankstatement succeeded.");

$dbh->disconnect();
