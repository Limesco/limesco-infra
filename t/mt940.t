use Test::More tests => 17;
use strict;
use warnings;
require_ok 'Limesco::MT940';

is_deeply([Limesco::MT940->parse_fields("")], [], "Empty mt940 file");
is_deeply([Limesco::MT940->parse_fields(":11:abc")], [["11", "abc"]], "Single-line mt940 file");
is_deeply([Limesco::MT940->parse_fields(":11:abc\n:22:def")],
	[["11", "abc"], ["22", "def"]], "Two-line mt940 file");
is_deeply([Limesco::MT940->parse_fields(":11:abc\nfoobar\nquux\n:22:def")],
	[["11", "abc\nfoobar\nquux"], ["22", "def"]], "Two-line mt940 file with continuation lines");

undef $@;
eval { Limesco::MT940->parse_fields("abc") };
ok($@, "Non-mt940 line throws");

my $list = [['50', 'foo bar']];
is(Limesco::MT940->get_next_tag_value('50', $list), 'foo bar', 'get_next_tag_value');
is_deeply($list, [], 'get_next_tag_value removed entry');
$list = [['50', 'foo bar'], ['60', 'bar baz']];
is(Limesco::MT940->get_next_tag_value('50', $list), 'foo bar', 'get_next_tag_value');
is_deeply($list, [['60', 'bar baz']], 'get_next_tag_value removed entry');

undef $@;
eval { Limesco::MT940->get_next_tag_value('50', []) };
ok($@, "get_next_tag_value on empty list throws");

undef $@;
eval { Limesco::MT940->get_next_tag_value('50', [['60', 'bar baz']]) };
ok($@, "get_next_tag_value on incorrect tag throws");

my @fields = (['10', 'foo'], ['10', 'bar'], ['20', 'baz'], ['10', 'quux'], ['30', 'mumble']);
is_deeply([Limesco::MT940->cut_fields('10', @fields)],
	[
		[['10', 'foo']],
		[['10', 'bar'], ['20', 'baz']],
		[['10', 'quux'], ['30', 'mumble']],
	], "cut_fields cut correctly");

is_deeply([Limesco::MT940->cut_fields('10')], [], "cut_fields on empty list returns empty list");

undef $@;
eval { Limesco::MT940->cut_fields('10', ['20', 'foo'], ['10', 'bar']) };
ok($@, "cut_fields on list that doesn't start with right field throws");

my $mt940 = <<EOF;
:940:
:20:940S150107
:25:NL24RABO0169207587 EUR
:28C:0
:60F:C150106EUR000000012345,67
:61:150107D000000010101,10N012MARF            
NL12INGB1234567890
:86:/MARF/12345-001/EREF/Foo Bar Baz/ORDP//NAME/floepsie
woepselblaat/REMI/101202/CSID/NL12ZZZ012345678900
:61:150108D000000002020,02N345EREF            
NL18FVLB0123456789
:86:/EREF/12345678/BENM//NAME/Quux Mumble/REMI/This is a
test transaction/ISDT/2015-01-08
:62F:C150107EUR000000000224,55
:20:940S150108
:25:NL24RABO0169207587 EUR
:28C:0
:60F:C150108EUR000000000224,55
:61:150108C000000001113,00N123EREF            
NL12ABNA9876543210
:86:/EREF/123/ORDP//NAME/Stichting Gratis Geld/ADDR/TOERNOOIVELD 
100 6525EC NIJMEGEN NL/REMI/Gratis geld
/ISDT/2015-01-08
:62F:C150108EUR000000001337,55
:20:940S150109
:25:NL24RABO0169207587 EUR
:28C:0
:60F:C150108EUR000000001337,55
:62F:C150108EUR000000001337,55
EOF

is_deeply([Limesco::MT940->parse_from_string($mt940)],
	[{
		account => 'NL24RABO0169207587 EUR',
		date => '940S150107',
		start_balance => 'C150106EUR000000012345,67',
		end_balance => 'C150107EUR000000000224,55',
		transactions => [{
			description => "/MARF/12345-001/EREF/Foo Bar Baz/ORDP//NAME/floepsie\nwoepselblaat/REMI/101202/CSID/NL12ZZZ012345678900",
			transaction => "150107D000000010101,10N012MARF            \nNL12INGB1234567890",
		}, {
			description => "/EREF/12345678/BENM//NAME/Quux Mumble/REMI/This is a\ntest transaction/ISDT/2015-01-08",
			transaction => "150108D000000002020,02N345EREF            \nNL18FVLB0123456789",
		}],
		vol_balance => undef,
		statementnr => 0,
	}, {
		account => 'NL24RABO0169207587 EUR',
		date => '940S150108',
		start_balance => 'C150108EUR000000000224,55',
		end_balance => 'C150108EUR000000001337,55',
		transactions => [{
			description => "/EREF/123/ORDP//NAME/Stichting Gratis Geld/ADDR/TOERNOOIVELD \n100 6525EC NIJMEGEN NL/REMI/Gratis geld\n/ISDT/2015-01-08",
			transaction => "150108C000000001113,00N123EREF            \nNL12ABNA9876543210",
		}],
		vol_balance => undef,
		statementnr => 0,
	}, {
		account => 'NL24RABO0169207587 EUR',
		date => '940S150109',
		start_balance => 'C150108EUR000000001337,55',
		end_balance => 'C150108EUR000000001337,55',
		transactions => [],
		vol_balance => undef,
		statementnr => 0,
	}],
	"MT940 parsed from string correctly");

# Testcase for T36
$mt940 = <<EOF;
:940:
:20:940S150107
:25:NL24RABO0169207587 EUR
:28C:0
:60F:C150106EUR000000012345,67
:61:150107D000000010101,10N012MARF            
NL12INGB1234567890
:86:/MARF/12345-001/EREF/Foo Bar Baz/ORDP//NAME/floepsie
: woepselblaat/REMI/101202/CSID/NL12ZZZ012345678900
:62F:C150107EUR000000002244,57
EOF

is_deeply([Limesco::MT940->parse_from_string($mt940)],
	[{
		account => 'NL24RABO0169207587 EUR',
		date => '940S150107',
		start_balance => 'C150106EUR000000012345,67',
		end_balance => 'C150107EUR000000002244,57',
		transactions => [{
			description => "/MARF/12345-001/EREF/Foo Bar Baz/ORDP//NAME/floepsie\n: woepselblaat/REMI/101202/CSID/NL12ZZZ012345678900",
			transaction => "150107D000000010101,10N012MARF            \nNL12INGB1234567890",
		}],
		vol_balance => undef,
		statementnr => 0,
	}],
	"MT940 with colon injection parsed from string correctly");
