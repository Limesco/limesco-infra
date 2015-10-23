use Test::More tests => 4;
use strict;
use warnings;
use File::Temp qw/tempdir/;
require_ok 'Limesco::Util';

sub touch {
	open my $fh, '>', $_[0] or die $!;
	close $fh;
}

my $dir = tempdir(CLEANUP => 1);
is_deeply([Limesco::Util::directory_list($dir)], [], "Empty directory contains no entries");
touch("$dir/foo.txt");
is_deeply([Limesco::Util::directory_list($dir)], ["foo.txt"], "Directory contains file");
mkdir("$dir/bar");
touch("$dir/bar/baz.txt");
touch("$dir/bar/quux.txt");
is_deeply([Limesco::Util::directory_list($dir)], ["bar/baz.txt", "bar/quux.txt", "foo.txt"], "Directory contains directories");
