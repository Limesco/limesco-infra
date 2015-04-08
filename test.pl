#!/usr/bin/perl
use strict;
use warnings;
use TAP::Harness;
use File::Basename qw(basename dirname);
use File::Find;
use Cwd qw(realpath getcwd);
use Config;

# This variable lists all test files that are run first, in the order given.
# All other .t files are run in arbitrary order after these files are done.
my @test_files = (
	"tools/upgrade/upgrade.t",
);

##### No configuration below this point

# Find an absolute path to myself
my $basepath = realpath(dirname($0));
if(!$basepath) {
	die "Could not find a path to myself, therefore could not run tests.\n";
}

# Find all other tests
find(sub {
	if(/\.t$/) {
		my $name = substr($File::Find::name, length($basepath) + 1);
		push @test_files, $name
			unless grep {$_ eq $name} @test_files;
	}
}, $basepath."/t", $basepath."/tools");

# Run them
my $harness = TAP::Harness->new({
	color => 1,
	exec => sub {
		my (undef, $test) = @_;

		my $test_dir = $basepath . '/' . dirname($test);
		my $test_file = basename($test);

		return [$Config{perlpath}, "-I$basepath/lib", "-e", "chdir('$test_dir'); do('$test_file');"];
	},
});
$harness->runtests(@test_files);
