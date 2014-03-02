use 5.012004;
use ExtUtils::MakeMaker;
use File::Basename qw(basename dirname);
use File::Find;
use Cwd qw(realpath getcwd);

# Include the full path to .pl files that shouldn't be installed with the distribution
my @skip_exe_files = (
	"test.pl",
);

##### NO CONFIGURATION BELOW THIS POINT #######

# Find an absolute path to myself
my $basepath = realpath(dirname($0));
if(!$basepath) {
	die "Could not find a path to myself, therefore could not install.\n";
}

# Find all other executables
my @exe_files;
find(sub {
	if(/\.pl$/) {
		my $name = substr($File::Find::name, length($basepath) + 1);
		push @exe_files, $name
			unless(grep {$name eq $_} @skip_exe_files);
	}
}, $basepath."/tools");

WriteMakefile(
	NAME              => 'Limesco',
	VERSION_FROM      => 'lib/Limesco.pm',
	PREREQ_PM         => {
		'Carp'           => 1.17,
		'JSON'           => 2.53,
		'Config::Tiny'   => 2.20,
		'DBD::Pg'        => "2.19.3",
	},
	AUTHOR         => 'Sjors Gielen <sjors@limesco.org>',
	EXE_FILES      => \@exe_files,
);
