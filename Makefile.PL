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
		'Business::IBAN' => 0,
		'Carp'           => 1.17,
		'Config::Tiny'   => 2.14,
		'DBD::Pg'        => "2.19.2",
		'DateTime'       => 0,
		'Email::MIME'    => 0,
		'Email::Sender' => 0,
		'IPC::Run'       => 0,
		'JSON'           => 2.53,
		'Term::Menu'     => 0,
		'Term::ReadLine::Perl' => 0,
		'Term::Shell'    => 0,
		'Test::HTTP::Server' => 0,
		'Test::PostgreSQL' => 0.10,
		'Test::XML::Simple' => '1.04',
		'Text::CSV'      => 0,
		'Text::Template' => 0,
		'Try::Tiny'      => 0,
	},
	AUTHOR         => 'Sjors Gielen <sjors@limesco.org>',
	EXE_FILES      => \@exe_files,
);
