#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Text::Template;
use File::Temp qw(tempfile tempdir);
use File::Basename qw(dirname);
use Cwd qw(realpath);
use IPC::Run;
use Try::Tiny;
use Email::MIME;
use Email::Sender::Transport::SMTP;
use Email::Sender::Simple qw(sendmail);
use v5.14; # Unicode string features
use open qw( :encoding(UTF-8) :std);
use utf8;

# get_account
do '../account-change/account-change.pl' unless $INC{'../account-change/account-change.pl'};
do '../sim-change/sim-change.pl' unless $INC{'../sim-change/sim-change.pl'};
do '../directdebit/directdebit.pl' unless $INC{'../directdebit/directdebit.pl'};

Limesco::DirectDebit->import(qw(generate_directdebit_authorization));

=head1 letter-generate.pl

Usage: letter-generate.pl [infra-options] --account <findstring> [--sim <iccid>] [--template <textemplate>] [--out <filename>]

This tool can be used to export PDF or Tex templates, using the files given
using --template (multiple --template options can be given) or the welcome
letters by default. Writes to the filename given by --out, or generates a new
PDF filename by default. If multiple --templates are given, and a single --out
parameter, will use -2, -3, -4... to make sure all written filenames are
unique.

=cut

if(!caller) {
	my @templates;
	my $filename;
	my $account;
	my $sim;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--account") {
			$account = $args->[++$$iref];
		} elsif($arg eq "--sim") {
			$sim = $args->[++$$iref];
		} elsif($arg eq "--template") {
			push @templates, $args->[++$$iref];
		} elsif($arg eq "--out") {
			$filename = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if(!$account) {
		die "The --account option is required\n";
	}

	if(!@templates) {
		@templates = ("welkomstbrief.tex", "sepamachtiging.tex");
	}

	my ($basename, $extension);
	if(defined($filename)) {
		($basename, $extension) = $filename =~ /^(.+)\.(pdf|tex)$/;
		if(!$extension) {
			die "Unrecognised filename, must end with .pdf or .tex\n";
		}
	}

	if($account =~ /^\d+$/) {
		$account = $lim->get_account($account);
	} else {
		$account = $lim->get_account_like($account);
	}

	my @localtime = localtime;
	my $objects = {
		authorization => generate_directdebit_authorization($lim),
		pwd => realpath(dirname($0)),
		account => $account,
		sim => $sim ? get_sim($lim, $sim) : undef,
		date => sprintf("%04d-%02d-%02d", $localtime[5] + 1900, $localtime[4] + 1, $localtime[3]),
	};

	my $instancename;
	if($account) {
		if($account->{'company_name'}) {
			$instancename = $account->{'company_name'};
		} else {
			$instancename = sprintf("%s %s", $account->{'first_name'}, $account->{'last_name'});
		}
		$instancename =~ s/ /_/g;
	}

	for(my $file = 1; $file <= @templates; ++$file) {
		my $template = $templates[$file-1];
		my ($templatebasename) = $template =~ /^(.+)\.tex$/;
		if(!$templatebasename) {
			die "Unrecognised template filename, must end with .tex\n";
		}

		my $save_as;
		if(!$filename) {
			$save_as = sprintf("%s%s.pdf", $templatebasename, $instancename ? "-$instancename" : "");
		} elsif(@templates > 1) {
			$save_as = sprintf("%s-%d.%s", $basename, $file, $extension);
		} else {
			$save_as = $filename;
		}

		if($save_as eq $template) {
			die "Would overwrite template with result. Refusing to continue.\n";
		}

		open my $fh, '>', $save_as or die $!;

		if(!$filename || $extension eq "pdf") {
			binmode $fh;
			print $fh generate_pdf($lim, $objects, $template);
		} else {
			print $fh generate_tex($lim, $objects, $template);
		}

		close $fh;
	}
}

=head2 Methods

=head3 generate_tex($lim, $objects, $template)

Generate a .tex file using given objects and a TeX template filename.

=cut

sub generate_tex {
	my ($lim, $objects, $template) = @_;
	my $t = Text::Template->new(
		# 'vars' adds the 'Variable "$invoice" is not imported' error
		# which I don't know how to work around.
		PREPEND => q{use strict; no strict 'vars'; use locale; use POSIX qw(locale_h); setlocale(LC_ALL, "nl_NL"); },
		TYPE => 'FILE',
		SOURCE => $template,
		DELIMITERS => ['\\beginperl', '\\endperl'],
	);
	no warnings 'once';
	my %methods;
	$methods{'formatPrice'} = sub {
		return $_[0] . $_[1];
	};
	$methods{'formatDate'} = sub {
		if(!defined($_[0])) {
			return "";
		} elsif($_[0] =~ /^(20\d\d)-(\d\d)-(\d\d)$/) {
			my ($year, $month, $day) = ($1, $2, $3);
			$month = qw(null januari februari maart april mei juni juli augustus september oktober november december)[$month];
			return "$day $month $year";
		}
		die "Unknown date format: " . $_[0] . "\n";
	};
	my $tex = $t->fill_in(HASH => {%$objects, %methods, lim => $lim});
	if(!defined($tex)) {
		die "Failed to generate TeX template: $Text::Template::ERROR\n";
	}
	return $tex;
}

=head3 generate_pdf($lim, $objects, $filename)

Generate a .pdf file using given objects and a TeX template filename.

=cut

sub generate_pdf {
	my ($lim, $objects, $filename) = @_;
	my $dir = tempdir(CLEANUP => 1);
	open my $fh, '>', "$dir/file.tex" or die $!;
	my $input_tex = generate_tex($lim, $objects, $filename);
	print $fh $input_tex;
	close $fh;

	my $pdflatex_output = '';
	IPC::Run::run(
		["pdflatex", "-halt-on-error", "-interaction=batchmode", "file.tex"],
		'>', \$pdflatex_output,
		'2>', \$pdflatex_output,
		init => sub {chdir($dir)},
	);
	my $child_status = $?;

	if(-f "$dir/file.log" && open $fh, '<', "$dir/file.log") {
		$pdflatex_output = '';
		$pdflatex_output .= $_ while(<$fh>);
		close $fh;
	}

	if($child_status != 0) {
		die "pdflatex failed to run (exit code $child_status). "
			."Log follows:\n$pdflatex_output\nTeX follows:\n$input_tex\npdflatex failed to run (exit code $child_status)\n";
	}

	if(!-f "$dir/file.pdf") {
		die "pdflatex exited but did not create a pdf file. "
			."Log follows:\n$pdflatex_output\nTeX follows:\n$input_tex\npdflatex exited but did not create a pdf file.\n";
	}

	my $pdffile = '';
	open $fh, '<', "$dir/file.pdf" or die $!;
	binmode $fh;
	while(<$fh>) {
		$pdffile .= $_;
	}
	close $fh;
	return $pdffile;
}

=head3 send_pdf_by_email($lim, $pdf, $filename, $subject, $body, $email_name, $email_address)

Send a document by e-mail to the given "$email_name <$email_address>".

=cut

sub send_pdf_by_email {
	my ($lim, $pdf, $filename, $subject, $body, $email_name, $email_address) = @_;

	my @parts = (
		Email::MIME->create(
			attributes => {
				content_type => "text/plain",
				charset => "UTF-8",
			},
			body => $body,
		),
		Email::MIME->create(
			attributes => {
				filename => $filename,
				content_type => "application/pdf",
				encoding => "base64",
				name => $filename,
				disposition => "attachment",
			},
			body => $pdf,
		),
	);
	my $email_config = $lim->email_config();
	my $emailobj = Email::MIME->create(
		header_str => [
			To => sprintf('"%s" <%s>', $email_name, $email_address),
			From => $email_config->{'from'},
			'Reply-To' => $email_config->{'replyto'},
			Subject => $subject,
		],
		parts => [@parts],
	);
	my $transport = Email::Sender::Transport::SMTP->new({
		host => $email_config->{'smtp_host'},
		port => $email_config->{'smtp_port'},
	});
	sendmail($emailobj, {transport => $transport, to => [$email_address]});
}

1;
