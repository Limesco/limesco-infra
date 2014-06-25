#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Text::Template;
use File::Temp qw(tempdir);
use IPC::Run qw(run);
use v5.14; # Unicode string features
use open qw( :encoding(UTF-8) :std);

# get_account
do '../account-change/account-change.pl' unless UNIVERSAL::can('main', "get_account");

=head1 invoice-export.pl

Usage: invoice-export.pl [infra-options] [--template <textemplate>] --write-invoice <num> --write-to <filename>

This tool can be used to export PDF or Tex templates for an invoice, using
the file given using --template or 'invoice-template.tex' by default. It will
write factoid <num> (e.g. 13C000144) to <filename>, which can be a pdf or tex
file.

=cut

if(!caller) {
	my $template = "invoice-template.tex";
	my $invoice_id;
	my $filename;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--write-invoice") {
			$invoice_id = $args->[++$$iref];
		} elsif($arg eq "--write-to") {
			$filename = $args->[++$$iref];
		} elsif($arg eq "--template") {
			$template = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if(!$invoice_id) {
		die "--write-invoice option is required\n";
	}
	if(!$filename) {
		die "--write-to option is required\n";
	}
	if(! -f $template) {
		die "Failed to open template '$template': it doesn't exist\n";
	}

	my $invoice = get_invoice($lim, $invoice_id);

	my $content;
	my $raw_content = 0;
	if($filename =~ /\.pdf$/) {
		$content = generate_pdf($lim, $invoice, $template);
		$raw_content = 1;
	} elsif($filename =~ /\.tex$/) {
		$content = generate_tex($lim, $invoice, $template);
	} else {
		die "Didn't understand extension for filename, can't generate output format.\n";
	}

	open my $fh, '>', $filename or die $!;
	binmode $fh if $raw_content;
	print $fh $content;
	close $fh;
}

=head2 Methods

=head3 list_invoices($lim, $account_id)

Return a list of all invoices for a given account ID.

=cut

sub list_invoices {
	my ($lim, $account_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM invoice WHERE account_id=? ORDER BY id ASC");
	$sth->execute($account_id);
	my @invoices;
	while(my $invoice = $sth->fetchrow_hashref) {
		push @invoices, $invoice;
	}
	$sth = $dbh->prepare("SELECT * FROM invoice_itemline WHERE invoice_id=?");
	foreach my $invoice (@invoices) {
		$sth->execute($invoice->{'id'});
		$invoice->{'item_lines'} = [];
		while(my $row = $sth->fetchrow_hashref) {
			push @{$invoice->{'item_lines'}}, $row;
		}
	}
	return @invoices;
}

=head3 get_invoice($lim, $invoice_id)

Retrieve information about invoice $invoice_id.

=cut

sub get_invoice {
	my ($lim, $invoice_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM invoice WHERE id=?");
	$sth->execute($invoice_id);
	my $invoice = $sth->fetchrow_hashref() or die "Invoice doesn't exist: '$invoice_id'\n";
	$sth = $dbh->prepare("SELECT * FROM invoice_itemline WHERE invoice_id=?");
	$sth->execute($invoice_id);
	$invoice->{'item_lines'} = [];
	while(my $row = $sth->fetchrow_hashref()) {
		push @{$invoice->{'item_lines'}}, $row;
	}
	return $invoice;
}

=head3 generate_tex($lim, $invoice, $template)

Generate a .tex file using invoice information and a TeX template filename.

=cut

sub generate_tex {
	my ($lim, $invoice, $template) = @_;
	my $t = Text::Template->new(
		# 'vars' adds the 'Variable "$invoice" is not imported' error
		# which I don't know how to work around.
		PREPEND => q{use strict; no strict 'vars'; use locale; use POSIX qw(locale_h); setlocale(LC_ALL, "nl_NL"); },
		TYPE => 'FILE',
		SOURCE => $template,
		DELIMITERS => ['\\beginperl', '\\endperl'],
	);
	no warnings 'once';
	package T {};
	$T::invoice = $invoice;
	$T::lim = $lim;
	$T::account = get_account($lim, $invoice->{'account_id'});
	sub T::formatPrice {
		return $_[0] . $_[1];
	}
	sub T::formatDate {
		if($_[0] =~ /^(20\d\d)-(\d\d)-(\d\d)$/) {
			my ($year, $month, $day) = ($1, $2, $3);
			$month = qw(null januari februari maart april mei juni juli augustus september oktober november december)[$month];
			return "$day $month $year";
		}
		die "Unknown date format: " . $_[0] . "\n";
	}
	my $tex = $t->fill_in(PACKAGE => 'T');
	if(!defined($tex)) {
		die "Failed to generate TeX template: $Text::Template::ERROR\n";
	}
	return $tex;
}

=head3 generate_pdf($lim, $invoice, $filename)

Generate a .pdf file using invoice information and a TeX template filename.

=cut

sub generate_pdf {
	my ($lim, $invoice, $filename) = @_;
	my $dir = tempdir(CLEANUP => 1);
	open my $fh, '>', "$dir/file.tex" or die $!;
	my $input_tex = generate_tex($lim, $invoice, $filename);
	print $fh $input_tex;
	close $fh;

	my $pdflatex_output = '';
	run(
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

1;
