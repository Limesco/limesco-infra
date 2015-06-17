#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Text::Template;
use File::Temp qw(tempfile tempdir);
use IPC::Run qw(run);
use Try::Tiny;
use Email::MIME;
use Email::Sender::Transport::SMTP;
use Email::Sender::Simple qw(sendmail);
use v5.14; # Unicode string features
use open qw( :encoding(UTF-8) :std);
use locale;
use POSIX qw(locale_h);

# get_account
do '../account-change/account-change.pl' unless UNIVERSAL::can('main', "get_account");
do '../letter-generate/letter-generate.pl' unless UNIVERSAL::can('main', "generate_tex");
do '../balance/balance.pl';

=head1 invoice-export.pl

Usage:
  1. invoice-export.pl [infra-options] --invoice <num> [--template <textemplate>] --write-to <filename>
  2. invoice-export.pl [infra-options] --invoice <num> [--template <textemplate>] --open
  3. invoice-export.pl [infra-options] --invoice <num> [--template <textemplate>] --email-template <txttemplate> { --email-owner | --email-to <address> }

This tool can be used to export PDF or Tex templates for an invoice, using the
file given using --template or 'invoice-template.tex' by default.

There are various export options:

1. It can write the invoice to <filename>, which can be a pdf or tex file.
2. It can write the invoice to a temporary location and open it.
3. It can send the invoice PDF by e-mail using --email-to or --email-owner,
which will send it respectively to a given address or the account of the
invoice.

Multiple of these options can be given simultaneously, such as in the following
command-line:

invoice-export.pl --invoice 14C000001 --write-to 14C000001.pdf --email-to sjors@limesco.nl

=cut

if(!caller) {
	my $template = "invoice-template.tex";
	my $invoice_id;
	my $filename;
	my $email_address;
	my $email_owner;
	my $email_template = "email-template.txt";
	my $open;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--invoice") {
			$invoice_id = $args->[++$$iref];
		} elsif($arg eq "--write-to") {
			$filename = $args->[++$$iref];
		} elsif($arg eq "--template") {
			$template = $args->[++$$iref];
		} elsif($arg eq "--email-template") {
			$email_template = $args->[++$$iref];
		} elsif($arg eq "--email-to") {
			$email_address = $args->[++$$iref];
		} elsif($arg eq "--email-owner") {
			$email_owner = 1;
		} elsif($arg eq "--open") {
			$open = 1;
		} else {
			return 0;
		}
	});

	if(!$invoice_id) {
		die "--invoice option is required\n";
	}
	if(!$filename && !$email_address && !$email_owner && !$open) {
		die "One of --write-to, --email-to, --email-owner or --open options is required\n";
	}
	if(! -f $template) {
		die "Failed to open template '$template': it doesn't exist\n";
	}

	my $invoice = get_invoice($lim, $invoice_id);
	my $pdf_content;

	# Let's set LC_NUMERIC to the desired format, since Perl defaults to LC_NUMERIC="C" :-(
	if ($lim->{'config'}->{'locale'}->{'lc_numeric'}) {
		setlocale(LC_NUMERIC, $lim->{'config'}->{'locale'}->{'lc_numeric'});
	} else {
		warn "Lo, and behold! Your LC_NUMERIC might not be in your desired format: LC_NUMERIC=".setlocale(LC_NUMERIC)."\n";
	}

	if($filename) {
		my $content;
		my $raw_content = 0;
		if($filename =~ /\.pdf$/) {
			$pdf_content = $content = generate_invoice_pdf($lim, $invoice, $template);
			$raw_content = 1;
		} elsif($filename =~ /\.tex$/) {
			$content = generate_invoice_tex($lim, $invoice, $template);
		} else {
			die "Didn't understand extension for filename, can't generate output format.\n";
		}

		open my $fh, '>', $filename or die $!;
		binmode $fh if $raw_content;
		print $fh $content;
		close $fh;
	}

	if($email_address || $email_owner) {
		if(! -f $email_template) {
			die "Failed to open template '$email_template': it doesn't exist\n";
		}

		try {
			send_invoice_by_email($lim, $invoice, $template, $email_template, $email_address) if $email_address;
		} catch {
			warn "Failed to send invoice $invoice to address $email_address: $_\n";
		};
		try {
			send_invoice_by_email($lim, $invoice, $template, $email_template, undef) if $email_owner;
		} catch {
			warn "Failed to send invoice $invoice to invoice account: $_\n";
		};
	}

	if($open) {
		$pdf_content ||= generate_invoice_pdf($lim, $invoice, $template);
		my ($fh, $filename) = tempfile();
		binmode $fh;
		print $fh $pdf_content;
		close $fh;
		if(-x "/usr/bin/xdg-open") {
			system("/usr/bin/xdg-open", $filename);
		} elsif(-x "/usr/bin/open") {
			system("/usr/bin/open", $filename);
		} else {
			warn "Don't know how to open filenames on this operating system.";
		}
	}
}

=head2 Methods

=head3 list_invoices($lim, [$account_id])

Return a list of all invoices for a given account ID. If no account ID is
given, return a list of all invoices.

=cut

sub list_invoices {
	my ($lim, $account_id) = @_;
	my $dbh = $lim->get_database_handle();
	my $where_clause = $account_id ? "WHERE account_id=?" : "";
	my $sth = $dbh->prepare("SELECT * FROM invoice $where_clause ORDER BY id ASC");
	$sth->execute($account_id ? ($account_id) : ());
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

=head3 get_invoice($lim | $dbh, $invoice_id)

Retrieve information about invoice $invoice_id.

=cut

sub get_invoice {
	my ($lim, $invoice_id) = @_;
	my $dbh_is_mine = ref($lim) eq "Limesco";
	my $dbh = $dbh_is_mine ? $lim->get_database_handle() : $lim;
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

=head3 generate_invoice_tex($lim, $invoice, $template)

Generate a .tex file using invoice information and a TeX template filename.

=cut

sub generate_invoice_tex {
	my ($lim, $invoice, $template) = @_;
	my $account = get_account($lim, $invoice->{'account_id'}, $invoice->{'date'});
	my @p_and_i = get_payments_and_invoices($lim, $account->{'id'}, $invoice->{'date'});
	my $balance = @p_and_i ? (pop @p_and_i)->{'balance'} : 0;
	my $objects = {
		invoice => $invoice,
		account => $account,
		balance => $balance,
	};
	return generate_tex($lim, $objects, $template);
}

=head3 generate_invoice_pdf($lim, $invoice, $filename)

Generate a .pdf file using invoice information and a TeX template filename.

=cut

sub generate_invoice_pdf {
	my ($lim, $invoice, $filename) = @_;
	my $account = get_account($lim, $invoice->{'account_id'}, $invoice->{'date'});
	my @p_and_i = get_payments_and_invoices($lim, $account->{'id'}, $invoice->{'date'});
	my $balance = @p_and_i ? (pop @p_and_i)->{'balance'} : 0;
	my $objects = {
		invoice => $invoice,
		account => $account,
		balance => $balance,
	};
	return generate_pdf($lim, $objects, $filename);
}

=head3 send_invoice_by_email($lim, $invoice, $invoice_template, $email_template, [$email])

Send this invoice by e-mail. If $email is undefined or not given, the e-mail is
sent to the invoice account.

=cut

sub send_invoice_by_email {
	my ($lim, $invoice, $invoice_template, $email_template, $email) = @_;
	my $dbh = $lim->get_database_handle();
	my $account;
	try {
		$account = $lim->get_account($invoice->{'account_id'});
	} catch {
		die sprintf("Invoice %s belongs to a nonexistant account", $invoice->{'id'});
	};

	if(!$email) {
		$email = $account->{'email'};
	}
	if(!$email) {
		die sprintf("No e-mail address to send invoice %s to", $invoice->{'id'});
	}

	print "Sending to $email\n";

	my $pdf = generate_pdf($lim, {invoice => $invoice, account => $account}, $invoice_template);

	my $accountname = $account->{'company_name'};
	if(!$accountname) {
		$accountname = $account->{'first_name'} . " " . $account->{'last_name'};
	}
	my $fn_accountname = $accountname;
	$fn_accountname =~ s/\s/_/g;
	my $filename = sprintf "%s-%s.pdf", $fn_accountname, $invoice->{'id'};

	# This is a dirty hack to generate a Bitcoin URL. Because of an OpenSSL interface
	# difference, tokens encoded using Perl could not be decoded using the PHP payment end.
	# Once the other end is rewritten using Perl, we can fix this properly. For now, just
	# encode using PHP too. :-(
	my $id = $invoice->{'id'};
	my $rounded_with_taxes = $invoice->{'rounded_with_taxes'};
	my $company = $account->{'company_name'} || "";
	my $fullname = $account->{'first_name'} . " " . $account->{'last_name'};
	my $btc_url = `php token_generate.php "$id" "$rounded_with_taxes" "$company" "$fullname"`;
	1 while chomp $btc_url;
	$btc_url = "https://api.limesco.nl/betaling/?token=$btc_url";

	# Open a handle ourselves, so it is in utf8 mode
	open my $tfh, '<', $email_template or die $!;
	my $t = Text::Template->new(
		PREPEND => q{use strict; no strict 'vars'; use locale; use POSIX qw(locale_h); setlocale(LC_ALL, "nl_NL"); },
		TYPE => 'FILEHANDLE',
		SOURCE => $tfh,
	);
	close $tfh;

	# Denote the invoice amount with a comma, should probably be fixed with correct locale
	$invoice->{'rounded_with_taxes'} =~ s/\./,/;

	my $body = $t->fill_in(HASH => {account => $account, invoice => $invoice, payment_url => $btc_url});
	if(!defined($body)) {
		die "Failed to generate e-mail body: $Text::Template::ERROR\n";
	}

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
			To => sprintf('"%s" <%s>', $accountname, $email),
			From => $email_config->{'from'},
			'Reply-To' => $email_config->{'replyto'},
			Subject => 'Limesco factuur ' . $id,
		],
		parts => [@parts],
	);
	my $transport = Email::Sender::Transport::SMTP->new({
		host => $email_config->{'smtp_host'},
		port => $email_config->{'smtp_port'},
	});
	sendmail($emailobj, {transport => $transport, to => [$email, $email_config->{'blind_cc'}]});
}

1;
