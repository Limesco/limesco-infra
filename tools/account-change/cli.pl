#!/usr/bin/perl
use strict;
use warnings;
use lib '../../lib';
use lib '../lib';
use lib 'lib';
use Limesco;

do 'account-change.pl' or die $!;
do '../sim-change/sim-change.pl' or die $!;
do '../invoice-export/invoice-export.pl' or die $!;
do '../directdebit/directdebit.pl' unless UNIVERSAL::can("main", "generate_directdebit_authorization") or die $!;
do '../directdebit/bic-convert.pl' or die $!;
do '../balance/balance.pl' or die $!;

my $lim = Limesco->new_from_args(\@ARGV);
my $shell = LimescoShell->new($lim);
$shell->cmdloop;

package LimescoShell;
use strict;
use warnings;
use Encode;
use Term::Menu;
use Data::Dumper;
use Carp;
use Term::Shell;
use Try::Tiny;
use File::Basename qw(dirname);
use Cwd qw(realpath);
use base qw(Term::Shell);

sub today {
	my @time = localtime(time);
	return sprintf("%04d-%02d-%02d", $time[5] + 1900, $time[4] + 1, $time[3]);
}

sub numeric_convert_if_numeric {
	my ($value) = @_;
	if($value =~ /^(\d+)[,\.](\d+)$/) {
		return "$1.$2" + 0;
	}
	return $value;
}

# ask_question($question, sub ...) = ask question, feed answers through subroutine
# subroutine can return undef to try again, the given value to accept it, or
# another value that will be returned by ask_question()
# ask_question($question) = ask question, return any answer, including empty
sub ask_question {
	my ($question, $validator) = @_;
	while(1) {
		print "$question ";
		my $answer = <STDIN>;
		$answer =~ s/^\s+//;
		$answer =~ s/\s+$//;
		if($validator) {
			my $validated = $validator->($answer);
			return $validated if defined $validated;
		} else {
			return $answer;
		}
	}
}

# ask_date_or_today($question) = ask question, give today's date as suggestion;
# return the date the user gave or today's date if the user answered nothing
sub ask_date_or_today {
	my ($question) = @_;
	my $today = today();
	return ask_question("$question [$today]", sub {
		if(length $_[0]) {
			if($_[0] =~ /\d{4}-\d\d-\d\d/) {
				return $_[0];
			} else {
				warn "That's not a valid date!\n";
				return;
			}
		} else {
			return $today;
		}
	});
}

sub format_period {
	my ($period) = @_;
	my ($lower, $upper) = $period =~ /^(?:\[|\()([0-9\-]*),\s*([0-9\-]*)\)$/;
	my $from = $lower ? sprintf("from %s",  $lower) : "";
	my $to   = $upper ? sprintf("until %s", $upper) : "";
	if(!$lower && !$upper) {
		$from = "always";
	}
	my $space = $from && $to ? " " : "";
	return $from . $space . $to;
}

sub init {
	my ($self) = @_;
	$self->{lim} = $self->{API}{args}[0];
	if(!$self->{lim} || ref($self->{lim}) ne "Limesco") {
		croak "Failed to create LimescoShell: Limesco object must be given as the first parameter";
	}
}

sub prompt_str {
	my ($self) = @_;
	my $prompt = "liminfra";
	if($self->{account}) {
		$prompt .= sprintf(" account(%d)", $self->{account}{id});
		if($self->{sim}) {
			$prompt .= sprintf(" sim(%s)", substr($self->{sim}{iccid}, -6));
		}
	}
	return "$prompt> ";
}

sub run_account {
	my ($self, $search, $date) = @_;
	if(@_ < 2 || !$search) {
		warn help_account();
		return;
	}
	$date ||= 'today';

	if($self->{queued_changes}) {
		warn "Cannot select account when changes are queued. Use 'commit' or 'rollback' first.\n";
		return;
	}

	delete $self->{account};
	delete $self->{sim};

	try {
		if($search =~ /^\d+$/) {
			$self->{account} = $self->{lim}->get_account($search, $date);
		} else {
			$self->{account} = $self->{lim}->get_account_like($search, $date);
		}
	} catch {
		warn $_;
	};
}

sub help_account {
	return <<HELP;
account <id|string> [date]

Selects an account. You can give an ID to immediately select that account, or
give a single string to search through accounts. If multiple accounts match the
search string, a list is given and the current account is deselected. The
optional parameter date can be used to search from a specific date onwards.
HELP
}

sub smry_account {
	return "selects an account";
}

sub comp_sim {
	my ($self, $word, $line, $start) = @_;
	if(!$self->{'account'}) {
		return;
	}

	try {
		my $dbh = $self->{'lim'}->get_database_handle();
		my $sth = $dbh->prepare("SELECT DISTINCT iccid FROM sim WHERE owner_account_id=?");
		$sth->execute($self->{'account'}{'id'});
		my @iccids;
		while(my $row = $sth->fetchrow_arrayref()) {
			push @iccids, $row->[0];
		}
		return grep { substr($_, 0, length($word)) eq $word } @iccids;
	} catch {
		warn "Failed to tab complete: $_\n";
	};
}

sub run_sim {
	my ($self, $iccid) = @_;
	if(@_ != 1 && @_ != 2) {
		warn help_sim();
		return;
	}

	if($self->{queued_changes}) {
		warn "Cannot select SIM when changes are queued. Use 'commit' or 'rollback' first.\n";
		return;
	}

	delete $self->{'sim'};

	if(!$self->{'account'}) {
		warn "An account must be selected to use the 'sim' command.\n";
		return;
	}

	my $accountid = $self->{'account'}{'id'};

	if($iccid) {
		try {
			my $sim = ::get_sim($self->{lim}, $iccid);
			if($sim->{'owner_account_id'} != $accountid) {
				die "That SIM is not owned by the selected account";
			}
			$self->{'sim'} = $sim;
		} catch {
			warn $_;
		};
		return;
	}

	# TODO: filter by account ID in list_sims
	my @sims = grep { $_->{'owner_account_id'} && $_->{'owner_account_id'} eq $accountid } ::list_sims($self->{lim});
	foreach(@sims) {
		my @phonenumbers = ::list_phonenumbers($lim, $_->{'iccid'});
		my $phonenumber = "(no phone numbers)" if @phonenumbers == 0;
		$phonenumber = $phonenumbers[0]{'phonenumber'} if @phonenumbers == 1;
		$phonenumber = "" if @phonenumbers > 1;

		printf("%s %s %s %s\n", $_->{'iccid'}, $_->{'state'}, $_->{'period'}, $phonenumber);
		if(@phonenumbers > 1) {
			printf("  %s\n", $_->{'phonenumber'}) foreach(@phonenumbers);
		}
	}
	if(@sims == 1) {
		$self->{'sim'} = $sims[0];
	} elsif(@sims == 0) {
		warn "No SIMs in this account.\n";
	}
}

sub help_sim {
	return <<HELP;
sim [iccid]

Selects a SIM card. If no parameters are given, lists all SIM cards currently
active in the account and, if only one SIM card is active, selects it.
Optionally, a parameter can be given which is the ICCID of the SIM card.
HELP
}

sub smry_sim {
	return "selects a SIM";
}

sub run_back {
	my ($self) = @_;

	if($self->{queued_changes}) {
		warn "Cannot go back when changes are queued. Use 'commit' or 'rollback' first.\n";
		return;
	}

	if($self->{account}) {
		if($self->{sim}) {
			delete $self->{sim};
		} else {
			delete $self->{account};
		}
	}
}

sub help_back {
	return <<HELP;
back

Go back to the previous level. If a SIM is selected, go back to account. If an
account is selected, go back to main menu.
HELP
}

sub smry_back {
	return "go back to the previous level";
}

sub cli_create_account {
	my ($self) = @_;
	local $SIG{INT} = sub { die "Interrupted\n" };
	my $starting_date = ask_date_or_today("Starting date?");

	my $account = {};
	$account->{'first_name'} = ask_question("First name?");
	$account->{'last_name'} = ask_question("Last name?");
	$account->{'company_name'} = ask_question("Company name?") || undef;
	$account->{'street_address'} = ask_question("Street address?");
	$account->{'postal_code'} = ask_question("Postal code?");
	$account->{'city'} = ask_question("City?");
	$account->{'email'} = ask_question("E-mail address?");
	$account->{'contribution'} = numeric_convert_if_numeric(
		ask_question("Contribution amount (ex VAT)?"));
	return ::create_account($self->{'lim'}, $account, $starting_date);
}

sub cli_create_sim {
	my ($self) = @_;
	local $SIG{INT} = sub { die "Interrupted\n" };
	my $starting_date = ask_date_or_today("Starting date?");

	my $stock_sim;
	my $iccid = ask_question("ICCID?", sub {
		my $iccid = $_[0];
		try {
			$stock_sim = ::get_sim($self->{'lim'}, $iccid, $starting_date);
			if($stock_sim->{'state'} ne "STOCK") {
				warn "That ICCID is already taken: it belongs to a non-STOCK SIM at $starting_date.\n";
				undef $stock_sim;
				return undef;
			}
			return $iccid;
		} catch {
			warn "Couldn't retrieve that stock SIM: $_";
			warn "Assuming we will create it as a new SIM\n";
			return $iccid;
		};
	});

	my $sim = {iccid => $iccid};
	if($stock_sim) {
		$sim->{'puk'} = $stock_sim->{'puk'};
	} else {
		$sim->{'puk'} = ask_question("PUK?");
	}
	$sim->{'owner_account_id'} = $self->{'account'}{'id'};
	# all of our SIMs have APN_NODATA currently
	$sim->{'data_type'} = "APN_NODATA";
	$sim->{'state'} = "ACTIVATED";
	$sim->{'exempt_from_cost_contribution'} = 0;
	$sim->{'call_connectivity_type'} = ask_question("DIY or OOTB?", sub {
		if(uc($_[0]) eq "DIY") {
			return "DIY";
		} elsif(uc($_[0]) eq "OOTB") {
			return "OOTB";
		} else {
			warn "Invalid response: make it 'DIY' or 'OOTB'.\n";
			return;
		}
	});
	if($stock_sim) {
		delete $sim->{'iccid'};
		return ::update_sim($self->{'lim'}, $iccid, $sim, $starting_date);
	} else {
		return ::create_sim($self->{'lim'}, $sim, $starting_date);
	}
}

sub run_create {
	my ($self) = @_;

	if($self->{queued_changes}) {
		warn "Cannot create anything when changes are queued. Use 'commit' or 'rollback' first.\n";
		return;
	}

	if(!$self->{account}) {
		try {
			$self->{account} = cli_create_account($self);
		} catch {
			warn $_;
		};
	} elsif(!$self->{sim}) {
		try {
			$self->{sim} = cli_create_sim($self);
		} catch {
			warn $_;
		};
	} else {
		warn "Cannot create anything in SIM mode. Use 'back' to go back.\n";
	}
}

sub help_create {
	return <<HELP;
create

In main mode, create an account. In account mode, create a SIM. Otherwise, give
an error.
HELP
}

sub smry_create {
	return "create an account/SIM";
}

sub cli_account_oneliner {
	my ($account) = @_;
	if($account->{'company_name'}) {
		my $company = $account->{'company_name'};
		delete $account->{'company_name'};
		return $company . " (" . cli_account_oneliner($account) . ")";
	} else {
		return $account->{'first_name'} . " " . $account->{'last_name'};
	}
}

sub run_info {
	my ($self, $what) = @_;
	$what = lc($what) if $what;
	if($what && $what eq "sim" && !$self->{sim}) {
		warn "Can't give info about SIM: no SIM selected (use 'sim' first)\n";
		return;
	} elsif($what && $what eq "account" && !$self->{account}) {
		warn "Can't give info about account: no account selected (use 'account' first)\n";
		return;
	} elsif($what && $what eq "directdebit") {
		return run_directdebit("info");
	} elsif(!$what) {
		$what = "account" if($self->{'account'});
		$what = "sim" if($self->{'sim'});
		if(!$what) {
			print "No account or SIM selected, listing all accounts.\n";
			my @account = sort {$a->{'first_name'} cmp $b->{'first_name'}} ::list_accounts($lim);
			foreach my $acc (@account) {
				print "(" . $acc->{'id'} . ")\t" . $acc->{'first_name'} . " " . $acc->{'last_name'};
				print "\n\t(" . $acc->{'company_name'} . ")" if ($acc->{'company_name'});
				print "\n";
			}
			print "Total: " . scalar @account . "\n";
			return;
		}
	}

	if($what eq "sim") {
		my $s = $self->{sim};
		print "ICCID: " . $s->{'iccid'} . "\n";
		print "Current information validity period: " . $s->{'period'} . "\n";
		print "SIM State: " . $s->{'state'} . "\n";
		print "PUK: " . $s->{'puk'} . "\n";
		die if $s->{'owner_account_id'} != $self->{'account'}{'id'};
		print "Owner: " . cli_account_oneliner($self->{'account'}) . "\n";
		print "Data type: " . $s->{'data_type'} . "\n";
		print "Exempt from cost contribution: " . $s->{'exempt_from_cost_contribution'} . "\n";
		print "Call connectivity type: " . $s->{'call_connectivity_type'} . "\n";
		print "\n";
		my $activation = $s->{'activation_invoice_id'};
		if($activation) {
			print "Activation invoiced at: $activation\n";
		} else {
			print "Activation not invoiced yet\n";
		}
		my $last_invoice = $s->{'last_monthly_fees_invoice_id'};
		if($last_invoice) {
			print "Last invoiced at: $last_invoice (for " . $s->{'last_monthly_fees_month'} . ")\n";
		}
	} elsif($what eq "account") {
		my $a = $self->{'account'};
		print "Account ID: " . $a->{'id'} . "\n";
		print "Current information validity period: " . $a->{'period'} . "\n";
		print "Company name: " . ($a->{'company_name'} || "(none)") . "\n";
		print "First name: " . $a->{'first_name'} . "\n";
		print "Last name: " . $a->{'last_name'} . "\n";
		print "E-mail address: " . $a->{'email'} . "\n";
		print "Contribution: " . $a->{'contribution'} . "\n";
		print "Address:\n";
		print "  " . $a->{'street_address'} . "\n";
		print "  " . $a->{'postal_code'} . " " . $a->{'city'} . "\n";
	}
}

sub help_info {
	return <<HELP;
info [sim|account|directdebit]

Give information about the currently selected SIM or account or print all
active accounts when no argument is given.
HELP
}

sub smry_info {
	return "give information about currently selected object";
}

sub run_ls {
	return run_info(@_);
}

sub help_ls {
	return help_info();
}

sub smry_ls {
	return smry_info();
}

sub comp_invoice {
	my ($self, $word, $line, $start) = @_;
	try {
		my $accountid = $self->{'account'}{'id'} if($self->{'account'});
		my @invoices = map { $_->{'id'} } ::list_invoices($lim, $accountid);
		return grep { substr($_, 0, length($word)) eq $word } @invoices;
	} catch {
		warn "Failed to tab complete: $_\n";
	};
}

sub run_invoice {
	my ($self, $invoice_id, $email) = @_;
	if(!$invoice_id && !$self->{'account'}) {
		warn "Select an account to list its invoices, or give an invoice ID to dump its information.\n";
		return;
	} elsif(!$invoice_id) {
		my @invoices = ::list_invoices($lim, $self->{'account'}{'id'});
		if(!@invoices) {
			print "This account was never invoiced.\n";
			return;
		}
		foreach my $invoice (@invoices) {
			printf("%s (%s): %.02f\n", $invoice->{'id'}, $invoice->{'date'}, $invoice->{'rounded_with_taxes'});
		}
		return;
	} else {
		my $invoice;
		try {
			$invoice = ::get_invoice($lim, $invoice_id);
			die if(!$invoice);
		} catch {
			warn "Could not retrieve this invoice: $_\n";
		};
		return if(!$invoice);

		if ($email) {
			my $filename = "$invoice_id.pdf";
			try {
				# TODO: configuration option for invoice TeX template
				my $pdf = ::generate_invoice_pdf($lim, $invoice, '../invoice-export/invoice-template.tex');
				::send_pdf_by_email($lim, $pdf, $filename, "Liminfra invoice $invoice_id",
					"Attached to this e-mail is your requested invoice: $invoice_id.",
					"Liminfra user", $email);
			} catch {
				warn "Failed to generate and send PDF for invoice $invoice_id: $_\n";
			};
			return;
		}

		if($self->{'account'} && $invoice->{'account_id'} != $self->{'account'}{'id'}) {
			warn "Refusing to list this invoice: it does not belong to the selected account.\n";
			warn "Deselect the current account using 'back' to list this invoice.\n";
			return;
		}
		printf("Invoice ID: %s\n", $invoice->{'id'});
		my $account_description;
		if($self->{'account'}) {
			$account_description = cli_account_oneliner($self->{'account'});
		} else {
			try {
				$account_description = cli_account_oneliner(::get_account($lim, $invoice->{'account_id'}));
			} catch {
				$account_description = "(deleted account)";
			};
		}
		printf("Account: %s\n", $account_description);
		printf("Invoice date: %s\n", $invoice->{'date'});
		printf("Invoice creation time: %s\n", $invoice->{'creation_time'});
		printf("----------------------------------------------------------\n");
		foreach my $line (@{$invoice->{'item_lines'}}) {
			next if($line->{'type'} eq "TAX");
			# Note: Description may be multi-line
			printf("%s\n", $line->{'description'});
			if($line->{'type'} eq "NORMAL") {
				printf("    %d * %.4f = %.2f\n", $line->{'item_count'}, $line->{'item_price'}, $line->{'rounded_total'});
			} elsif($line->{'type'} eq "DURATION") {
				printf("    %d calls * %.4f ppc\n", $line->{'number_of_calls'}, $line->{'price_per_call'});
				printf("    %d minutes * %.4f ppm\n", $line->{'number_of_seconds'} / 60, $line->{'price_per_minute'});
				printf("    ----------------------+ = %.2f\n", $line->{'rounded_total'});
			} else {
				die "Unknown itemline type\n";
			}
		}
		printf("----------------------------------------------------------\n");
		printf("Total without taxes: %.2f\n", $invoice->{'rounded_without_taxes'});
		foreach my $line (@{$invoice->{'item_lines'}}) {
			next if($line->{'type'} ne "TAX");
			printf("%.2f%% taxes over %.2f: %.2f\n", $line->{'taxrate'} * 100, $line->{'base_amount'}, $line->{'rounded_total'});
		}
		printf("Total with taxes: %.2f\n", $invoice->{'rounded_with_taxes'});
	}
}

sub help_invoice {
	return <<HELP;
invoice [invoiceid] [email address]

List information about given invoice and, optionally, send a
PDF of that invoice to the specified e-mail address. If no
invoice name is given, give a list of invoices for the
selected account if there is one.
HELP
}

sub smry_invoice {
	return "dump an invoice or a list of invoices";
}

sub run_speakup_account {
	my ($self, $command, $name, $date) = @_;

	if(!$self->{'account'}) {
		warn "Can only use the 'speakup_account' command when an account is selected.\n";
		return;
	}

	if(!$command || $command eq "list") {
		my $dbh = $self->{lim}->get_database_handle();
		my $sth = $dbh->prepare("SELECT name, period FROM speakup_account WHERE account_id=? ORDER BY period");
		$sth->execute($self->{'account'}{'id'});
		while(my $row = $sth->fetchrow_hashref()) {
			my $spaces = ' ' x (14 - length($row->{'name'}));
			printf("  %s%s (%s)\n", $row->{'name'}, $spaces, format_period($row->{'period'}));
		}
	} elsif($command eq "link" || $command eq "unlink") {
		if($name && $date) {
			# use the given ones
		} elsif(!$name && !$date) {
			my $add = $command eq "link" ? "link" : "unlink";
			$name = ask_question("What speakup account name to $add?");
			my $activated = $command eq "link" ? "linked" : "unlinked";
			$date = ask_date_or_today("On what date will the speakup account be $activated?");
		} else {
			warn help_speakup_account();
			return;
		}

		try {
			if($command eq "link") {
				::link_speakup_account($lim, $name, $self->{'account'}{'id'}, $date);
			} else {
				::unlink_speakup_account($lim, $name, $date);
			}
		} catch {
			warn $_;
		};
	} else {
		warn help_speakup_account();
		return;
	}
}

sub help_speakup_account {
	return <<HELP;
speakup_account [list]
speakup_account link [<name> <date>]
speakup_account unlink [<name> <date>]

List speakup accounts, link one to an account, or unlink one from an account.
If no parameters are given to link or unlink, the command asks interactively.
'today' is allowed as the date.
HELP
}

sub smry_speakup_account {
	return "view, link or unlink speakup accounts to liminfra accounts";
}


sub run_phonenumber {
	my ($self, $command, $number, $date) = @_;

	if(!$self->{'sim'}) {
		warn "Can only use the 'phonenumber' command when a SIM is selected.\n";
		return;
	}

	if(!$command || $command eq "list") {
		my $dbh = $self->{lim}->get_database_handle();
		my $sth = $dbh->prepare("SELECT phonenumber, period FROM phonenumber WHERE sim_iccid=? ORDER BY period");
		$sth->execute($self->{'sim'}{'iccid'});
		while(my $row = $sth->fetchrow_hashref()) {
			printf("  %s    (%s)\n", $row->{'phonenumber'}, format_period($row->{'period'}));
		}
	} elsif($command eq "add" || $command eq "remove") {
		if($number && $date) {
			# use the given ones
		} elsif(!$number && !$date) {
			my $add = $command eq "add" ? "add" : "remove";
			$number = ask_question("What phone number to $add?");
			my $activated = $command eq "add" ? "activated" : "deactivated";
			$date = ask_date_or_today("On what date will the phone number be $activated?");
		} else {
			warn help_phonenumber();
			return;
		}

		try {
			if($command eq "add") {
				::create_phonenumber($lim, $number, $self->{'sim'}{'iccid'}, $date);
			} else {
				::delete_phonenumber($lim, $number, $date);
			}
		} catch {
			warn $_;
		};
	} else {
		warn help_phonenumber();
		return;
	}
}

sub help_phonenumber {
	return <<HELP;
phonenumber [list]
phonenumber add [<number> <date>]
phonenumber remove [<number> <date>]

List phone numbers, add one to a SIM, or remove one from a SIM. If no
parameters are given to add or remove, the command asks interactively.
'today' is allowed as the date.
HELP
}

sub smry_phonenumber {
	return "view, add or remove phone numbers from SIMs";
}

sub cli_add_directdebit_authorization {
	my ($self) = @_;

	if(!$self->{'account'}) {
		die "Must have an account selected to authorize it for directdebit\n";
	}

	local $SIG{INT} = sub { die "Interrupted\n" };
	my $authorization_id = ask_question("Authorization ID?", sub {
		if(length($_[0]) != 24) {
			warn "Invalid authorization ID.\n";
			undef $_[0];
		}
		return $_[0];
	});
	my $bank_account_name = ask_question("Name of bank account holder?");
	my $iban = ask_question("IBAN?", sub {
		if (!::iban_check($_[0])) {
			undef $_[0];
		}
		return $_[0];
	});
	my $proposedbic = ::iban_to_bic($iban);
	print ">>> BIC suggested: [".$proposedbic."].\n";
	my $confirmbic = ask_question("Enter 'yes' to accept suggestion or fill in BIC manually:");
	my $bic = ($confirmbic eq lc("yes")) ? $proposedbic : $confirmbic;
	print ">>> BIC entered: ".$bic."\n";
	my $date = ask_question("Signature date (YYYY-MM-DD)?");
	return ::add_directdebit_account($lim, $self->{'account'}{'id'},
		$authorization_id, $bank_account_name, $iban, $bic, $date);
}

sub cli_delete_directdebit_authorization {
	my ($self) = @_;

	if(!$self->{'account'}) {
		die "Must have an account selected to deauthorize it for directdebit\n";
	}

	local $SIG{INT} = sub { die "Interrupted\n" };

	my @authorizations = ::get_active_directdebit_authorizations($lim, $self->{'account'}{'id'});
	my $authorization_id;
	if(@authorizations == 0) {
		die "No authorizations to remove.\n";
	} elsif(@authorizations == 1) {
		$authorization_id = $authorizations[0]{'authorization_id'};
	} else {
		my @options;
		my $i = 0;
		foreach(@authorizations) {
			push @options, $_->{'authorization_id'}, [$_->{'iban'} . " (". $_->{'authorization_id'} . ")", ++$i];
		}
		$authorization_id = Term::Menu->menu(@options);
	}

	ask_question("Sure to delete authorization ID $authorization_id? [yN]", sub {
		if(!$_[0] || lc($_[0]) ne "y") {
			die "Cancelled.\n";
		}
		return 1;
	});

	my $enddate = ask_question("Directdebit end date?");
	::delete_directdebit_account($self->{'lim'}, $authorization_id, $enddate);
}

sub run_directdebit {
	my ($self, $command) = @_;

	if(!$command || $command eq "info") {
		if(!$self->{'account'}) {
			warn "Must have an account selected to see its directdebit info\n";
			return;
		}
		my @authorizations = ::get_all_directdebit_authorizations($lim, $self->{'account'}{'id'});
		foreach my $row (@authorizations) {
			printf("%s: %s\n", $row->{'authorization_id'}, format_period($row->{'period'}));
			printf("  Bank account name: %s\n", $row->{'bank_account_name'});
			printf("  IBAN: %s\n", $row->{'iban'});
			printf("  BIC: %s\n", $row->{'bic'});
			printf("  Signature date: %s\n", $row->{'signature_date'});
		}
	} elsif($command eq "generate") {
		print "New authorization ID: " . ::generate_directdebit_authorization($lim) . "\n";
	} elsif($command eq "authorize") {
		try {
			cli_add_directdebit_authorization($self);
		} catch {
			warn "\nFailed to add authorization: $_\n";
		}
	} elsif($command eq "deauthorize") {
		try {
			cli_delete_directdebit_authorization($self);
		} catch {
			warn "\nFailed to remove authorization: $_\n";
		}
	} else {
		warn help_directdebit();
	}
}

sub help_directdebit {
	return <<HELP;
directdebit [info]
directdebit generate
directdebit authorize
directdebit deauthorize

Get information about current authorizations, generate new authorization ID's,
add a new authorization or delete an existing one.
HELP
}

sub smry_directdebit {
	return "various direct-debit related operations";
}

sub run_changes {
	my ($self, $date) = @_;

	my @changes;
	if($self->{'sim'}) {
		@changes = ::sim_changes_between($self->{'lim'}, $self->{'sim'}{'iccid'}, $date);
	} elsif($self->{'account'}) {
		@changes = ::account_changes_between($self->{'lim'}, $self->{'account'}{'id'}, $date);
	} else {
		warn "No SIM or account selected.\n";
		return;
	}

	foreach my $changeset (@changes) {
		my $period = delete $changeset->{'period'};
		my ($startdate) = $period =~ /^(?:\(|\[)(\d{4}-\d\d-\d\d)?,.*\)$/;
		if(!$startdate) {
			die "Couldn't parse period: $period\n";
		}

		print "$startdate:";
		print " (no actual changes)\n" if keys %$changeset == 0;
		print "\n" if keys %$changeset > 1;

		foreach my $key (sort keys %$changeset) {
			my $value = $changeset->{$key};
			$value = "(undef)" if(!defined($value));
			print "  $key => $value\n";
		}
	}

	if($self->{queued_changes}) {
		print "\nQueued changes:\n";
		foreach my $key (keys %{$self->{'queued_changes'}}) {
			my $value = $self->{'queued_changes'}{$key};
			$value = "(undef)" if(!defined($value));
			print "  $key => $value\n";
		}
	}
}

sub help_changes {
	return <<HELP;
changes [date]

List all changes done to the current object (account or SIM). If a parameter is given,
list all changes since that date. If changes are queued, list them too.
HELP
}

sub smry_changes {
	return "list changes to the current object";
}

sub run_set {
	my ($self, $variable, $value) = @_;
	if(@_ > 3 || !$variable) {
		warn help_set();
		return;
	}

	if(!$self->{'account'}) {
		warn "You must select an account or SIM first.\n";
		return;
	}

	my @forbidden_variables = qw(id iccid period activation_invoice_id last_monthly_fees_invoice_id
		last_monthly_fees_month last_contribution_month);
	for(@forbidden_variables) {
		if($_ eq $variable) {
			warn "Variable cannot be changed using shell: $variable\n";
			return;
		}
	}

	my $object = $self->{'sim'} ? $self->{'sim'} : $self->{'account'};
	if(!exists $object->{$variable}) {
		warn "No such variable: $variable\n";
		return;
	}

	$value = numeric_convert_if_numeric($value);

	$self->{queued_changes} ||= {};
	$self->{queued_changes}{$variable} = $value;
	my $num = keys %{$self->{queued_changes}};
	print "$num queued changes.\n";
	print "Run 'changes' to see them, 'commit' to commit them, 'rollback' to cancel them.\n";
}

sub help_set {
	return <<HELP;
set variable
set variable value...

Queue a change to set 'variable' to 'value' in the selected object. If no
'value' is given, set it to undefined. 'variable' must be a valid property of
the currently selected object. Select an account or SIM first using 'account'
or 'sim'. When you want to change an account property and have a SIM selected,
use 'back' to go back to the account first.

After queueing a set of changes, you MUST USE the 'commit' command to actually
confirm the changes. Use the 'rollback' command to rollback the queued changes.
HELP
}

sub smry_set {
	return "prepare changes to the current object";
}

sub run_rollback {
	my ($self) = @_;

	if(!$self->{queued_changes}) {
		warn "No changes were queued.\n";
		return;
	}

	my $q = delete $self->{queued_changes};
	print "Rolled back changes:\n";
	foreach(keys %$q) {
		my $value = $q->{$_};
		$value = "(undef)" if(!defined($value));
		print "  $_ => $value\n";
	}
}

sub help_rollback {
	return <<HELP;
rollback

Rollback the queued changes, i.e. forget about them without executing them.
HELP
}

sub smry_rollback {
	return "rollback prepared changes";
}

sub run_commit {
	my ($self, $date) = @_;

	if(!$self->{queued_changes}) {
		warn "No changes were queued.\n";
		return;
	}

	if(!$date) {
		warn "Cannot commit without a date. Maybe you meant 'commit today'?\n";
		return;
	}

	my $q = delete $self->{queued_changes};
	try {
		if($self->{sim}) {
			::update_sim($self->{lim}, $self->{sim}{iccid}, $q, $date);
		} elsif($self->{account}) {
			::update_account($self->{lim}, $self->{account}{id}, $q, $date);
		} else {
			die "No account? This should be impossible\n";
			# ... because the other commands disallow scope changes when there are
			# queued changes
		}
		my $num = keys %$q;
		print "Done. $num changes committed.\n";
	} catch {
		warn "Commit failed: $_\n";
	};
}

sub help_commit {
	return <<HELP;
commit <date>

Commit the queued changes. You must give a date (or 'today') when the changes
become effective.
HELP
}

sub smry_commit {
	return "commit prepared changes";
}

sub run_delete {
	my ($self, $date) = @_;

	if(!$date) {
		warn help_delete();
		return;
	}

	if($self->{queued_changes}) {
		warn "Cannot delete object when changes are queued. Use 'commit' or 'rollback' first.\n";
		return;
	}

	if(!$self->{'account'}) {
		warn "Can only delete when an object is selected.\n";
		return;
	}

	my @changes;

	if($self->{'sim'}) {
		@changes = ::sim_changes_between($self->{'lim'}, $self->{'sim'}{'iccid'}, $date);
	} else {
		@changes = ::account_changes_between($self->{'lim'}, $self->{'account'}{'id'}, $date);
	}

	my $force;
	if(@changes > 0) {
		print "The date you selected is historic for this object. The following changes appear\n";
		print "at or after this date:\n";
		print "\n";
		for(0..$#changes) {
			if($_ == 3) {
				print "...and " . (@changes-$_) . " more (see 'changes' to see them).\n";
				last;
			}

			my $changeset = $changes[$_];
			my $period = delete $changeset->{'period'};
			my ($startdate) = $period =~ /^(?:\(|\[)(\d{4}-\d\d-\d\d)?,.*\)$/;
			if(!$startdate) {
				die "Couldn't parse period: $period\n";
			}

			print "$startdate:";
			print " (no actual changes)\n" if keys %$changeset == 0;
			print "\n" if keys %$changeset > 1;
			foreach my $key (keys %$changeset) {
				my $value = $changeset->{$key};
				$value = "(undef)" if(!defined($value));
				print "  $key => $value\n";
			}
		}
		print STDERR "Sure to LOSE the changes above and DELETE this object? ('yes' or 'no'): ";
		$force = 1;
	} else {
		print STDERR "Sure to DELETE this object? ('yes' or 'no'): ";
	}

	my $yesno = <STDIN>;
	1 while chomp $yesno;
	if($yesno ne 'yes') {
		print STDERR "Cancelled.\n";
		return;
	}

	if($self->{'sim'}) {
		::delete_sim($self->{'lim'}, $self->{'sim'}{'iccid'}, $date, $force);
		delete $self->{'sim'};
	} else {
		::delete_account($self->{'lim'}, $self->{'account'}{'id'}, $date, $force);
		delete $self->{'account'};
	}
}

sub help_delete {
	return <<HELP;
delete <date>

Delete the selected object (account or SIM). You must give a date (or 'today') when the
changes become effective.
HELP
}

sub smry_delete {
	return "delete selected object";
}

sub run_letter {
	my ($self, $address) = @_;
	if(!$self->{'account'}) {
		warn "An account must be selected to use this command.";
		return;
	}

	if(!$address) {
		warn help_letter();
		return;
	}

	my $sim = $self->{'sim'};
	if(!$sim) {
		# TODO: use a WHERE ... ORDER BY ... query
		my @sims = grep { $_->{'owner_account_id'} && $_->{'owner_account_id'} == $self->{'account'}{'id'} }
			::list_sims($self->{lim});

		if(@sims == 0) {
			warn "An acccount with at least one SIM must be selected to use this command.\n";
			return;
		} elsif(@sims > 1) {
			warn "An account with more than one SIM is selected. Select a SIM explicitly using 'sim'.\n";
			return;
		} else {
			$sim = $sims[0];
		}
	}

	my @localtime = localtime;
	my $objects = {
		authorization => ::generate_directdebit_authorization($lim),
		pwd => realpath(dirname($0)) . "/../letter-generate/",
		account => $self->{'account'},
		sim => $sim,
		date => sprintf("%04d-%02d-%02d", $localtime[5] + 1900, $localtime[4] + 1, $localtime[3]),
	};

	my $instancename = $self->{'account'}{'company_name'};
	if(!$instancename) {
		$instancename = sprintf("%s %s", $self->{'account'}{'first_name'}, $self->{'account'}{'last_name'});
	}
	$instancename =~ s/ /_/g;

	my @templates = ("welkomstbrief", "sepamachtiging");
	for(my $file = 0; $file < @templates; ++$file) {
		my $template = $templates[$file];

		my $filename = sprintf("%s-%s.pdf", $template, $instancename);
		try {
			my $pdf = ::generate_pdf($lim, $objects, "../letter-generate/" . $template . ".tex");
			::send_pdf_by_email($lim, $pdf, $filename, "Liminfra document",
				"Attached to this e-mail is your requested Liminfra document.",
				"Liminfra user", $address);
		} catch {
			warn "Failed to generate and send PDF for template $template: $_\n";
		};
	}
}

sub help_letter {
	return <<HELP;
letter [email_address]

Send the welcome letters for the selected account to the given e-mail address.
Account must have at least one SIM; if it has more than one, one must be
selected using the 'sim' command.
HELP
}

sub smry_letter {
	return "generate and e-mail welcome letters";
}

sub run_queue {
	my ($self, $cmd, $description, $count, $price, $taxrate) = @_;
	if(!$self->{'account'}) {
		warn "An account must be selected to use this command.";
		return;
	}

	if(!$cmd) {
		warn help_queue();
		return;
	}

	my $dbh = $lim->get_database_handle();

	if ($cmd eq "list") {
		my $sql = "SELECT * FROM invoice_itemline WHERE queued_for_account_id = ? AND invoice_id IS NULL";
		my $sth = $dbh->prepare($sql);
		$sth->execute($self->{'account'}{'id'});
		while(my $row = $sth->fetchrow_hashref()) {
			printf("%s\n\tCount: %.2f\n\tPrice: %.4f\n\tTotal: %.2f\n", $row->{'description'},
				$row->{'item_count'}, $row->{'item_price'}, $row->{'rounded_total'});
		}

	} elsif ($cmd eq "add") {
		$taxrate ||= 0.21;

		$taxrate =~ s/,/./g;
		$price =~ s/,/./g;

		warn "Item count must be positive.\n" if ($count < 0);
		return if ($count < 0);

		warn "Tax rate should be between 0 and 1.\n" if ($taxrate < 0 || $taxrate > 1);
		return if ($taxrate < 0 || $taxrate > 1);

		my $total_amount = $count * $price;

		print "Description : " . $description . "\n";
		print "Item count  : " . $count . "\n";
		print "Item price  : " . $price . "\n";
		print "Tax rate    : " . $taxrate . "\n";
		print "Total amount: " . $total_amount . "\n";
		print "Rounded     : " . sprintf("%.2f", $total_amount) . "\n";

		my $confirminput = ask_question(">>> Please confirm insertion with 'yes':");
		my $input = ($confirminput eq lc("yes")) ? 1 : 0;
		if ($input != 1) {
			warn "Insertion not confirmed, returning to shell...\n";
			return
		}
		my $sth = $dbh->prepare("INSERT INTO invoice_itemline (type, queued_for_account_id, description,
			item_count, item_price, taxrate, rounded_total) VALUES ('NORMAL', ?, ?, ?, ?, ?, ?);");
		$sth->execute($self->{'account'}{'id'}, $description, $count, $price, $taxrate, sprintf("%.2f", $total_amount));
	}
}

sub help_queue {
	return <<HELP;
queue list
queue add [description] [item count] [item price] [tax rate]

List all queued itemlines for an account.

Add a queued itemline for the selected account. That item will be added to the
next invoice. If [tax rate] is omitted, the default value of 0.21 (21%) will be
used. [item count] must be positive. [item price] may be negative.

HELP
}

sub smry_queue {
	return "list and add queued itemlines";
}

sub run_balance {
	my ($self, $date) = @_;

	if($self->{'account'}) {
		my @p_and_i = ::get_payments_and_invoices($lim, $self->{'account'}{'id'}, $date);
		foreach(@p_and_i) {
			my $descr;
			my $amount;
			if($_->{'objecttype'} eq "PAYMENT") {
				$amount = $_->{'amount'};
				$descr = lc($_->{'type'}) .  " " . $_->{'origin'};
			} else {
				$amount = -$_->{'rounded_with_taxes'};
				$descr = $_->{'id'};
			}
			printf("%s %s %40s  %s -> %s\n", lc($_->{'objecttype'}), $_->{'date'}, $descr,
				::sprintf_money($amount), ::sprintf_money($_->{'balance'}));
		}
	} else {
		my $dbh = $lim->get_database_handle();
		my $sth = $dbh->prepare("SELECT DISTINCT(id) FROM account ORDER BY id ASC");
		$sth->execute();
		my @accounts;
		while(my $account_id = $sth->fetchrow_hashref) {
			$account_id = $account_id->{'id'};
			my $asth = $dbh->prepare("SELECT id, first_name, last_name, company_name FROM account WHERE id=? AND
				period=(SELECT period FROM account WHERE id=? ORDER BY lower(period) DESC LIMIT 1)");
			$asth->execute($account_id, $account_id);
			push @accounts, $asth->fetchrow_hashref;
		}
		for my $account (@accounts) {
			my @p_and_i = ::get_payments_and_invoices($lim, $account->{'id'}, $date);
			my $balance = @p_and_i ? (pop @p_and_i)->{'balance'} : 0;
			my $name = sprintf("%s %s", $account->{'first_name'}, $account->{'last_name'});
			if($account->{'company_name'}) {
				$name = $account->{'company_name'} . " ($name)";
			}
			printf("%3d %45s -> %s\n", $account->{'id'}, $name, ::sprintf_money($balance));
		}
	}
}

sub help_balance {
	return <<HELP;
balance [date]

If an account is selected, list all invoices and payments along with their
running balance as of the given date (or all of them if no date is given).
The format is as follows:

<pay/inv> <date>   <transaction type> <information>  <amount> -> <new balance>

If no account is selected, list the balance for every account as of the given
date (or taking into account all invoices and payments if no date is given).
The format is as follows:

<account ID>                <account name> [(<company name>)] -> <balance>

HELP
}

sub smry_balance {
	return "list user's balance after invoices and payments";
}
