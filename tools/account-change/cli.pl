#!/usr/bin/perl
use strict;
use warnings;
use lib '../../lib';
use lib '../lib';
use lib 'lib';
use Limesco;

do 'account-change.pl';
do '../sim-change/sim-change.pl';

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
use base qw(Term::Shell);

sub today {
	my @time = localtime(time);
	return sprintf("%04d-%02d-%02d", $time[5] + 1900, $time[4] + 1, $time[3]);
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
		1 while chomp $answer;
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
	my ($self, $search) = @_;
	if(@_ != 2 || !$search) {
		warn help_account();
		return;
	}

	delete $self->{account};
	delete $self->{sim};

	if($search =~ /^\d+$/) {
		$self->{account} = $self->{lim}->get_account($search);
	} else {
		try {
			$self->{account} = $self->{lim}->get_account_like($search);
		} catch {
			warn $_;
		};
	}
}

sub help_account {
	return <<HELP;
account <id|string>

Selects an account. You can give an ID to immediately select that account, or
give a single string to search through accounts. If multiple accounts match the
search string, a list is given and the current account is deselected.
HELP
}

sub smry_account {
	return "selects an account";
}

sub run_sim {
	my ($self, $iccid) = @_;
	if(@_ != 1 && @_ != 2) {
		warn help_sim();
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
		# TODO: list phonenumbers here
		printf("%s %s %s\n", $_->{'iccid'}, $_->{'state'}, $_->{'period'});
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
	$sim->{'porting_state'} = ask_question("Will a number be ported immediately? (y/n)", sub {
		if(lc($_[0]) eq "y") {
			return "WILL_PORT";
		} elsif(lc($_[0]) eq "n") {
			return "NO_PORT";
		} else {
			warn "Invalid response: make it 'y' or 'n'.\n";
			return;
		}
	});
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
	return ::create_sim($self->{'lim'}, $sim, $starting_date);
}

sub run_create {
	my ($self) = @_;
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
		warn "Cannot create anything in SIM mode. Use 'back' to go back.";
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
	} elsif(!$what) {
		$what = "account" if($self->{'account'});
		$what = "sim" if($self->{'sim'});
		if(!$what) {
			print "Info about what? Select an account or SIM first.\n";
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
		print "Porting state: " . $s->{'porting_state'} . "\n";
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
		print "Address:\n";
		print "  " . $a->{'street_address'} . "\n";
		print "  " . $a->{'postal_code'} . " " . $a->{'city'} . "\n";
	}
}

sub help_info {
	return <<HELP;
info [sim|account]

Give information about the currently selected SIM or account.
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
