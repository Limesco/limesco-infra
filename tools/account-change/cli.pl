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

sub init {
	my ($self) = @_;
	$self->{lim} = $self->{API}{args}[0];
	if(!$self->{lim} || ref($self->{lim}) ne "Limesco") {
		croak "Failed to create LimescoShell: Limesco object must be given as the first parameter";
	}

	delete $self->{account};
	delete $self->{sim};
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
