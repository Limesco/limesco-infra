#!/usr/bin/perl
use strict;
use warnings;
use lib '../../lib';
use lib "../lib";
use lib "lib";
use Encode;
use Curses;
use Curses::UI;
use Term::Menu;
use Test::Deep::NoTest;
use Data::Dumper;
use Limesco;

require 'account-change.pl';
require '../sim-change/sim-change.pl';

my $lim = Limesco->new_from_args(\@ARGV);
my $ui = Curses::UI->new(-clear_on_exit => 1,
	-color_support => 1,
	-mouse_support => 0);

my $win;
my $accountwin;
my $simwin;
my $allocate_listbox;

reinit:

if($simwin) {
	$simwin->hide();
	$accountwin->delete('simwin');
	undef $simwin;
}
if($accountwin) {
	$accountwin->hide();
	$win->delete('subwin');
	undef $accountwin;
}
if($win) {
	$win->hide();
	$ui->delete('win');
}

$win = $ui->add('win', 'Window',
	-border => 1,
	-bfg => "red",
	-title => "Account list");
$win->show();
$win->focus();

my @accounts = list_accounts($lim);

@accounts = sort { lc($a->{'first_name'}||"") cmp lc($b->{'first_name'}||"") } @accounts;
@accounts = sort { lc($a->{'last_name'}||"") cmp lc($b->{'last_name'}||"") } @accounts;
my $listbox = $win->add("acctbox", 'Listbox',
	-values => [map {$_->{'id'}} @accounts],
	-labels => {map {$_->{'id'} => " ".account_to_str($_, 1)} @accounts},
	-vscrollbar => 'right',
	-hscrollbar => 'bottom',
	-htmltext => 1,
);
$listbox->focus();

$win->set_binding(sub {
	if($allocate_listbox) {
		$allocate_listbox->hide();
		$accountwin->delete('sim_allocbox');
		$accountwin->focus();
		undef $allocate_listbox;
	} elsif($simwin) {
		$simwin->hide();
		$accountwin->delete('simwin');
		$accountwin->focus();
		undef $simwin;
	} elsif($accountwin) {
		$accountwin->hide();
		$win->delete('subwin');
		$win->focus();
		undef $accountwin;
	} else {
		exit(0);
	}
}, "q", KEY_LEFT());

## ADD
$win->set_binding(sub {
	if($allocate_listbox) {
		# ignore
	} elsif($simwin) {
		# ignore
	} elsif($accountwin) {
		my $account_id = $accountwin->userdata();
		my @unallocated_sims = grep { $_->{'state'} eq "STOCK" } list_sims($lim);
		if(@unallocated_sims == 0) {
			$ui->dialog("There are no unallocated SIMs to allocate.");
			return;
		}
		my $allocate_listbox = $accountwin->add('sim_allocbox', 'Listbox',
			-values => [map {$_->{'iccid'}} @unallocated_sims],
			-labels => {map {$_->{'iccid'} => sim_to_str($_, 0, 1)} @unallocated_sims},
			-vscrollbar => 'right',
			-hscrollbar => 'bottom',
			-htmltext => 1,
			-title => "Select a SIM to allocate",
		);
		$allocate_listbox->focus();
		$allocate_listbox->onChange(sub {
			my $sim_id = $allocate_listbox->get();
			$allocate_listbox->clear_selection();
			my $apn = "";
			my $cct = "";
			my $npt = "";
			$ui->leave_curses();
			until($apn eq "APN_NODATA" || $apn eq "APN_500MB" || $apn eq "APN_2000MB") {
				print "Valid inputs are: APN_NODATA, APN_500MB, APN_2000MB.\n";
				print "Internet / APN type? ";
				$apn = <STDIN>;
				1 while chomp $apn;
			}
			until($cct eq "DIY" || $cct eq "OOTB") {
				print "Valid inputs are: DIY, OOTB.\n";
				print "Call connectivity type? ";
				$cct = <STDIN>;
				1 while chomp $cct;
			}
			until($npt eq "true" || $npt eq "false") {
				print "Valid inputs are: true, false.\n";
				print "Will a number be ported immediately? ";
				$npt = <STDIN>;
				1 while chomp $npt;
			}
			update_sim($lim, $sim_id, {
				owner_account_id => $account_id,
				data_type => $apn,
				call_connectivity_type => $cct,
				exempt_from_cost_contribution => 0,
				state => "ACTIVATED",
			});
			$ui->reset_curses();
			goto reinit;
		});
	} else {
		$ui->leave_curses();
		print "Creating a new account.\n";
		my @vars = (
			["First name", "first_name"],
			["Last name", "last_name"],
			["Company name (or empty)", "company_name"],
			["E-mail address", "email"],
			["Street address", "street_address"],
			["Postal code", "postal_code"],
			["City / locality (, country)", "city"],
		);
		my %opts;
		foreach(@vars) {
			print $_->[0] . "? ";
			my $var = <STDIN>;
			$var = decode_utf8($var);
			1 while chomp($var);
			$opts{$_->[1]} = $var;
		}
		my $account = create_account($lim, \%opts);
		$ui->reset_curses();
		goto reinit;
	}
}, "a");

## UPDATE
$win->set_binding(sub {
	if($allocate_listbox) {
		# ignore
	} elsif($simwin) {
		$ui->leave_curses();
		my $simid = $simwin->userdata();
		gui_update_sim($lim, $simid);
		$ui->reset_curses();
		goto reinit;
	} elsif($accountwin) {
		$ui->leave_curses();
		my $accountid = $accountwin->userdata();
		gui_update_account($lim, $accountid);
		$ui->reset_curses();
		goto reinit;
	} else {
		# ignore
	}
}, "u", "e");

$listbox->onChange(sub {
	my $account_id = $listbox->get();
	$listbox->clear_selection();
	my $account = get_account($lim, $account_id);
	$accountwin = $win->add('subwin', 'Window',
		-border => 1,
		-bfg => "green",
		-userdata => $account_id,
		-title => "Account view: " . account_to_str($account));

	my @sims = sort { $a->{'iccid'} <=> $b->{'iccid'} }
		grep { $_->{'owner_account_id'} && $_->{'owner_account_id'} == $account_id } list_sims($lim);

	my $ext = $account->{'externalAccounts'} || {};
	my $text = join "\n",
		"ID: " . $account->{'id'},
		"E-mail address: " . ($account->{'email'} || "unset"),
		"Account state: " . ($account->{'state'} || "unset"),
		"Company name: " . ($account->{'company_name'} || ""),
		"Full name: " . ($account->{'first_name'} || "") . " " . ($account->{'last_name'} || ""),
		"Address: ",
		"    " . ($account->{'street_address'} || ""),
		"    " . ($account->{'postal_code'} || "") . " " . ($account->{'city'} || "");

	my $halfheight = $accountwin->height() / 2;
	$accountwin->add('accountinfo', 'Label', -text => $text, -width => -1, -height => $halfheight)->show();
	$accountwin->add('simboxtitle', 'Label', -text => "SIMs in this account", -width => -1, -height => 1, -y => $halfheight)->show();

	my $simbox = $accountwin->add("simbox", 'Listbox',
		-values => [map {$_->{'iccid'}} @sims],
		-labels => {map {$_->{'iccid'} => sim_to_str($_, 0, 1)} @sims},
		-vscrollbar => 'right',
		-hscrollbar => 'bottom',
		-htmltext => 1,
		-height => $halfheight,
		-y => $halfheight + 1,
		-title => "SIMs in this account",
	);

	$simbox->show();
	$accountwin->show();
	$simbox->focus();

	$simbox->onChange(sub {
		my $sim_id = $simbox->get();
		return if(!defined($sim_id));
		$simbox->clear_selection();
		my $sim = get_sim($lim, $sim_id);
		$simwin = $accountwin->add('simwin', 'Window',
			-border => 1,
			-bfg => "yellow",
			-userdata => $sim_id,
			-title => "SIM view: " . sim_to_str($sim));
		my $csd = "Not started yet";
		if($sim->{'contractStartDate'}) {
			my (undef, undef, undef, $mday, $mon, $year) = localtime($sim->{'contractStartDate'} / 1000);
			$csd = sprintf("%4d-%02d-%02d", $year + 1900, $mon + 1, $mday);
		}
		my $owner = "(none)";
		if($sim->{'owner_account_id'}) {
			$owner = account_to_str(get_account($lim, $sim->{'owner_account_id'}), 0);
		}
		my @sipsettings = "(No SIP settings found in this SIM)";
		if($sim->{'sipSettings'}) {
			@sipsettings = (
				"Realm: " . $sim->{'sip_realm'},
				"Username: " . $sim->{'sip_username'},
				"Authentication username: " . $sim->{'sip_authentication_username'},
				"Password: " . $sim->{'sip_password'},
				"URI: " . $sim->{'sip_uri'},
				"Expiry: " . $sim->{'sip_expiry'},
				"SpeakUp trunk password: " . $sim->{'sip_trunk_password'},
			);
		}
		my $lmfi = "(none)";
		if($sim->{'last_monthly_fees_month'}) {
			my ($yearmonth) = $sim->{'last_monthly_fees_month'} =~ /^(\d{4}-\d\d)-\d\d$/;
			$yearmonth ||= $sim->{'last_monthly_fees_month'};
			$lmfi = sprintf("At %s: %s", $yearmonth, $sim->{'last_monthly_fees_invoice_id'});
		}
		my $text = join "\n",
			"ICCID: " . $sim->{'iccid'},
			"PUK: " . $sim->{'puk'},
			"State: " . $sim->{'state'},
			"",
			"Contract start date: $csd",
			"Call connectivity type: " . $sim->{'call_connectivity_type'},
			"Phone number: unknown",
			"Owner: $owner",
			"APN type: " . $sim->{'data_type'},
			"Exempt from cost contribution: " . $sim->{'exempt_from_cost_contribution'},
			"Activation invoice ID: " . $sim->{'activation_invoice_id'},
			"Last monthly fees invoice: $lmfi",
			"",
			@sipsettings;
		$simwin->add('siminfo', 'Label', -text => $text)->show();
		$simwin->show();
		$simwin->focus();
	});

});

$ui->mainloop();
exit;

sub account_to_str {
	my ($account, $html) = @_;
	$html ||= 0;

	my $name = $account->{'first_name'} . " " . $account->{'last_name'};
	my $email = $account->{'email'};
	my $company = $account->{'company_name'};
	my $namedescr = $html ? ("<underline>" . $name . "</underline>") : $name;
	if($company) {
		$namedescr = $html ? ("<underline>$company</underline> ($name)") : "$company ($name)";
	}
	return "$namedescr <$email>";
}

sub sim_to_str {
	my ($sim, $with_account, $html) = @_;
	$with_account ||= 0;
	$html ||= 0;

	my $iccid = $sim->{'iccid'};

	my $marker = "   ";
	if($sim->{'state'} eq "STOCK") {
		return "Stock SIM, ICCID " . $iccid;
	} elsif($sim->{'state'} eq "ALLOCATED") {
		$marker = "[A]";
	} elsif($sim->{'state'} eq "ACTIVATION_REQUESTED") {
		$marker = "[Q]";
	} elsif($sim->{'state'} eq "DISABLED") {
		$marker = "[D]";
	}

	my $phonenr = "(unknown)";
	my ($startdate) = $sim->{period} =~ /^\[(\d{4}-\d\d-\d\d)/;
	return sprintf("%s started %s, number %s, iccid %s", $marker, $startdate, $phonenr, $iccid);
}

sub gui_update_account {
	my ($lim, $accountid) = @_;
	my $account = get_account($lim, $accountid);
	die if(!$account);

	# Two types of account updates are available: updates suggested by the API,
	# and direct field updates.
	# Updates suggested by the API might spawn third actions to take, such as
	# sending an e-mail. They may also cause direct field updates. When the
	# updates are explicitly confirmed, the field updates are written into the
	# Account object and sent back to the API, which will save them into the
	# database.
	#my @suggested = $lim->getAccountValidation($accountid);
	#my $suggested = [map {[0, $_]} @suggested];
	my $suggested = [];
	my $updates = {};
	warn Dumper($account);
	gui_update_object_step("account", $account, $suggested, $updates);
	return if(keys %$updates == 0);
	if(!eq_deeply($account, get_account($lim, $accountid))) {
		warn "WARNING: Account changed during process, refusing to process changes.\n";
		warn Dumper($updates);
		die "Fatal error.\n";
	}
	warn "Old account:\n";
	warn Dumper($account);
	warn "\n";
	$account = update_account($lim, $accountid, $updates);
	warn "New account:\n";
	warn Dumper($account);
	warn "\n";
}

sub gui_update_sim {
	my ($lim, $simid) = @_;
	my $sim = get_sim($lim, $simid);
	return if(!$sim);
	#my @suggested = $lim->getSimValidation($simid);
	#my $suggested = [map {[0, $_]} @suggested];
	my $suggested = [];
	my $updates = {};
	warn Dumper($sim);
	gui_update_object_step("sim", $sim, $suggested, $updates);
	return if(keys %$updates == 0);
	if(!eq_deeply($sim, get_sim($lim, $simid))) {
		warn "WARNING: SIM changed during process, refusing to process changes.\n";
		warn Dumper($updates);
		die "Fatal error.\n";
	}
	warn "Old SIM:\n";
	warn Dumper($sim);
	warn "\n";
	$sim = update_sim($lim, $simid, $updates);
	warn "New SIM:\n";
	warn Dumper($sim);
	warn "\n";
}

sub gui_update_object_step {
	my ($type, $object, $suggested, $proposed_updates) = @_;

	system 'clear';

	if(%$proposed_updates) {
		print "Planned updates:\n";
		foreach(keys %$proposed_updates) {
			my $u = $proposed_updates->{$_};
			$u .= "\n" if(!ref($u));
			$u = Dumper($u) if(ref($u));
			print "  $_ => $u";
		}
	}

	my $prompt = Term::Menu->new(
		beforetext => "Available actions:",
		aftertext => "Select an action: ");

	my $choices = 0;
	my %options;

	for(my $i = 0; $i < @$suggested; ++$i) {
		my ($rerun, $suggestion) = @{$suggested->[$i]};
		my $title = ($rerun ? "Re-run suggested: " : "Suggested: ") . $suggestion->{'explanation'};
		$options{'sugg_' . $i} = [$title, ++$choices];
	}

	$options{'specific'} = ["Update specific field", ++$choices];

	my $answer = $prompt->menu(
		write  => ["Write updates", "w"],
		cancel => ["Cancel updates", "c"],
		%options
	);
	print "\n";
	if($answer eq "write") {
		return;
	} elsif($answer eq "cancel") {
		for(keys %$proposed_updates) {
			delete $proposed_updates->{$_};
		}
		return;
	} elsif($answer eq "specific") {
		$prompt = Term::Menu->new(
			beforetext => "What field to update?",
			aftertext => "Select an action: ");
		$choices = 0;
		%options = ();
		foreach(keys %$object) {
			next if($_ eq "id");
			$options{'opt_' . $_} = [$_, ++$choices];
		}
		$answer = $prompt->menu(
			cancel => ["Cancel update", "c"],
			%options);
		print "\n";
		if($answer eq "cancel") {
			# do nothing
		} elsif($answer =~ /^opt_(.+)$/) {
			# update field $1
			gui_run_object_field_update($type, $object, $proposed_updates, $1);
		} else {
			die "Unexpected answer from menu";
		}
	} elsif($answer =~ /^sugg_(\d+)$/) {
		my ($suggestion) = $suggested->[$1][1];
		if(gui_run_object_suggestion($type, $object, $proposed_updates, $suggestion)) {
			$suggested->[$1][0] = 1;
		}
	} else {
		die "Unexpected answer from menu";
	}
	print "\n\nReturning to menu...\n";
	sleep 1;
	return gui_update_object_step($type, $object, $suggested, $proposed_updates);
}

sub gui_run_object_suggestion {
	die "There are no more suggestions, this should be re-implemented\n";
}

sub gui_run_object_field_update {
	my ($type, $object, $proposed_updates, $field) = @_;

	my %closed_choices;
	my @date_fields;

	if($type eq "account") {
		%closed_choices = (
			state => [qw(UNPAID UNCONFIRMED CONFIRMATION_REQUESTED CONFIRMED CONFIRMATION_IMPOSSIBLE DEACTIVATED)],
		);
	} elsif($type eq "sim") {
		%closed_choices = (
			state => [qw(STOCK ALLOCATED ACTIVATION_REQUESTED ACTIVATED DISABLED)],
			data_ype => [qw(APN_NODATA APN_500MB APN_2000MB)],
			exempt_from_cost_contribution => [qw(true false)],
			call_connectivity_type => [qw(OOTB DIY)],
		);
		@date_fields = ('last_monthly_fees_month');
	} else {
		die "Unknown type";
	}
	my @closed_options = keys %closed_choices;

	print "Updating field $field.\n";
	my $curval = $object->{$field};
	if(ref($object->{$field}) eq "JSON::XS::Boolean" || ref($object->{$field}) eq "JSON::PP::Boolean") {
		$curval = $object->{$field} ? "true" : "false";
	} elsif(ref($object->{$field})) {
		print "Unable to update field $field: it's a complex " . ref($object->{$field}) . " structure.\n";
		return;
	}
	$curval = Dumper($curval) if(ref($curval));
	print "Current value: $curval\n";
	if(exists $proposed_updates->{$field}) {
		my $planupd = $proposed_updates->{$field};
		$planupd = Dumper($planupd) if(ref($planupd));
		print "Planned update: $planupd\n";
	}
	
	if($field ~~ @closed_options) {
		my $prompt = Term::Menu->new(
			beforetext => "Set it to?",
			aftertext => "Select an action: ");
		my %options;
		my $count = 0;
		foreach(@{$closed_choices{$field}}) {
			$options{'opt_' . $_} = [$_, ++$count];
		}
		my $answer = $prompt->menu(
			"cancel" => ["Cancel", "c"],
			%options
		);
		if($answer eq "cancel") {
			# ignore
		} elsif($answer =~ /^opt_(.+)$/) {
			$proposed_updates->{$field} = $1;
		} else {
			die "Unexpected answer from menu";
		}
	} elsif($field ~~ @date_fields) {
		print "Enter a date, in the format YYYY-MM-DD (i.e. 2012-10-13), or nothing to cancel:\n";
		my $value = <STDIN>;
		1 while chomp($value);
		use Time::Local;
		if($value =~ /^(\d\d\d\d)-(\d\d)-(\d\d)$/) {
			$proposed_updates->{$field} = timelocal(0, 0, 0, $3, $2-1, $1-1900) * 1000;
		} else {
			print "That's not valid input.\n";
		}
	} else {
		# Open choice
		print "Enter a new value, or underscore ('_') to cancel:\n";
		my $value = <STDIN>;
		$value = decode_utf8($value);
		1 while chomp($value);
		if($value ne "_") {
			$proposed_updates->{$field} = $value;
		}
	}
}
