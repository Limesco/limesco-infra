#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Limesco::TemporalSupport;
use Try::Tiny;

=head1 sim-change.pl

Usage: sim-change.pl [no CLI options available yet]

This file contains methods to create SIMs, change their properties and delete
them, in the temporal fashion that the database uses. See the documentation for
Limesco::TemporalSupport for more information.

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--export") {
			print $args->[++$$iref];
		} else {
			return 0;
		}
	});
}

=head2 Methods

=cut

# The field information used by change-support to handle temporal SIM
# changes
sub _sim_object_info {
	return {
		# The fields without which a SIM may not be created. This should
		# correspond to NOT NULL fields without a DEFAULT in the SIM table.
		required_fields => [qw(iccid state puk)],
		# The fields which can be automatically filled in by the database. This list
		# should correspond to either NULL fields or those with a DEFAULT in the
		# sim table.
		optional_fields => [qw(owner_account_id data_type exempt_from_cost_contribution
			porting_state activation_invoice_id last_monthly_fees_invoice_id
			last_monthly_fees_month call_connectivity_type sip_realm sip_username
			sip_authentication_username sip_password sip_uri sip_expiry sip_trunk_password)],
		table_name => "sim",
		primary_key => "iccid",
	};
}

=head3 create_sim($lim | $dbh, $sim, [$date])

Create a SIM. $date is the optional starting date of the SIM; if not given,
'today' is assumed. $sim must contain all required fields (e.g. iccid and
state), must contain all state-required fields (e.g. data_type if the state is
not 'STOCK'), may contain optional fields (e.g. sip_realm) and must contain
nothing else.

This method returns the newly created SIM, or throws an exception if
something failed.

=cut

sub create_sim {
	my ($lim, $sim, $date) = @_;
	return create_object($lim, _sim_object_info(), $sim, $date);
}

=head3 get_sim($lim | $dbh, $sim_iccid, [$date])

Retrieve a SIM. If $date is given, retrieve an SIM on the given date.

=cut

sub get_sim {
	my ($lim, $sim_id, $date) = @_;
	return get_object($lim, _sim_object_info(), $sim_id, $date);
}

=head3 list_sims($lim | $dbh, [$date])

Retrieve a list of SIMs active on the given $date. If $date is not given,
only SIMs active 'today' are returned.

=cut

sub list_sims {
	my ($lim, $date) = @_;
	return list_objects($lim, _sim_object_info(), $date);
}

=head3 update_sim($lim | $dbh, $sim_iccid, $changes, [$date])

Update a SIM. $date is the optional date of the changes; if not given, 'today'
is assumed. $sim_iccid is the ICCID of the SIM to change, $changes is a hashref
of wanted changes. If any of the given changes is impossible, the full update
is aborted and an exception is thrown. $changes must only contain fields that
are either required or optional for a SIM.

This method returns the updated SIM, or throws an exception if something
failed.

=cut

sub update_sim {
	my ($lim, $sim_iccid, $changes, $date) = @_;
	return update_object($lim, _sim_object_info(), $sim_iccid, $changes, $date);
}

=head3 delete_sim($lim | $dbh, $sim_iccid, [$date])

Delete a SIM. $date is the optional date of deletion; if not given, 'today' is
assumed.

=cut

sub delete_sim {
	my ($lim, $sim_iccid, $date) = @_;
	delete_object($lim, _sim_object_info(), $sim_iccid, $date);
}

1;