#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use DateTime;

# The SIM changer is necessary for updating the last-invoiced information in a
# SIM after generating an invoice for it
do '../sim-change/sim-change.pl';

=head1 invoice-generate.pl

Usage: invoice-generate.pl [infra-options] { --all-accounts | --account <accountID> [ --account <accountID> [ ... ] ] } --date <date>

This tool is used to generate an invoice for an account with a given date. You can give
multiple --account flags or just give --all-accounts.

=cut

if(!caller) {
	my $all_accounts = 0;
	my @account_ids;
	my $date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--account") {
			push @account_ids, $args->[++$$iref];
		} elsif($arg eq "--all-accounts") {
			$all_accounts = 1;
		} elsif($arg eq "--date") {
			$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	if(!$all_accounts && !@account_ids) {
		die "--account option is required (or give --all-accounts)\n";
	}
	if(!$date) {
		die "--date option is required\n";
	}

	if($all_accounts) {
		@account_ids = get_all_active_account_ids($lim, $date);
	} else {
		for(my $i = 0; $i < @account_ids; ++$i) {
			if($account_ids[$i] !~ /^\d+$/) {
				# Not just numbers, try to convert it to an account ID
				$account_ids[$i] = $lim->get_account_like($account_ids[$i])->{'id'};
			}
		}
	}

	my ($year, $month, $day) = $date =~ /^(\d{4})-(\d\d)-(\d\d)$/;
	if(!$day) {
		die "Could not parse date (it should be YYYY-MM-DD): $date\n";
	}
	$date = DateTime->new(year => $year, month => $month, day => $day);

	foreach my $account_id (@account_ids) {
		try {
			my $invoice_id = generate_invoice($lim, $account_id, $date);
			if(!$invoice_id) {
				print "Nothing to invoice for account $account_id.\n";
			} else {
				print "Invoice generated for account $account_id: $invoice_id\n";
			}
		} catch {
			print "Invoice generation for account $account_id failed: $_";
		};
	}
}

=head2 Methods

=head3 get_all_active_account_ids($lim, [$date])

Returns an array of active account IDs (those where $date falls within the 'period'
field). If not given, $date is 'today'.

=cut

sub get_all_active_account_ids {
	my ($lim, $date) = @_;
	my $dbh = $lim->get_database_handle();
	$date ||= 'today';

	my @accounts;
	my $sth = $dbh->prepare("SELECT id FROM account WHERE period @> ?::date ORDER BY id ASC");
	$sth->execute($date);
	while(my $row = $sth->fetchrow_arrayref) {
		push @accounts, $row->[0];
	}
	return @accounts;
}

=head3 find_next_invoice_id($dbh, $currentdate, [$invoice_digits])

Given a database handle, returns the next invoice ID to use. You should get an
exclusive lock on the invoice table on this handle before calling this method.
The currentdate member should be a DateTime-compatible object, of which the
year member will be used to form the year part of the invoice ID (e.g.
'14C000001' for the first invoice in 2014).

=cut

sub find_next_invoice_id {
	my ($dbh, $currentdate, $invoice_digits) = @_;
	$invoice_digits ||= 6;

	my $currentyear = $currentdate->year % 100;
	my $first_invoice_id = $currentyear . 'C' . ('0' x ($invoice_digits-1)) . '1';

	my $sth = $dbh->prepare("SELECT id FROM invoice ORDER BY id DESC LIMIT 1");
	$sth->execute();
	my $row = $sth->fetchrow_arrayref();
	if(!$row) {
		return $first_invoice_id;
	}

	my $last_invoice_year = substr($row->[0], 0, 2);
	if($last_invoice_year < $currentyear) {
		return $first_invoice_id;
	} elsif($last_invoice_year > $currentyear) {
		die "Last invoice was generated after this year (" . $row->[0] . " > " . $currentyear
			. "), cannot return the next invoice ID\n";
	}

	my $last_invoice_num = substr($row->[0], 3);
	return sprintf("%02dC%0${invoice_digits}d", $currentyear, $last_invoice_num + 1);
}

=head3 first_day_of_next_month($date)

Takes a DateTime-compatible object, and returns a DateTime-compatible object
that points to the first day of the month in the given object. E.g. it turns
2014-02-20 into 2014-03-01.

=cut

sub first_day_of_next_month {
	my $date = $_[0]->clone;
	$date->set(day => 1);
	$date->add(months => 1);
	return $date;
}

=head3 last_day_of_this_month($date)

Takes a DateTime-compatible object, and returns a DateTime-compatible object
that points to the last day of the month in the given object. E.g. it turns
2014-02-20 into 2014-02-28.

=cut

sub last_day_of_this_month {
	my $date = $_[0]->clone;
	$date->set(day => 1);
	$date->add(months => 1);
	$date->subtract(days => 1);
	return $date;
}

=head3 get_partial_month_factor($start_date, $end_date)

Returns the "partial month factor" between the given dates. They must both fall
in the same month, and end_date must be higher than start_date. The "partial
month factor" is the number of days between the given dates divided by the
number of days in the month. I.e. if the start_date is the first day of the
month and the end_date is the last day, this method returns 1; if the dates
indicate half the month, the method returns 0.5.

=cut

sub get_partial_month_factor {
	my ($start_date, $end_date) = @_;
	if($start_date->year != $end_date->year
	|| $start_date->month != $end_date->month) {
		die("get_partial_month_factor dates must be in the same month");
	}

	my $first_date = $start_date->clone->set(day => 1);
	my $last_date = $first_date->clone->add(months => 1)->subtract(days => 1);
	return ($end_date->day - $start_date->day) / ($last_date->day - $first_date->day);
}


=head3 get_sim_contract_start_date($dbh, $iccid)

Use the given database handle to figure out when a SIM contract started.
Returns a DateTime-compatible object pointing to sometime during the day the
SIM contract started. Returns undef if the SIM contract has not yet started.

=cut

sub get_sim_contract_start_date {
	my ($dbh, $iccid) = @_;
	my $sth = $dbh->prepare("SELECT lower(period) FROM sim WHERE iccid=? AND state='ACTIVATED' ORDER BY period ASC LIMIT 1");
	$sth->execute($iccid);
	my $sim = $sth->fetchrow_arrayref();
	if(!$sim) {
		return;
	}
	my ($startyear, $startmonth, $startday) = $sim->[0] =~ /^(\d{4})-(\d\d)-(\d\d)$/;
	if(!$startday) {
		die "Could not parse SIM activation date: " . $sim->[0];
	}
	return DateTime->new(
		year => $startyear,
		month => $startmonth,
		day => $startday,
	);
}

=head3 get_sim_contract_end_date($dbh, $iccid);

Use the given database handle to figure out when a SIM contract ended. Returns
a DateTime-compatible object pointing to sometime during the last day of the
SIM contract. Returns undef if the SIM contract has not yet ended.

=cut

sub get_sim_contract_end_date {
	my ($dbh, $iccid) = @_;
	my $sth = $dbh->prepare("SELECT lower(period) FROM sim WHERE iccid=? AND state='DISABLED' ORDER BY period ASC LIMIT 1");
	$sth->execute($iccid);
	my $sim = $sth->fetchrow_arrayref();
	if(!$sim) {
		return;
	}
	my ($endyear, $endmonth, $endday) = $sim->[0] =~ /^(\d{4})-(\d\d)-(\d\d)$/;
	if(!$endday) {
		die "Could not parse SIM deactivation date: " . $sim->[0];
	}
	return DateTime->new(
		year => $endyear,
		month => $endmonth,
		day => $endday,
	)->subtract(days => 1);
}

=head3 phonenumber_to_apn_type_in_month($dbh, $account_id, $number, $yearmonth)

Take a phone number, of which we know it belongs to a certain account ID, and try
to determine what APN type it had during a month. This works for SIMs that were
already active before this month, but also for SIMs that became active during the
month.

=cut

sub phonenumber_to_apn_type_in_month {
	my ($dbh, $account_id, $number, $yearmonth) = @_;
	my ($year, $month) = $yearmonth =~ /^(\d{4})-(\d\d)$/;
	if(!$year || !$month) {
		die "Unrecognised yearmonth format: $yearmonth";
	}

	# Find the first occurance of an APN for this SIM
	# TODO: this is possible in a non-iterative manner by splitting the
	# subquery off and accepting periods during the given month.
	my $date = DateTime->new(year => $year, month => $month, day => 1);
	my $sth = $dbh->prepare("SELECT data_type FROM sim WHERE owner_account_id=? AND period @> ?::date AND iccid=
		(SELECT sim_iccid FROM phonenumber WHERE phonenumber.phonenumber=? AND (period @> ?::date "
		# Also accept if the number was valid for this SIM one day
		# before the CDR occured. This fixes the situation where a
		# number is ported on the first of the month, and data CDRs
		# occuring just before the port have a phone number that is
		# never valid during the month. With this fix, phone numbers
		# that are valid the last day of the month before are also
		# accepted. This introduces the risk of a number being reused
		# within a day for another SIM within the same account with a
		# different data type, which is an extreme corner case.
		. " OR period @> (?::date - '1 day'::interval)::date))");
	until($date->month != $month) {
		$sth->execute($account_id, $date->ymd, $number, $date->ymd, $date->ymd);
		my $sim = $sth->fetchrow_hashref;
		if($sim && $sth->fetchrow_hashref) {
			die "Could not invoice data CDR: it belongs to multiple SIMs\n";
		}
		if($sim) {
			return $sim->{'data_type'};
		}
		$date->add(days => 1);
	}
	die sprintf("Could not invoice data CDR for account %d, month %s, phone number %s: no valid data contract found",
		$account_id, $yearmonth, $number);
}

=head3 generate_invoice($lim, $account_id, $date)

Create an invoice in the database for account $account_id, with invoice date
$date (as a DateTime-compatible object). Returns the invoice ID if an invoice was
created, undef if there was nothing to invoice; throws an exception if invoice
generation failed for some reason.

=cut

sub generate_invoice {
	my ($lim, $account_id, $date) = @_;
	my $dbh = $lim->get_database_handle();

	# TODO: this could go in the pricing table?
	my $TAX_RATE = 0.21;
	my $SIM_CARD_ACTIVATION_PRICE = 34.7107;
	my $SIM_NO_DATA_MONTHLY_PRICE =      2.8926;
	my $SIM_APN_500_MB_MONTHLY_PRICE =  13.8843;
	my $SIM_APN_2000_MB_MONTHLY_PRICE = 23.8843;
	my $LIQUID_PRICING_PER_SIM =  3.0992;
	my $DATA_USAGE_TIER1_PER_MB = 0.0248;
	my $DATA_USAGE_TIER2_PER_MB = 0.0165;
	my $DATA_USAGE_TIER3_PER_MB = 0.0083;
	my $DATA_USAGE_OUT_OF_BUNDLE_PER_MB = 0.1653;

	my @itemlines;

	my $normal_itemline = sub {
		my ($invoice_id, $description, $count, $price, $service) = @_;
		push @itemlines, {
			type => "NORMAL",
			invoice_id => $invoice_id,
			description => $description,
			taxrate => $TAX_RATE,
			rounded_total => sprintf("%.2f", $count * $price),
			item_price => $price,
			item_count => $count,
			service => $service,
		};
	};

	my $duration_itemline = sub {
		my ($invoice_id, $description, $rounded_total, $number_of_calls, $number_of_seconds, $pricing) = @_;
		push @itemlines, {
			type => "DURATION",
			invoice_id => $invoice_id,
			description => $description,
			taxrate => $TAX_RATE,
			number_of_calls => $number_of_calls,
			number_of_seconds => $number_of_seconds,
			price_per_call => $pricing->{'price_per_line'},
			price_per_minute => $pricing->{'price_per_unit'} * 60,
			rounded_total => sprintf("%.2f", $rounded_total),
			service => $pricing->{'service'},
		};
	};

	try {
		$dbh->begin_work;

		# Prevent writing to the tables we read from, and
		# reading from the tables we write
		$dbh->do("LOCK TABLE account IN SHARE MODE;");
		$dbh->do("LOCK TABLE cdr IN SHARE MODE;");
		$dbh->do("LOCK TABLE invoice IN EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE pricing IN SHARE MODE;");
		$dbh->do("LOCK TABLE sim IN ROW EXCLUSIVE MODE;");
		$dbh->do("LOCK TABLE speakup_account IN SHARE MODE;");

		# Find the next invoice number: we have an exclusive lock
		# on the table, so if we find one here it will be ours
		my $invoice_id = find_next_invoice_id($dbh, $date);

		# Add activation costs for all unactivated SIMs
		my $sth = $dbh->prepare("SELECT * FROM sim WHERE owner_account_id=? AND activation_invoice_id IS NULL AND state != 'DISABLED' AND period @> ?::date");
		$sth->execute($account_id, $date);
		while(my $sim = $sth->fetchrow_hashref) {
			# TODO: this calls die() when $date is already a 'history record' (i.e. there
			# is a planned update for this SIM after $date); this can be made more robust
			# by implementing 'history changing support' in update_sim so that it is able
			# to update all records starting with $date into infinity.
			update_sim($dbh, $sim->{'iccid'}, {
				activation_invoice_id => $invoice_id,
			}, $date);
			$normal_itemline->($invoice_id, "Activatie SIM-kaart", 1, $SIM_CARD_ACTIVATION_PRICE);
		}

		# Add monthly SIM costs as of invoice date
		$sth = $dbh->prepare("SELECT * FROM sim WHERE owner_account_id=? AND period @> ?::date AND state='ACTIVATED'");
		$sth->execute($account_id, $date->ymd);
		while(my $sim = $sth->fetchrow_hashref) {
			# Start of monthly cost invoicing: first of month after last invoiced month
			# If SIM was never invoiced, it's the activation date
			my $monthly_cost_start;
			if($sim->{'last_monthly_fees_invoice_id'}) {
				my ($year, $month, $day) = $sim->{'last_monthly_fees_month'} =~ /^(\d{4})-(\d\d)-(\d\d)$/;
				if($year == $date->year && $month == $date->month) {
					# SIM contract was already invoiced this month
					next;
				}
				my $dt = DateTime->new(year => $year, month => $month, day => $day);
				$monthly_cost_start = first_day_of_next_month($dt);
			} else {
				$monthly_cost_start = get_sim_contract_start_date($dbh, $sim->{'iccid'});
				if(!$monthly_cost_start) {
					# SIM contract hasn't started yet
					next;
				}
			}

			# End of monthly cost invoicing: deactivation date / last of this month
			my $monthly_cost_end = get_sim_contract_end_date($dbh, $sim->{'iccid'});
			if(!$monthly_cost_end) {
				# SIM contract hasn't ended yet, so end of this month
				$monthly_cost_end = last_day_of_this_month($date);
			}

			if($monthly_cost_end < $monthly_cost_start) {
				die "SIM ended before it began, that should be impossible: " . $monthly_cost_end->ymd . " < " . $monthly_cost_start->ymd;
			}

			my $invoicing_month = $monthly_cost_start;
			until($invoicing_month > $monthly_cost_end) {
				my $sim_monthly_price_description;
				my $sim_monthly_price;
				if($sim->{'data_type'} eq "APN_500MB") {
					$sim_monthly_price_description = "Vaste kosten (500 MB-bundel)";
					$sim_monthly_price = $SIM_APN_500_MB_MONTHLY_PRICE;
				} elsif($sim->{'data_type'} eq "APN_2000MB") {
					$sim_monthly_price_description = "Vaste kosten (2000 MB-bundel)";
					$sim_monthly_price = $SIM_APN_2000_MB_MONTHLY_PRICE;
				} elsif($sim->{'data_type'} eq "APN_NODATA") {
					$sim_monthly_price_description = "Vaste kosten";
					$sim_monthly_price = $SIM_NO_DATA_MONTHLY_PRICE;
				} else {
					die "Unknwon data type: " . $sim->{'data_type'};
				}

				# TODO: end_of_this_period is until the next change of APN and
				# should use the APN in this period to compute description and price
				my $end_of_this_period = last_day_of_this_month($invoicing_month->clone);
				if($end_of_this_period > $monthly_cost_end) {
					$end_of_this_period = $monthly_cost_end->clone;
				}

				my $factor = get_partial_month_factor($invoicing_month, $end_of_this_period);
				my $description = $sim_monthly_price_description . "\n";
				$description .= $invoicing_month->ymd . " - " . $end_of_this_period->ymd;
				$normal_itemline->($invoice_id, $description, 1, $sim_monthly_price * $factor);

				# Liquid Pricing
				if(!$sim->{'exempt_from_cost_contribution'}) {
					$description = "Liquid Pricing\n" . $invoicing_month->ymd . " - " . $end_of_this_period->ymd;
					$normal_itemline->($invoice_id, $description, 1, $LIQUID_PRICING_PER_SIM * $factor);
				}

				$invoicing_month = $end_of_this_period->add(days => 1);
			}
			# TODO: this calls die() when $date is already a 'history record' (i.e. there
			# is a planned update for this SIM after $date); this can be made more robust
			# by implementing 'history changing support' in update_sim so that it is able
			# to update all records starting with $date into infinity.
			update_sim($dbh, $sim->{'iccid'}, {
				last_monthly_fees_invoice_id => $invoice_id,
				last_monthly_fees_month => $date,
			}, $date);
		}

		my %pricings;
		my $get_pricing = sub {
			my $pid = $_[0];
			if(!$pricings{$pid}) {
				my $sth = $dbh->prepare("SELECT * FROM pricing WHERE id=?");
				$sth->execute($pid);
				$pricings{$pid} = $sth->fetchrow_hashref;
			}
			if(!$pricings{$pid}) {
				die "Failed to generate invoice: failed to get pricing ID $pid\n";
			}
			return $pricings{$pid};
		};

		my $beyond_cdr_period = $date->clone->set(day => 1);
		# Group CDRs by pricing ID
		$sth = $dbh->prepare("SELECT * FROM cdr
			FULL JOIN speakup_account ON lower(cdr.speakup_account)=lower(speakup_account.name)
			WHERE speakup_account.period @> cdr.time::date
			AND speakup_account.account_id=?
			AND cdr.time < ?::date
			AND cdr.invoice_id IS NULL
			AND cdr.pricing_info IS NOT NULL
			ORDER BY cdr.time ASC;");
		$sth->execute($account_id, $beyond_cdr_period);
		my %cdr_per_pricing_rule;
		while(my $cdr = $sth->fetchrow_hashref) {
			$cdr_per_pricing_rule{$cdr->{'pricing_id'}} ||= [];
			push @{$cdr_per_pricing_rule{$cdr->{'pricing_id'}}}, $cdr;
			$dbh->do("UPDATE cdr SET invoice_id=? WHERE id=?", undef, $invoice_id, $cdr->{'id'});
		}

		my %month_to_number_to_data_cdrs;
		foreach(reverse sort keys %cdr_per_pricing_rule) {
			my $pricing = $get_pricing->($_);

			if($pricing->{'service'} eq "SMS") {
				my $num = @{$cdr_per_pricing_rule{$_}};
				my $price_per_line = $pricing->{'price_per_line'};
				my $description = sprintf("%s", $pricing->{'description'});
				$normal_itemline->($invoice_id, $description, $num, $price_per_line, $pricing->{'service'});
			} elsif($pricing->{'service'} eq "VOICE") {
				my $sum_units = 0;
				my $sum_prices = 0;
				my $number_of_lines = 0;
				foreach(@{$cdr_per_pricing_rule{$_}}) {
					$number_of_lines += 1;
					$sum_units += $_->{'units'};
					$sum_prices += $_->{'computed_price'};
				}
				my $description = "Bellen " . $pricing->{'description'};

				if($pricing->{'hidden'} && $sum_prices == 0) {
					# hide this itemline
				} else {
					$duration_itemline->($invoice_id, $description, $sum_prices, $number_of_lines, $sum_units, $pricing);
				}
			} elsif($pricing->{'service'} eq "DATA") {
				foreach my $cdr (@{$cdr_per_pricing_rule{$_}}) {
					my ($month) = $cdr->{'time'} =~ /^(\d{4}-\d{2})-\d{2} [\d:]+$/;
					if(!$month) {
						die "Could not parse CDR timestamp: " . $cdr->{'time'};
					}
					my $number = $cdr->{'from'};
					$month_to_number_to_data_cdrs{$month} ||= {};
					$month_to_number_to_data_cdrs{$month}{$number} ||= [];
					push @{$month_to_number_to_data_cdrs{$month}{$number}}, $cdr;
				}
			} else {
				die "Unknown pricing service referenced by CDR pricing information\n";
			}
		}

		# Process data CDRs per month
		# APN_x at the beginning of the month counts for that month
		# make a sum per month; if it's a bundle make one item line with inside and outside usage
		# if it's not a bundle make an itemline with tiered usage
		foreach my $month (keys %month_to_number_to_data_cdrs) {
			foreach my $number (keys %{$month_to_number_to_data_cdrs{$month}}) {
				my $apn = phonenumber_to_apn_type_in_month($dbh, $account_id, $number, $month);
				my $sum = 0;
				foreach(@{$month_to_number_to_data_cdrs{$month}{$number}}) {
					$sum += $_->{'units'};
				}
				if($apn eq "APN_500MB" || $apn eq "APN_2000MB") {
					my $in_bundle_usage = $sum;
					my $out_bundle_usage = 0;
					my $limit = $apn eq "APN_500MB" ? 500 * 1024 : 2000 * 1024;
					if($in_bundle_usage > $limit) {
						$out_bundle_usage = $in_bundle_usage - $limit;
						$in_bundle_usage = $limit;
					}
					my $description = sprintf("%s (bundel %s)", "Data Nederland", $month);
					$normal_itemline->($invoice_id, $description, $in_bundle_usage, 0, "DATA");
					if($out_bundle_usage > 0) {
						$description = sprintf("%s (buiten bundel %s, SIM %s)", "Data Nederland", $month, $number);
						$normal_itemline->($invoice_id, $description, $out_bundle_usage, $DATA_USAGE_OUT_OF_BUNDLE_PER_MB / 1000, "DATA");
					}
				} elsif($apn eq "APN_NODATA") {
					my ($tier1, $tier2, $tier3) = (0, 0, 0);
					if($sum > 1000 * 1024) {
						$tier1 = 500 * 1024;
						$tier2 = 500 * 1024;
						$tier3 = $sum - 1000 * 1024;
					} elsif($sum > 500 * 1024) {
						$tier1 = 500 * 1024;
						$tier2 = $sum - 500 * 1024;
					} else {
						$tier1 = $sum;
					}
					$normal_itemline->($invoice_id, "Dataverbruik onder 500 MB", $tier1, $DATA_USAGE_TIER1_PER_MB / 1000, "DATA");
					if($tier2 > 0 || $tier3 > 0) {
						$normal_itemline->($invoice_id, "Dataverbruik onder 1000 MB", $tier2, $DATA_USAGE_TIER2_PER_MB / 1000, "DATA");
						if($tier3 > 0) {
							$normal_itemline->($invoice_id, "Dataverbruik boven 1000 MB", $tier3, $DATA_USAGE_TIER3_PER_MB / 1000, "DATA");
						}
					}
				} else {
					die "Unknown data APN\n";
				}
			}
		}

		# Queued itemlines
		my @queued_itemlines;
		$sth = $dbh->prepare("SELECT * FROM invoice_itemline WHERE queued_for_account_id=?");
		$sth->execute($account_id);
		while(my $row = $sth->fetchrow_hashref) {
			push @queued_itemlines, $row;
		}

		if(@itemlines == 0 && @queued_itemlines == 0) {
			# Nothing to invoice
			$dbh->rollback;
			return;
		}

		my $invoice_sum = 0;
		# TODO: tax per taxrate
		my $tax = 0;
		foreach(@itemlines, @queued_itemlines) {
			$invoice_sum += $_->{'rounded_total'};
			$tax += $_->{'rounded_total'} * $_->{'taxrate'};
		}

		# Tax itemline
		push @itemlines, {
			type => "TAX",
			invoice_id => $invoice_id,
			description => "Tax",
			taxrate => $TAX_RATE,
			base_amount => $invoice_sum,
			rounded_total => $tax,
			item_price => $tax,
			item_count => 1,
		};

		$dbh->do("INSERT INTO invoice (id, account_id, currency, date, creation_time, rounded_without_taxes, rounded_with_taxes)
				VALUES (?, ?, ?, ?, now()::timestamp, ?, ?)", undef, $invoice_id, $account_id, 'EUR', $date, $invoice_sum, $invoice_sum + $tax);

		foreach my $il (@itemlines) {
			$dbh->do("INSERT INTO invoice_itemline (type, invoice_id, description, taxrate, rounded_total, base_amount, item_price,
				item_count, number_of_calls, number_of_seconds, price_per_call, price_per_minute, service) VALUES (?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?);", undef, map { $il->{$_} } qw/type invoice_id description taxrate rounded_total base_amount item_price
					item_count number_of_calls number_of_seconds price_per_call price_per_minute service/);
		}
		foreach my $il (@queued_itemlines) {
			$dbh->do("UPDATE invoice_itemline SET queued_for_account_id=NULL, invoice_id=? WHERE id=?", undef,
				$invoice_id, $il->{'id'});
		}

		$dbh->commit;
		return $invoice_id;
	} catch {
		$dbh->rollback;
		die $_;
	};

	# TODO: Dump information on included CDRs from earlier than last month
	# TODO: Dump information on unpriced CDRs
}

1;
