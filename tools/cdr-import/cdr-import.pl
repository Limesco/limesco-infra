#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use DateTime;
use LWP::UserAgent;
use HTTP::Cookies;
use Text::CSV;

=head1 cdr-import.pl

Usage: cdr-import.pl [infra-options] [--date YYYY-MM-DD]

Import all CDRs from SpeakUp. Takes URI base, username and password from config
file. If --date is given, retrieves CDRs for that date only; otherwise, make
sure that all dates are up-to-date by 24 hours after the day ended.

=cut

if(!caller) {
	my $date;
	my $lim = Limesco->new_from_args(\@ARGV, sub {
		my ($args, $iref) = @_;
		my $arg = $args->[$$iref];
		if($arg eq "--date") {
			$date = $args->[++$$iref];
		} else {
			return 0;
		}
	});

	$|++;
	my $config = $lim->speakup_config();
	my $uri_base = $config->{'uri_base'};
	my $username = $config->{'username'};
	my $password = $config->{'password'};
	print "Retrieving token...\n";
	my $token = get_speakup_login_token($lim, $uri_base, $username, $password);
	if($date) {
		print "Starting import for $date...\n";
		import_speakup_cdrs_by_day($lim, $uri_base, $token, $date);
	} else {
		my $today = DateTime->now(time_zone => 'local')->ymd;
		print "Starting import until $today...\n";
		import_speakup_cdrs($lim, $uri_base, $token, $today);
	}

	my @errors = get_cdr_import_errors($lim);
	if(@errors > 0) {
		print "Errors exist in the database after import:\n";
		foreach(@errors) {
			printf("CDR date %s, import at %s: %s\n", $_->{'cdr_date'}, $_->{'import_time'}, $_->{'error'});
		}
	}
}

=head2 Methods

=head3 get_speakup_login_token($lim, $uri_base, $username, $password)

=cut

sub get_speakup_login_token {
	my ($lim, $uri_base, $username, $password) = @_;
	$uri_base =~ s#/$##;

	my $cookie_jar = HTTP::Cookies->new;

	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->agent("Liminfra/0.0 ");
	$ua->cookie_jar($cookie_jar);
	my $response = $ua->post($uri_base . '/login', {
		username => $username,
		password => $password,
	});
	# Failed login gives response code 200
	if($response->code != 302) {
		die "SpeakUp login token request failed: " . $response->status_line;
	}
	my $login_cookie;
	$cookie_jar->scan(sub {
		my (undef, $key, $val) = @_;
		if($key eq "PHPSESSID") {
			$login_cookie = $val;
		} else {
			warn "$key=$val\n";
		}
	});
	if(!$login_cookie) {
		die "SpeakUp login token request failed: no session cookie found\n";
	}
	return $login_cookie;
}

=head3 get_cdr_import_dates($lim, $date_today)

=cut

sub get_cdr_import_dates {
	my ($lim, $date_today) = @_;

	{
		my ($t_year, $t_month, $t_day) = $date_today =~ /^(\d{4})-(\d\d)-(\d\d)$/;
		if(!$t_day) {
			die "Don't understand date syntax of today: $date_today";
		}
		$date_today = DateTime->new(year => $t_year, month => $t_month, day => $t_day);
	}

	my $dbh = $lim->get_database_handle();

	# Re-import all CDR dates where an error occured or the day was last refreshed
	# less than 48 hours after the day began

	my @days_to_update;

	my $sth = $dbh->prepare("SELECT cdr_date FROM cdrimports
		WHERE error IS NOT NULL
		OR (import_time <= cdr_date + '24 hours'::interval)");
	$sth->execute();
	while(my $row = $sth->fetchrow_arrayref) {
		push @days_to_update, $row->[0];
	}

	# Add all dates which are between last mentioned date and given 'today' date
	$sth = $dbh->prepare("SELECT cdr_date + '1 day'::interval AS startdate
		FROM cdrimports ORDER BY cdr_date DESC LIMIT 1");
	$sth->execute();
	my $date = $sth->fetchrow_arrayref;
	if(!$date) {
		warn "No dates in the cdrimports table yet, will start fetching today.\n";
		warn "If you want to start on another date, add a row to the cdrimports table\n";
		warn "with 'error' set to anything non-NULL, and I will start fetching there.\n";
		return ($date_today);
	}

	my ($year, $month, $day) = $date->[0] =~ /^(\d{4})-(\d\d)-(\d\d) /;
	$date = DateTime->new(year => $year, month => $month, day => $day);
	until($date > $date_today) {
		push @days_to_update, $date->ymd;
		$date->add(days => 1);
	}

	return @days_to_update;
}

=head3 retrieve_speakup_cdrs($lim, $uri_base, $token, $date, $callback)

=cut

sub retrieve_speakup_cdrs {
	my ($lim, $uri_base, $token, $date, $callback) = @_;

	my $uri = URI->new($uri_base);
	$uri_base =~ s#/$##;
	$date =~ s#^(\d{4})-(\d\d)-(\d\d)$#$3/$2/$1#;

	my $cookie_jar = HTTP::Cookies->new;
	$cookie_jar->set_cookie(1, "PHPSESSID", $token, $uri->path, $uri->host, $uri->port);

	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->agent("Liminfra/0.0 ");
	$ua->cookie_jar($cookie_jar);
	my $response = $ua->post($uri_base . '/partner/cdr', {
		period => 'day',
		startdate => $date,
		direction => 'both',
		deliverystatus => 'both',
		displaysip => 'yes',
		displayppc => 'yes',
		ratetype => 'normal',
		outputData => 'detailed',
		outputFormat => 'csv',
		extensionId => '',
		subscriptionAccountId => '',
		customerId => '',
		extensionNumber => '',
		submit => '',
	});
	if($response->code != 200) {
		die "SpeakUp CDR request for $date failed: " . $response->status_line;
	}
	if($response->content_type ne "text/csv") {
		die "SpeakUp CDR request for $date failed: content-type is not text/csv";
	}
	import_speakup_cdrs_from_csv($lim, $callback, ${$response->content_ref});
}

=head3 import_speakup_cdrs_from_csv($lim, $callback, $csv_content)

=cut

sub import_speakup_cdrs_from_csv {
	my ($lim, $callback, $content) = @_;
	my $csv = Text::CSV->new({binary => 1})
		or die "Cannot use CSV: ".Text::CSV->error_diag();
	
	my $i = 0;
	my @column_names;
	for(split /^/, $content) {
		++$i;
		if(!$csv->parse($_)) {
			die "Failed to parse line $i of response";
		}
		if(!@column_names) {
			@column_names = $csv->fields;
			next;
		}

		my @columns = $csv->fields;
		my $cdr = {};
		for (0 .. $#column_names) {
			$cdr->{$column_names[$_]} = $columns[$_];
		}

		try {
			$callback->($cdr);
		} catch {
			die "Callback failed during parsing of line $i of response: $_";
		};
	}
}

=head3 import_speakup_cdrs_by_day($lim, $uri_base, $token, $date)

=cut

sub import_speakup_cdrs_by_day {
	my ($lim, $uri_base, $token, $date) = @_;
	my $dbh = $lim->get_database_handle;

	$dbh->begin_work;
	my $is_new_row = 1;
	try {
		$dbh->do("LOCK TABLE cdrimports");

		my $sth = $dbh->prepare("SELECT cdr_date FROM cdrimports WHERE cdr_date=?");
		$sth->execute($date);
		my $row = $sth->fetchrow_arrayref;
		$is_new_row = !defined $row;

		retrieve_speakup_cdrs($lim, $uri_base, $token, $date, sub {
			my $su_cdr = $_[0];

			my ($service) = $su_cdr->{'cid'} =~ /@(sms|data)$/;
			$service ||= "voice";

			my $connected;
			if($service eq "voice") {
				$connected = ($su_cdr->{'sip'} =~ /^200/) ? 1 : 0;
			}

			my $units = $su_cdr->{'duration'};
			if(defined($connected) && !$connected) {
				$units = 0;
			}

			if($service eq "data" || ($service eq "sms" && $su_cdr->{'direction'} eq "in")) {
				delete $su_cdr->{'destination'};
			}

			my $cdr = {
				service => uc($service),
				call_id => $su_cdr->{'cid'},
				speakup_account => $su_cdr->{'account'},
				units => $units,
				connected => $connected,
				destination => ($su_cdr->{'destination'} || undef),
				direction => uc($su_cdr->{'direction'}),
			};
			for(qw/from to time/) {
				$cdr->{$_} = $su_cdr->{$_};
			}

			# does one already exist?
			# TODO: this should use leg and legreason
			$sth = $dbh->prepare("SELECT * FROM cdr WHERE call_id=? AND time=? AND \"from\"=? AND \"to\"=?");
			$sth->execute($cdr->{'call_id'}, $cdr->{'time'}, $cdr->{'from'}, $cdr->{'to'});
			if(my $row = $sth->fetchrow_hashref) {
				# then ours must be equal to the in-db one
				$cdr->{'id'} = $row->{'id'};

				# rows that may differ
				for(qw/pricing_id pricing_info computed_cost computed_price invoice_id/) {
					delete $cdr->{$_};
					delete $row->{$_};
				}

				foreach my $key (keys %$row) {
					if(!defined($cdr->{$key}) && !defined($row->{$key})) {
						# both undefined, that's fine but don't warn
					} elsif($key eq "destination"
					     && !defined($cdr->{$key})
					     && defined($row->{$key})
					     && $row->{$key} eq "") {
						# we made it undef, database has empty, that's ok
					} elsif(!defined($cdr->{$key})
					     || !defined($row->{$key})
					     || $cdr->{$key} ne $row->{$key}) {
						if($key eq "units") {
							if($cdr->{'units'} ne "") {
								# units can change when call was in progress and has now ended, so update it
								my $sth = $dbh->prepare("UPDATE cdr SET units=? WHERE id=?");
								$sth->execute($cdr->{'units'}, $row->{'id'});
								$row->{'units'} = $cdr->{'units'};
							}
						} elsif($key eq "connected") {
							# when call was not connected, then connected in the forwarded version,
							# we only import one cdr because we cannot distinguish between the two
							# set 'connected' to 1 for this call
							if(!$row->{'connected'}) {
								my $sth = $dbh->prepare("UPDATE cdr SET connected=true WHERE id=?");
								$sth->execute($row->{'id'});
								$row->{'connected'} = 1;
							}
						} else {
							# TODO: without a uniquely identifying field or combination of fields,
							# we can't be sure if this is the same CDR or one that looks a lot like
							# it; for now, warn about this situation and only one of two CDRs will be
							# inserted into the database
							warn sprintf("CDR-lookalike: id=%d/new, account=%s/%s, service=%s/%s, %s=%s/%s",
								$row->{'id'}, (map { $row->{$_}, $cdr->{$_} } qw/speakup_account service/),
								$key, $row->{$key}, $cdr->{$key});
						}
					}
				}
			} else {
				# add it to the database
				my @fields = keys %$cdr;
				my $fields = join('", "', @fields);
				my $placeholders = join ", ", (('?') x @fields);
				$sth = $dbh->prepare("INSERT INTO cdr (\"$fields\") VALUES ($placeholders)");
				$sth->execute(map { $cdr->{$_} } @fields);
			}
		});

		if($is_new_row) {
			$dbh->do("INSERT INTO cdrimports (import_time, cdr_date) VALUES ('now', ?)",
				undef, $date);
		} else {
			$dbh->do("UPDATE cdrimports SET error=NULL, import_time='now' WHERE cdr_date=?",
				undef, $date);
		}

		$dbh->commit;
	} catch {
		my $exception = $_ || "Unknown error";
		# TODO: this is a race condition, since the next importer may lock cdrimports
		# and finish an import succesfully while we wait to update the cdrimports table
		# with our error; this can be reliably fixed by taking a new lock, seeing if the
		# import_time changed, updating it if not, then committing
		$dbh->rollback;
		if($is_new_row) {
			$dbh->do("INSERT INTO cdrimports (error, import_time, cdr_date) VALUES (?, 'now', ?)",
				undef, $exception, $date);
		} else {
			$dbh->do("UPDATE cdrimports SET error=?, import_time='now' WHERE cdr_date=?",
				undef, $exception, $date);
		}
		# don't re-throw, exception is handled by inserting it into the database
	};
}

=head3 import_speakup_cdrs($lim, $uri_base, $token, $date_today)

=cut

sub import_speakup_cdrs {
	my ($lim, $uri_base, $token, $date_today) = @_;
	my @dates = get_cdr_import_dates($lim, $date_today);
	foreach(@dates) {
		import_speakup_cdrs_by_day($lim, $uri_base, $token, $_);
	}
}

=head3 get_cdr_import_errors($lim)

Return all import errors present in the database.

=cut

sub get_cdr_import_errors {
	my ($lim) = @_;
	my $dbh = $lim->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM cdrimports WHERE error IS NOT NULL");
	$sth->execute();
	my @errors;
	while(my $row = $sth->fetchrow_hashref) {
		push @errors, $row;
	}
	return @errors;
}

1;
