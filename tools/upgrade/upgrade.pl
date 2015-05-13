#!/usr/bin/perl
use strict;
use warnings;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;

=head1 upgrade.pl

Usage: upgrade.pl [infra-options]

This tool can be used to initialize or upgrade a database schema. It connects
to the database, detects the current schema version, and upgrades it to the
newest version.

=cut

if(!caller) {
	my $lim = Limesco->new_from_args(\@ARGV);
	my $version = get_current_schema_version($lim);
	my $latest_version = get_latest_schema_version();
	if(!$version) {
		if(ask_confirmation("The database seems uninitialized. Proceed to initialize to latest schema version $latest_version?")) {
			initialize_database($lim);
			print "Done initializing database.\n";
		} else {
			print "Cancel initialization.\n";
			exit 1;
		}
	} elsif($version == $latest_version) {
		print "The current database schema version is $version, which is the latest version. No upgrading needed.\n";
	} elsif($version > $latest_version) {
		print "The current database schema version is $version, which is newer than the latest version this tool can understand ($latest_version). Please upgrade your repository.\n";
		exit 1;
	} elsif(ask_confirmation("The current database schema version is $version, which is upgradable to the latest schema version $latest_version. Proceed to upgrade?")) {
		upgrade_database($lim, $version);
		print "Done upgrading database.\n";
	} else {
		print "Cancel upgrade.\n";
		exit 1;
	}
	exit 0;
}

=head2 Methods

=head3 get_current_schema_version($lim, [$current_date])

Returns the current schema version in the database, or undefined if none could be found.

The current schema version is stored in the "meta" table. If that table doesn't
exist, the current schema version is 0. Otherwise, the schema_version column of
the newest row is returned, which must have a period starting before
$current_date and not ending. If any of these preconditions is not true, or
more than one row matches these preconditions, this method throws.

=cut

sub get_current_schema_version {
	my ($lim, $current_date) = @_;
	my $dbh = $lim->get_database_handle();
	if(!$current_date) {
		my (undef, undef, undef, $mday, $mon, $yr) = localtime(time);
		$current_date = sprintf("%04d-%02d-%02d", $yr + 1900, $mon + 1, $mday);
	}

	# Check if the table exists
	my $sth = $dbh->prepare("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='meta')");
	$sth->execute();
	my $result = $sth->fetchrow_arrayref();
	if(!$result->[0]) {
		return 0;
	}

	# It exists, so it must have exactly one valid row
	$sth = $dbh->prepare("SELECT period, schema_version FROM meta WHERE upper_inf(period)");
	$sth->execute();
	$result = $sth->fetchrow_arrayref();
	if(!$result) {
		die "No rows match preconditions for get_current_schema_version";
	}
	if($sth->fetchrow_arrayref()) {
		die "More than one row matches preconditions for get_current_schema_version";
	}

	if(!daterange_in($result->[0], $current_date)) {
		die "Newest meta row is not valid yet, cannot determine current schema version";
	}

	return $result->[1];
}

sub daterange_in {
	my ($daterange, $date) = @_;
	my ($daterange_start, $daterange_end);
	# only allow inclusive start and exclusive end: [date, date) and [date,)
	if($daterange !~ /^\[\s*(\d{4}-\d\d-\d\d)\s*,(?:\s*(\d{4}-\d\d-\d\d)?\)|\))$/) {
		die "Could not parse invalid date range: $daterange";
	}
	$daterange_start = $1;
	$daterange_end = $2;
	if($date lt $daterange_start) {
		return 0;
	}
	if(defined($daterange_end) && $date ge $daterange_end) {
		return 0;
	}
	return 1;
}

=head3 get_latest_schema_version()

Returns the latest schema version supported by this tool.

=cut

sub get_latest_schema_version {
	return 11;
}

=head3 update_schema_version($lim, $dbh, $version)

Update the in-database schema version to the given parameter. Only for use
inside the upgrade tool.

=cut

sub update_schema_version {
	my ($lim, $dbh, $version) = @_;

	$dbh->do("UPDATE meta SET period=daterange(lower(period), 'today') where upper_inf(period);");
	$dbh->do("INSERT INTO meta (period, schema_version) VALUES ('[today,)', ?)", undef, $version);
}

=head3 ask_confirmation($question)

Utility method. Asks the question until yes or no was answered, then returns 1 if yes, 0 if no.

=cut

sub ask_confirmation {
	my ($question) = @_;
	while(1) {
		print $question . "\nYes or no? [yn] ";
		my $answer = lc(<STDIN>);
		1 while chomp $answer;
		if($answer eq "y" || $answer eq "yes") {
			return 1;
		} elsif($answer eq "n" || $answer eq "no") {
			return 0;
		} else {
			print "Incomprehensible reply. Try again.\n\n";
		}
	}
}

=head3 initialize_database($lim)

Initializes the database to the schema version returned by get_latest_schema_version().
Assumes no tables already exist in the database.

=cut

sub initialize_database {
	my ($lim) = @_;
	my $dbh = $lim->get_database_handle();
	try {
		$dbh->begin_work();
		# Disable CREATE TABLE notices for this transaction only
		$dbh->do("SET LOCAL client_min_messages='WARNING';");

		$dbh->do("CREATE EXTENSION IF NOT EXISTS btree_gist;");

		$dbh->do("CREATE TABLE meta (
			period DATERANGE NOT NULL,
			schema_version INT NOT NULL,
			EXCLUDE USING gist (period WITH &&)
		);");
		$dbh->do("INSERT INTO meta (period, schema_version) values ('[today,)', ?)", undef, get_latest_schema_version());

		# Monetary types
		$dbh->do("CREATE TYPE currency AS ENUM('EUR');");
		$dbh->do("CREATE DOMAIN money2 AS NUMERIC(12, 2);");
		$dbh->do("CREATE DOMAIN money8 AS NUMERIC(12, 8);");

		# A domain AS TEXT CONSTRAINT max_length is the fastest to
		# insert, fastest to search through, and fastest to change the
		# length constraint. We place a CONSTRAINT on our database
		# mostly to prevent these text from becoming insanely large.
		# See benchmark: http://www.depesz.com/2010/03/02/charx-vs-varcharx-vs-varchar-vs-text/
		$dbh->do("CREATE DOMAIN shorttext AS TEXT CONSTRAINT max_length CHECK (LENGTH(VALUE) <= 100);");
		$dbh->do("CREATE DOMAIN longtext AS TEXT CONSTRAINT max_length CHECK (LENGTH(VALUE) <= 400);");

		$dbh->do("CREATE SEQUENCE account_id_seq;");

		$dbh->do("CREATE TABLE account (
			id INTEGER NOT NULL,
			period DATERANGE NOT NULL,
			company_name SHORTTEXT NULL,
			first_name SHORTTEXT NOT NULL,
			last_name SHORTTEXT NOT NULL,
			street_address SHORTTEXT NOT NULL,
			postal_code SHORTTEXT NOT NULL,
			city SHORTTEXT NOT NULL,
			email SHORTTEXT NOT NULL,
			password_hash SHORTTEXT,
			admin BOOLEAN NOT NULL DEFAULT FALSE,

			contribution MONEY8 NOT NULL,
			last_contribution_month DATE,

			PRIMARY KEY (id, period),
			EXCLUDE USING gist (id WITH =, period WITH &&)
		);");

		$dbh->do("ALTER SEQUENCE account_id_seq OWNED BY account.id;");

		$dbh->do("CREATE TABLE speakup_account (
			name SHORTTEXT NOT NULL, -- NOTE: always do case insensitive comparisons when checking this (TODO: exclusion constraint should be case insensitive)
			period DATERANGE NOT NULL,
			account_id INTEGER,
			PRIMARY KEY (name, period),
			EXCLUDE USING gist (name WITH =, period WITH &&),
			EXCLUDE USING gist (account_id WITH =, period WITH &&)
		);");

		$dbh->do("CREATE TYPE simstate AS ENUM('STOCK', 'ALLOCATED', 'ACTIVATION_REQUESTED', 'ACTIVATED', 'DISABLED');");
		$dbh->do("CREATE TYPE simdatatype AS ENUM('APN_NODATA', 'APN_500MB', 'APN_2000MB');");
		$dbh->do("CREATE TYPE callconnectivitytype AS ENUM('OOTB', 'DIY');");
		$dbh->do('CREATE DOMAIN invoiceid AS TEXT CHECK(VALUE ~ \'^\d\dC\d{6}$\');');
		$dbh->do("CREATE TYPE itemlinetype AS ENUM('NORMAL', 'DURATION', 'TAX');");

		$dbh->do("CREATE TABLE invoice (
			id INVOICEID PRIMARY KEY NOT NULL,
			account_id INTEGER NOT NULL,
			currency CURRENCY NOT NULL DEFAULT 'EUR',
			date DATE NOT NULL,
			creation_time TIMESTAMP DEFAULT 'now',
			rounded_without_taxes MONEY2 NOT NULL,
			rounded_with_taxes MONEY2 NOT NULL
		);");

		$dbh->do("CREATE TABLE sim (
			iccid SHORTTEXT NOT NULL,
			period DATERANGE NOT NULL,
			state SIMSTATE NOT NULL,
			puk SHORTTEXT NOT NULL,
			owner_account_id INTEGER,
			CHECK (state='STOCK' OR owner_account_id IS NOT NULL),
			CHECK (state!='STOCK' OR owner_account_id IS NULL),
			data_type SIMDATATYPE,
			CHECK (state='STOCK' OR data_type IS NOT NULL),
			CHECK (state!='STOCK' OR data_type IS NULL),
			exempt_from_cost_contribution BOOLEAN,
			CHECK (state='STOCK' OR exempt_from_cost_contribution IS NOT NULL),
			CHECK (state!='STOCK' OR exempt_from_cost_contribution IS NULL),

			activation_invoice_id INVOICEID REFERENCES invoice(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			CHECK (state!='STOCK' OR activation_invoice_id IS NULL),
			last_monthly_fees_invoice_id INVOICEID REFERENCES invoice(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			CHECK (state!='STOCK' OR last_monthly_fees_invoice_id IS NULL),
			last_monthly_fees_month DATE,
			CHECK (last_monthly_fees_invoice_id IS NULL OR last_monthly_fees_month IS NOT NULL),
			CHECK (last_monthly_fees_invoice_id IS NOT NULL OR last_monthly_fees_month IS NULL),

			call_connectivity_type CALLCONNECTIVITYTYPE,
			CHECK (state='STOCK' OR call_connectivity_type IS NOT NULL),
			CHECK (state!='STOCK' OR call_connectivity_type IS NULL),

			sip_realm SHORTTEXT,
			CHECK (state!='STOCK' OR sip_realm IS NULL),
			sip_username SHORTTEXT,
			CHECK (state!='STOCK' OR sip_username IS NULL),
			sip_authentication_username SHORTTEXT,
			CHECK (state!='STOCK' OR sip_authentication_username IS NULL),
			sip_password SHORTTEXT,
			CHECK (state!='STOCK' OR sip_password IS NULL),
			sip_uri LONGTEXT,
			CHECK (state!='STOCK' OR sip_uri IS NULL),
			sip_expiry INTEGER,
			CHECK (state!='STOCK' OR sip_expiry IS NULL),
			sip_trunk_password SHORTTEXT
			CHECK (state!='STOCK' OR sip_trunk_password IS NULL),

			PRIMARY KEY (iccid, period),
			EXCLUDE USING gist (iccid WITH =, period WITH &&)
		);");

		$dbh->do("CREATE DOMAIN mobilenumber AS TEXT CONSTRAINT numberformat CHECK (LENGTH(VALUE)=11 AND SUBSTRING(VALUE FOR 3)='316')");
		$dbh->do("CREATE TABLE phonenumber (
			phonenumber MOBILENUMBER NOT NULL,
			period DATERANGE NOT NULL,
			sim_iccid SHORTTEXT NOT NULL,
			PRIMARY KEY (phonenumber, period),
			EXCLUDE USING gist (phonenumber WITH =, period WITH &&)
		)");

		$dbh->do("CREATE FUNCTION floorn (n numeric, places int) RETURNS numeric "
			."AS 'SELECT (floor(n * power(10, places)) / power(10, places))::numeric;' "
			."LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;");

		$dbh->do("CREATE FUNCTION constraint_list_matches (needle text, haystack text[]) RETURNS boolean "
			."AS 'SELECT array_lower(haystack, 1) IS NULL OR needle = ANY (haystack);' "
			."LANGUAGE SQL IMMUTABLE;");

		$dbh->do("CREATE TYPE servicetype AS ENUM('DATA', 'SMS', 'VOICE');");
		$dbh->do("CREATE TABLE invoice_itemline (
			id SERIAL PRIMARY KEY NOT NULL,
			type ITEMLINETYPE NOT NULL,

			queued_for_account_id INTEGER NULL,
			invoice_id INVOICEID NULL REFERENCES invoice(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			CHECK (queued_for_account_id IS NULL OR invoice_id IS NULL),
			CHECK (queued_for_account_id IS NOT NULL OR invoice_id IS NOT NULL),

			description LONGTEXT NOT NULL,
			taxrate MONEY8 NOT NULL,
			rounded_total MONEY2 NOT NULL,

			base_amount MONEY8,
			CHECK (type = 'TAX' OR base_amount IS NULL),
			CHECK (type != 'TAX' OR base_amount IS NOT NULL),

			item_price MONEY8,
			CHECK (type = 'DURATION' OR item_price IS NOT NULL),
			CHECK (type != 'DURATION' OR item_price IS NULL),
			item_count INTEGER,
			CHECK (item_price IS NULL OR item_count IS NOT NULL),
			CHECK (item_price IS NOT NULL OR item_count IS NULL),

			number_of_calls INTEGER,
			CHECK (type = 'DURATION' OR number_of_calls IS NULL),
			CHECK (type != 'DURATION' OR number_of_calls IS NOT NULL),
			number_of_seconds INTEGER,
			CHECK (number_of_calls IS NULL OR number_of_seconds IS NOT NULL),
			CHECK (number_of_calls IS NOT NULL OR number_of_seconds IS NULL),
			price_per_call MONEY8,
			CHECK (number_of_calls IS NULL OR price_per_call IS NOT NULL),
			CHECK (number_of_calls IS NOT NULL OR price_per_call IS NULL),
			price_per_minute MONEY8,
			CHECK (number_of_calls IS NULL OR price_per_minute IS NOT NULL),
			CHECK (number_of_calls IS NOT NULL OR price_per_minute IS NULL),

			service SERVICETYPE NULL
		);");

		$dbh->do("CREATE TYPE directiontype AS ENUM('IN', 'OUT');");

		$dbh->do("CREATE TYPE legreason AS ENUM('ORIG', 'CFIM', 'CFOR', 'CFBS', 'CFNA', 'ROAM', 'CALLBACK');");
		$dbh->do("CREATE TABLE pricing (
			id SERIAL PRIMARY KEY NOT NULL,
			period DATERANGE NOT NULL,
			description LONGTEXT NOT NULL,
			hidden BOOLEAN NOT NULL,

			-- Filters. An empty list means 'any is fine'
			service SERVICETYPE NOT NULL,
			call_connectivity_type CALLCONNECTIVITYTYPE[] NOT NULL,
			source TEXT[] NOT NULL,
			destination TEXT[] NOT NULL,
			direction DIRECTIONTYPE[] NOT NULL,
			connected BOOLEAN[] NOT NULL,
			legreason LEGREASON[] NOT NULL,

			cost_per_line MONEY8 NOT NULL, -- used to be cost.perCall/perSms
			cost_per_unit MONEY8 NOT NULL, -- used to be cost.perMinute/perKilobyte
			price_per_line MONEY8 NOT NULL, -- used to be price.perCall/perSms
			price_per_unit MONEY8 NOT NULL  -- used to be price.perMinute/perKilobyte
		);");

		$dbh->do("CREATE TABLE cdr (
			id SERIAL PRIMARY KEY NOT NULL,
			service SERVICETYPE NOT NULL,
			call_id SHORTTEXT NOT NULL,
			\"from\" SHORTTEXT NOT NULL,
			\"to\" SHORTTEXT NOT NULL,
			speakup_account SHORTTEXT NOT NULL,
			time TIMESTAMP NOT NULL,

			pricing_id BIGINT NULL REFERENCES pricing(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			pricing_id_two BIGINT NULL REFERENCES pricing(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			CHECK (pricing_id IS NOT NULL OR pricing_id_two IS NULL),
			pricing_info JSON NULL,
			CHECK (pricing_id IS NULL OR pricing_info IS NOT NULL),
			CHECK (pricing_id IS NOT NULL OR pricing_info IS NULL),
			computed_cost MONEY8 NULL,
			CHECK (pricing_id IS NULL OR computed_cost IS NOT NULL),
			CHECK (pricing_id IS NOT NULL OR computed_cost IS NULL),
			computed_price MONEY8 NULL,
			CHECK (pricing_id IS NULL OR computed_price IS NOT NULL),
			CHECK (pricing_id IS NOT NULL OR computed_price IS NULL),

			invoice_id INVOICEID NULL REFERENCES invoice(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			units INTEGER NOT NULL,
			connected BOOLEAN NULL,
			CHECK(service = 'VOICE' OR connected IS NULL),
			CHECK(service != 'VOICE' OR connected IS NOT NULL),
			source SHORTTEXT NULL,
			destination SHORTTEXT NULL,
			direction DIRECTIONTYPE NOT NULL,
			leg INT NULL,
			reason LEGREASON NULL
		);");

		add_directdebit_tables($lim, $dbh);
		add_cdr_import_tables($lim, $dbh);
		add_payment_tables($lim, $dbh);

		return $dbh->commit();
	} catch {
		$dbh->rollback();
		die $_;
	}
}

=head3 upgrade_database($lim, $current_version)

Upgrades the database schema to the version returned by get_latest_schema_version(), or throws
an exception if this was impossible.

=cut

sub upgrade_database {
	my ($lim, $current_version) = @_;
	if($current_version <= 0) {
		die "upgrade_database cannot initialize a database\n";
	}

	my $dbh = $lim->get_database_handle();
	$dbh->begin_work;

	try {
		$dbh->do("LOCK TABLE meta;");

		# Disable CREATE TABLE notices for this transaction only
		$dbh->do("SET LOCAL client_min_messages='WARNING';");

		if($current_version == 1) {
			add_directdebit_tables($lim, $dbh);
			update_schema_version($lim, $dbh, 2);
			$current_version = 2;
		}

		if($current_version == 2) {
			add_cdr_import_tables($lim, $dbh);
			update_schema_version($lim, $dbh, 3);
			$current_version = 3;
		}

		if($current_version == 3) {
			$dbh->do("ALTER TABLE account DROP COLUMN state");
			$dbh->do("DROP TYPE accountstate");
			update_schema_version($lim, $dbh, 4);
			$current_version = 4;
		}

		if($current_version == 4) {
			add_service_invoice_itemline_column($lim, $dbh);
			update_schema_version($lim, $dbh, 5);
			$current_version = 5;
		}

		if($current_version == 5) {
			remove_portingstate_from_sim($lim, $dbh);
			update_schema_version($lim, $dbh, 6);
			$current_version = 6;
		}

		if($current_version == 6) {
			# Allow multiple speakup_accounts for a single
			# account_id in a period
			$dbh->do("ALTER TABLE speakup_account DROP CONSTRAINT "
				." speakup_account_account_id_period_excl");

			# Remove null speakup_accounts
			$dbh->do("DELETE FROM speakup_account WHERE "
				." account_id IS NULL");
			# Disallow null speakup accounts
			$dbh->do("ALTER TABLE speakup_account ALTER COLUMN "
				."account_id SET NOT NULL");

			# fix default creation_time of an invoice
			$dbh->do("ALTER TABLE invoice ALTER COLUMN "
				."creation_time SET NOT NULL");
			$dbh->do("ALTER TABLE invoice ALTER COLUMN "
				."creation_time SET DEFAULT CURRENT_TIMESTAMP");

			update_schema_version($lim, $dbh, 7);
			$current_version = 7;
		}

		if($current_version == 7) {
			$dbh->do("ALTER TABLE pricing ADD COLUMN legreason LEGREASON[] NOT NULL DEFAULT '{}'");

			update_schema_version($lim, $dbh, 8);
			$current_version = 8;
		}

		if($current_version == 8) {
			$dbh->do("ALTER TABLE cdr ADD COLUMN pricing_id_two BIGINT NULL REFERENCES pricing(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED");
			$dbh->do("ALTER TABLE cdr ADD CHECK (pricing_id IS NOT NULL OR pricing_id_two IS NULL)");

			update_schema_version($lim, $dbh, 9);
			$current_version = 9;
		}

		if($current_version == 9) {
			# Add contribution field and set it to the value Liquid Pricing used to
			# have before 2015-01-28
			$dbh->do("ALTER TABLE account ADD COLUMN contribution MONEY8 NULL");
			$dbh->do("UPDATE account SET contribution=3.0991736");
			$dbh->do("ALTER TABLE account ALTER COLUMN contribution SET NOT NULL");

			# Add last_contribution_month. Keep it NULL, it will
			# default to start at 2015-02-01
			$dbh->do("ALTER TABLE account ADD COLUMN last_contribution_month DATE");

			update_schema_version($lim, $dbh, 10);
			$current_version = 10;
		}

		if($current_version == 10) {
			add_payment_tables($lim, $dbh);
			update_schema_version($lim, $dbh, 11);
			$current_version = 11;
		}

		if($current_version > 11) {
			die "Not implemented";
		}

		$dbh->commit;
	} catch {
		$dbh->rollback;
		die $_;
	};
}

=head3 add_directdebit_tables($lim, $dbh)

Add tables related to the directdebiting code. $dbh is the database handle
on which the queries will be ran.

=cut

sub add_directdebit_tables {
	my ($lim, $dbh) = @_;

	$dbh->do("CREATE TABLE account_directdebit_info (
		authorization_id SHORTTEXT NOT NULL,
		account_id INT NOT NULL,
		period DATERANGE NOT NULL,
		bank_account_name SHORTTEXT NOT NULL,
		iban SHORTTEXT NOT NULL,
		bic SHORTTEXT NOT NULL,
		signature_date DATE NOT NULL,
		CHECK(lower(period) = signature_date),

		PRIMARY KEY (authorization_id),
		UNIQUE (account_id, period),
		EXCLUDE USING gist (account_id WITH =, period WITH &&)
	);");

	$dbh->do("CREATE TYPE ddtransactionstatus AS ENUM('NEW', 'SUCCESS', 'PRESETTLEMENTREJECT', 'POSTSETTLEMENTREJECT');");
	$dbh->do("CREATE TYPE ddfiletype AS ENUM('FRST', 'RCUR', 'FNAL', 'OOFF');");

	$dbh->do("CREATE TABLE directdebit_file (
		id SERIAL PRIMARY KEY NOT NULL,
		message_id SHORTTEXT NOT NULL,
		creation_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
		processing_date DATE NOT NULL,
		type DDFILETYPE NOT NULL,
		UNIQUE(message_id)
	);");

	$dbh->do("CREATE TABLE directdebit_transaction (
		id SERIAL NOT NULL,
		invoice_id INVOICEID NOT NULL REFERENCES invoice(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,
		authorization_id SHORTTEXT NOT NULL REFERENCES account_directdebit_info(authorization_id),
		status DDTRANSACTIONSTATUS NOT NULL,
		directdebit_file_id INT NULL REFERENCES directdebit_file(id) ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED,

		-- No transaction can be successful or failed without being in a directdebit file
		CHECK(directdebit_file_id IS NOT NULL OR status='NEW'),
		-- No invoice can be in two transactions of the same status
		UNIQUE(invoice_id, status)
	);");
}

=head3 add_cdr_import_tables($lim, $dbh)

Add tables related to the CDR importer. $dbh is the database handle
on which the queries will be ran.

=cut

sub add_cdr_import_tables {
	my ($lim, $dbh) = @_;

	$dbh->do("CREATE TABLE cdrimports (
		cdr_date DATE PRIMARY KEY NOT NULL,
		import_time TIMESTAMP NOT NULL,
		error LONGTEXT NULL
	)");
}

=head3 add_service_invoice_itemline_column($lim, $dbh)

Add the optional service column to the invoice-itemline table. Update all
current itemlines so that their service is set to the service in the pricing
line with the same description, if possible.

=cut

sub add_service_invoice_itemline_column {
	my ($lim, $dbh) = @_;

	$dbh->do("ALTER TABLE invoice_itemline ADD COLUMN service SERVICETYPE NULL;");
	my $sth = $dbh->prepare("SELECT DISTINCT description FROM invoice_itemline");
	$sth->execute();
	my @descriptions;
	while(my $row = $sth->fetchrow_arrayref) {
		push @descriptions, $row->[0];
	}

	$sth = $dbh->prepare("SELECT service FROM pricing WHERE ?::text LIKE CONCAT('%', description, '%') ORDER BY id DESC");
	my %descriptions;
	foreach my $description (@descriptions) {
		$sth->execute($description);
		my $service = $sth->fetchrow_arrayref;
		if($service) {
			$descriptions{$description} = $service->[0];
		}
	}

	$sth = $dbh->prepare("UPDATE invoice_itemline SET service=? WHERE description=?");
	foreach(keys %descriptions) {
		$sth->execute($descriptions{$_}, $_);
	}

	$sth = $dbh->prepare("UPDATE invoice_itemline SET service='DATA' where description like 'Dataverbruik%'");
	$sth->execute();
}

=head3 remove_portingstate_from_sim($lim, $dbh)

Remove the portingstate column from the SIM table. Now that we have a temporal
database, it gives no information.

=cut

sub remove_portingstate_from_sim {
	my ($lim, $dbh) = @_;

	$dbh->do("ALTER TABLE sim DROP COLUMN porting_state;");
	$dbh->do("DROP TYPE portingstate;");
}

=head3 add_payment_tables($lim, $dbh)

Add the payment and bankaccountimports tables.

=cut

sub add_payment_tables {
	my ($lim, $dbh) = @_;
	$dbh->do("CREATE TABLE bankaccountimports (
			startdate DATE NOT NULL PRIMARY KEY,
			startbalance MONEY2 NOT NULL,
			enddate DATE NOT NULL UNIQUE,
			endbalance MONEY2 NOT NULL,
			transactions INT NOT NULL,
			payments INT NOT NULL,
			CHECK(enddate > startdate)
	)");
	$dbh->do("CREATE TYPE paymenttype AS ENUM('BANK_TRANSFER', 'TARGETPAY', 'DIRECTDEBIT', 'COINQY', 'BITKASSA', 'ADMINISTRATIVE');");
	$dbh->do("CREATE TABLE payment (
			id SERIAL PRIMARY KEY NOT NULL,
			account_id INTEGER NOT NULL,
			type PAYMENTTYPE NOT NULL,
			amount MONEY2 NOT NULL,
			date DATE NOT NULL,
			origin SHORTTEXT NULL,
			description LONGTEXT NULL
	)");
}

1;
