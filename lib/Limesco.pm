package Limesco;
use strict;
use warnings;
use constant default_configfile => "/etc/limesco.conf";
use Carp;
use Config::Tiny;
use DBI;
use DBD::Pg;

our $VERSION = "0.01";

sub new_from_args {
	my ($package, $args, $param_handler) = @_;
	$param_handler ||= sub { return 1; };

	my $configfile = default_configfile();
	for(my $i = 0; $i < @$args; ++$i) {
		my $arg = $args->[$i];
		if($arg eq "-c") {
			$configfile = $args->[++$i];
		} elsif(!$param_handler->($args, \$i)) {
			croak "Unknown parameter: $arg";
		}
	}

	return $package->new_from_configfile($configfile);
}

sub new_for_test {
	my ($package, $dsn) = @_;
	return $package->new_from_config({
		database => {
			dsn => $dsn,
		},
	});
}

sub new_from_configfile {
	my ($package, $configfile) = @_;
	my $config = read_config($configfile);
	return $package->new_from_config($config);
}

sub new_from_config {
	my ($package, $config) = @_;
	my $self = {config => $config};
	bless $self, "Limesco";
	return $self;
}

sub get_database_handle {
	my ($self, $allow_cached) = @_;
	$allow_cached = 1 if not defined($allow_cached);
	my $db = $self->{'config'}{'database'};
	if(!$db->{'dsn'}) {
		my $dbname = $db->{'database'};
		if(!$dbname) {
			croak "Missing database name in configuration file";
		}
		my $dbn = "dbi:Pg:dbname=$dbname";
		if($db->{'host'}) {
			$dbn .= ";host=".$db->{'host'};
		}
		if($db->{'port'}) {
			$dbn .= ";port=".$db->{'port'};
		}
		if($db->{'options'}) {
			$dbn .= ";options=".$db->{'options'};
		}
		$db->{'dsn'} = $dbn;
	}
	my $connect_method = $allow_cached ? \&DBI::connect_cached : \&DBI::connect;
	my $dbh = $connect_method->("DBI", $db->{'dsn'}, $db->{'username'} || '', $db->{'password'} || '', {AutoCommit => 1, RaiseError => 1, PrintError => 0});
	if(!$dbh) {
		die $DBI::errstr;
	} else {
		return $dbh;
	}
}

sub read_config {
	my $config = Config::Tiny->read($_[0], 'utf8');
	return $config;
}

sub get_account {
	my ($self, $account_id) = @_;
	my $dbh = $self->get_database_handle();
	my $sth = $dbh->prepare("SELECT * FROM account WHERE id=?");
	$sth->execute($account_id);
	my $account = $sth->fetchrow_hashref;
	if(!$account) {
		die "No such account";
	}
	return $account;
}

1;
