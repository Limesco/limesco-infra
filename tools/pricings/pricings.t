#!/usr/bin/perl
use strict;
use warnings;

use Test::PostgreSQL;
use Test::More;
use lib 'lib';
use lib '../lib';
use lib '../../lib';
use Limesco;
use Try::Tiny;
use POSIX 'strftime';

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 1;

require_ok("pricings.pl");

#my $lim = Limesco->new_for_test($pgsql->dsn);
#my $dbh = $lim->get_database_handle();

#require('../upgrade/upgrade.pl') or die $!;
#initialize_database($lim);

#$dbh->disconnect;
