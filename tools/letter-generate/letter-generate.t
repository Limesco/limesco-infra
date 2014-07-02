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
use File::Temp qw(tempdir);

my $pgsql = Test::PostgreSQL->new() or plan skip_all => $Test::PostgreSQL::errstr;
plan tests => 1;

require_ok("letter-generate.pl");
# The template generation itself is tested in invoice-export.
# TODO: move these tests here.
