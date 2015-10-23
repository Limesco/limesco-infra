package Limesco::Util;
use strict;
use warnings;

sub directory_list_ {
	my ($base, $path) = @_;
	my @paths;
	opendir my $dh, $path or die $!;
	while(my $file = readdir($dh)) {
		next if $file eq "." || $file eq "..";
		my $p = $base eq "" ? $file : "$base/$file";
		if(-d "$path/$file") {
			push @paths, directory_list_($p, "$path/$file");
		} else {
			push @paths, $p;
		}
	}
	closedir $dh;
	return @paths;
}

sub directory_list {
	my ($path) = @_;
	return sort {$a cmp $b} directory_list_("", $path);
}

1;
