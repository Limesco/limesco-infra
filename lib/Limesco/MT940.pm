package Limesco::MT940;

use strict;
use warnings;

sub parse_from_file {
	my ($pkg, $file) = @_;
	my $str = "";
	open my $fh, '<', $file or die $!;
	while(<$fh>) {
		$str .= $_;
	}
	close $fh;
	return $pkg->parse_from_string($str);
}

sub parse_from_string {
	my ($pkg, $string) = @_;
	my @fields = $pkg->parse_fields($string);
	if($pkg->get_next_tag_value('940', \@fields) ne "") {
		die "Corrupt mt940 file: 940 tag should not have a value\n";
	}

	my @date_sections = $pkg->cut_fields('20', @fields);
	my @dates;
	foreach my $date_fields (@date_sections) {
		my $date = $pkg->get_next_tag_value('20', $date_fields);
		my $account = $pkg->get_next_tag_value('25', $date_fields);
		my $statementnr = $pkg->get_next_tag_value('28C', $date_fields);
		my $start_balance = $pkg->get_next_tag_value('60F', $date_fields);
		my @transactions;
		while($date_fields->[0][0] eq '61') {
			my $transaction = $pkg->get_next_tag_value('61', $date_fields);
			my $description;
			if($date_fields->[0][0] eq '86') {
				$description = $pkg->get_next_tag_value('86', $date_fields);
			}
			push @transactions, {
				transaction => $transaction,
				description => $description,
			};
		}

		my $end_balance = $pkg->get_next_tag_value('62F', $date_fields);
		my $vol_balance;
		if(@$date_fields && $date_fields->[0][0] eq '64') {
			$vol_balance = $pkg->get_next_tag_value('64', $date_fields);
		}

		push @dates, {
			date => $date,
			account => $account,
			statementnr => $statementnr,
			start_balance => $start_balance,
			transactions => \@transactions,
			end_balance => $end_balance,
			vol_balance => $vol_balance,
		};
	}
	return @dates;
}

sub get_next_tag_value {
	my ($pkg, $tag, $fields) = @_;
	if(@$fields == 0) {
		die "Corrupt mt940 file: expected :$tag: field, got end of file\n";
	}
	my $field = shift @$fields;
	if($field->[0] ne $tag) {
		die "Corrupt mt940 file: expected :$tag: field, got :" . $field->[0] . ":\n";
	}
	return $field->[1];
}

sub cut_fields {
	my ($pkg, $cut, @fields) = @_;
	if(@fields == 0) {
		return;
	}

	my $first_field = shift @fields;
	if($first_field->[0] ne $cut) {
		die "Corrupt mt940 file: expected a :$cut: record, got :" . $first_field->[0] . ":\n";
	}

	my @cut_fields;
	my @these_fields = ($first_field);

	while(@fields) {
		my $field = shift @fields;
		if($field->[0] eq $cut) {
			# cut here!
			push @cut_fields, [@these_fields];
			@these_fields = ($field);
		} else {
			push @these_fields, $field;
		}
	}
	push @cut_fields, [@these_fields];
	return @cut_fields;
}

sub parse_fields {
	my ($pkg, $string) = @_;

	my $offset = 0;
	my $length = length($string);
	my @fields;
	while($offset < $length) {
		my $line_end = index($string, "\n", $offset);
		my $line;
		if($line_end == -1) {
			$line = substr($string, $offset);
			$offset = $length;
		} else {
			$line = substr($string, $offset, $line_end - $offset);
			$offset = $line_end + 1;
		}

		if(substr($line, 0, 1) ne ':') {
			die "Corrupt mt940 file: line does not start with colon\n";
		}

		my $colon = index($line, ":", 1);
		if($colon == -1) {
			die "Corrupt mt940 file: id does not end\n";
		}
		my $id = substr($line, 1, $colon - 1);
		my $value = substr($line, $colon + 1);
		while($offset < $length) {
			# $offset points at the start of the next line, and we're supposed to
			# figure out whether the next line is a continuation of this one, or a
			# new command (which must start with ':<id>:')
			my $line_end = index($string, "\n", $offset);
			my $line;
			if($line_end == -1) {
				$line = substr($string, $offset);
				$line_end = $length;
			} else {
				$line = substr($string, $offset, $line_end - $offset);
				$line_end++; # include newline
			}
			if($line =~ /^:\d\d[A-Z]?:/) {
				# not a continuation line
				last;
			}
			$value .= "\n$line";
			$offset = $line_end;
		}
		$value =~ s/\r//g;
		push @fields, [$id, $value];
	}
	return @fields;
}

1;
