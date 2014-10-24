use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
	plan tests => 2;
} else {
	print STDOUT "Bail out! DBI_DSN must be set: see the README file\n";
	plan skip_all => 'Cannot test without DB info';
}

my $dbh;
eval {
$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
	{RaiseError => 0, PrintError => 0, AutoCommit => 0});
};

ok((defined $dbh and $dbh->disconnect()),
	'connect with transaction'
) or print STDOUT "Bail out! Could not connect to the database.\n";

undef $dbh;
$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
	{RaiseError => 1, AutoCommit => 1});

ok((defined $dbh and $dbh->disconnect()),
	'connect without transaction'
);

