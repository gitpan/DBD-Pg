#!perl -w

# Create the "dbd_pg_test" table which is used for the other tests
# Because this table is used for the other tests,
# we bail out if we cannot create it.

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 4;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 0, PrintError => 0, AutoCommit => 1});

ok( defined $dbh, "Connect to database for test table creation");

# Remove the test table if it exists
# This assumes that pg_class is the one in the pg_catalog schema for > 7.3
# This is a pretty safe assumption, as making your own pg_class table
# is a very, very bad idea...

# Implicit tests of prepare, execute, fetchall_arrayref, and do
my $SQL = "SELECT COUNT(*) FROM pg_class WHERE relname='dbd_pg_test'";
my $sth = $dbh->prepare($SQL);
$sth->execute();
my $count = $sth->fetchall_arrayref()->[0][0];
if ($count == 1) {
	$dbh->do("DROP TABLE dbd_pg_test");
}

# If you add columns to this, please do not use reserved words!
$SQL = qq{
CREATE TABLE dbd_pg_test (
  id         integer not null primary key,
  pname      varchar(20) default 'Testing Default' ,
  val        text,
  score      float CHECK(score IN ('1','2','3')),
  Fixed      character(5),
  pdate      timestamp default now(),
  testarray  text[][],
  "CaseTest" boolean
)
};

$dbh->{Warn}=0;
ok( $dbh->do($SQL), qq{Created test table "dbd_pg_test"})
	or print STDOUT "Bail out! Test table could not be created: $DBI::errstr\n";

# Double check that the file is there
$sth->execute();
$count = $sth->fetchall_arrayref()->[0][0];
is( $count, 1, 'Test table was successfully created')
	or print STDOUT "Bail out! Test table was not created\n";

ok( $dbh->disconnect(), 'Disconnect from database');

