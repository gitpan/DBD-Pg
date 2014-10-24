#!perl

## Test all handle attributes: database, statement, and generic ("any")

use strict;
use warnings;
use Test::More;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my ($helpconnect,$connerror,$dbh) = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 135;

isnt ($dbh, undef, 'Connect to database for handle attributes testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});

my $attributes_tested = q{

d = database handle specific
s = statement handle specific
a = any type of handle (but we usually use database)

In order:

d Statement (must be the first one tested)
d CrazyDiamond (bogus)
d private_dbdpg_*
d AutoCommit
d Driver
d Name
d RowCacheSize
d Username
d PrintWarn
d pg_INV_READ
d pg_INV_WRITE
d pg_protocol
d pg_errorlevel
d pg_bool_tf
d pg_enable_utf8
d pg_db
d pg_user
d pg_pass
d pg_port
d pg_default_port
d pg_options
d pg_socket
d pg_pid

d pg_prepare_now - tested in 03smethod.t
d pg_server_prepare - tested in 03smethod.t
d pg_prepare_now - tested in 03smethod.t
d pg_placeholder_dollaronly - tested in 12placeholders.t

s NUM_OF_FIELDS, NUM_OF_PARAMS
s NAME, NAME_lc, NAME_uc, NAME_hash, NAME_lc_hash, NAME_uc_hash
s TYPE, PRECISION, SCALE, NULLABLE
s CursorName
s Database
s ParamValues
s ParamTypes
s RowsInCache

a Warn (inheritance test also)
a Active
a Kids
a ActiveKids
a CachedKids
a CompatMode
a PrintError
a RaiseError
a HandleError
a ShowErrorStatement (unsupported)
a TraceLevel
a FetchHashKeyName
a ChopBlanks
a LongReadLen
a LongTruncOk
a TaintIn
a TaintOut
a Taint
a Profile (not tested)

d InactiveDestroy (must be the last one tested)

};

my ($attrib,$SQL,$sth,$warning,$result,$expected,$t);

# Get the DSN and user from the test file, if it exists
my ($testdsn, $testuser) = get_test_settings();



#
# Test of the database handle attribute "Statement"
#

$SQL = 'SELECT 123';
$sth = $dbh->prepare($SQL);
$sth->finish();

$t='DB handle attribute "Statement" returns the last prepared query';
$attrib = $dbh->{Statement};
is ($attrib, $SQL, $t);

#
# Test of bogus database/statement handle attributes
#

## DBI switched from error to warning in 1.43
$t='Error or warning when setting an invalid database handle attribute';
$warning=q{};
eval {
	local $SIG{__WARN__} = sub { $warning = shift; };
	$dbh->{CrazyDiamond}=1;
};
isnt ($warning, q{}, $t);

$t='Setting a private attribute on a database handle does not throw an error';
eval {
	$dbh->{private_dbdpg_CrazyDiamond}=1;
};
is ($@, q{}, $t);

$sth = $dbh->prepare('SELECT 123');

$t='Error or warning when setting an invalid statement handle attribute';
$warning=q{};
eval {
	local $SIG{__WARN__} = sub { $warning = shift; };
	$sth->{CrazyDiamond}=1;
};
isnt ($warning, q{}, $t);

$t='Setting a private attribute on a statement handle does not throw an error';
eval {
	$sth->{private_dbdpg_CrazyDiamond}=1;
};
is ($@, q{}, $t);

#
# Test of the database handle attribute "AutoCommit"
#

$t='Commit after deleting all rows from dbd_pg_test';
$dbh->do('DELETE FROM dbd_pg_test');
ok ($dbh->commit(), $t);

$t='Connect to database with second database handle, AutoCommit on';
my $dbh2 = connect_database({AutoCommit => 1});
isnt ($dbh2, undef, $t);

$t='Insert a row into the database with first database handle';
ok ($dbh->do(q{INSERT INTO dbd_pg_test (id, pname, val) VALUES (1, 'Coconut', 'Mango')}), $t);

$t='Second database handle cannot see insert from first';
my $rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
is ($rows, 0, $t);

$t='Insert a row into the database with second database handle';
ok ($dbh->do(q{INSERT INTO dbd_pg_test (id, pname, val) VALUES (2, 'Grapefruit', 'Pomegranate')}), $t);

$t='First database handle can see insert from second';
$rows = ($dbh->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 2}))[0];
cmp_ok ($rows, '==', 1, $t);

ok ($dbh->commit, 'Commit transaction with first database handle');

$t='Second database handle can see insert from first';
$rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
is ($rows, 1, $t);

ok ($dbh2->disconnect(), 'Disconnect with second database handle');


#
# Test of the database handle attribute "Driver"
#

$t='$dbh->{Driver}{Name} returns correct value of "Pg"';
$attrib = $dbh->{Driver}->{Name};
is ($attrib, 'Pg', $t);

#
# Test of the database handle attribute "Name"
#

SKIP: {

	$t='DB handle attribute "Name" returns same value as DBI_DSN';
	if (! length $testdsn or $testdsn !~ /^dbi:Pg:(.+)/) {
		skip (q{Cannot test DB handle attribute "Name" invalid DBI_DSN}, 1);
	}
	$expected = $1 || $ENV{PGDATABASE};
	defined $expected and length $expected or skip ('Cannot test unless database name known', 1);
	$attrib = $dbh->{Name};
	$expected =~ s/(db|database)=/dbname=/;
	is ($attrib, $expected, $t);
}

#
# Test of the database handle attribute "RowCacheSize"
#

$t='DB handle attribute "RowCacheSize" returns undef';
$attrib = $dbh->{RowCacheSize};
is ($attrib, undef, $t);

$t='Setting DB handle attribute "RowCacheSize" has no effect';
$dbh->{RowCacheSize} = 42;
$attrib = $dbh->{RowCacheSize};
is ($attrib, undef, $t);

#
# Test of the database handle attribute "Username"
#

$t='DB handle attribute "Username" returns the same value as DBI_USER';
$attrib = $dbh->{Username};
is ($attrib, $testuser, $t);

#
# Test of the "PrintWarn" database handle attribute
#

$t='DB handle attribute "PrintWarn" defaults to on';
my $value = $dbh->{PrintWarn};
is ($value, 1, $t);

{

local $SIG{__WARN__} = sub { $warning = shift; };

$t='DB handle attribute "PrintWarn" works when on';
$warning = q{};
eval {
	$dbh->do('CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)');
};
is ($@, q{}, $t);

$t='DB handle attribute "PrintWarn" shows warnings when on';
like ($warning, qr{dbd_pg_test_temp}, $t);


$t='DB handle attribute "PrintWarn" works when on';
$dbh->rollback();
$dbh->{PrintWarn}=0;
$warning = q{};
eval {
	$dbh->do('CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)');
};
is ($@, q{}, $t);

$t='DB handle attribute "PrintWarn" shows warnings when on';
is ($warning, q{}, $t);

$dbh->{PrintWarn}=1;
$dbh->rollback();

}


#
# Test of the database handle attributes "pg_INV_WRITE" and "pg_INV_READ"
# (these are used by the lo_* database handle methods)
#

$t='Database handle attribute "pg_INV_WRITE" returns a number';
like ($dbh->{pg_INV_WRITE}, qr/^\d+$/, $t);
$t='Database handle attribute "pg_INV_READ" returns a number';
like ($dbh->{pg_INV_READ}, qr/^\d+$/, $t);

#
# Test of the database handle attribute "pg_protocol"
#

$t='Database handle attribute "pg_protocol" returns a number';
like ($dbh->{pg_protocol}, qr/^\d+$/, $t);

#
# Test of the database handle attribute "pg_errorlevel"
#

$t='Database handle attribute "pg_errorlevel" returns the default (1)';
is ($dbh->{pg_errorlevel}, 1, $t);

$t='Database handle attribute "pg_errorlevel" defaults to 1 if invalid';
$dbh->{pg_errorlevel} = 3;
is ($dbh->{pg_errorlevel}, 1, $t);

#
# Test of the database handle attribute "pg_bool_tf"
#

$t='DB handle method "pg_bool_tf" starts as 0';
$result = $dbh->{pg_bool_tf}=0;
is ($result, 0, $t);

$t=q{DB handle method "pg_bool_tf" returns '1' for true when on};
$sth = $dbh->prepare('SELECT ?::bool');
$sth->bind_param(1,1,SQL_BOOLEAN);
$sth->execute();
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, '1', $t);

$t=q{DB handle method "pg_bool_tf" returns '0' for false when on};
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, '0', $t);

$t=q{DB handle method "pg_bool_tf" returns 't' for true when on};
$dbh->{pg_bool_tf}=1;
$sth->execute(1);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, 't', $t);

$t=q{DB handle method "pg_bool_tf" returns 'f' for true when on};
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, 'f', $t);


## Test of all the informational pg_* database handle attributes

$t='DB handle attribute "pg_db" returns at least one character';
$result = $dbh->{pg_protocol};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_db" returns at least one character';
$result = $dbh->{pg_db};
ok (length $result, $t);

$t='DB handle attribute "pg_user" returns a value';
$result = $dbh->{pg_user};
ok (defined $result, $t);

$t='DB handle attribute "pg_pass" returns a value';
$result = $dbh->{pg_pass};
ok (defined $result, $t);

$t='DB handle attribute "pg_port" returns a number';
$result = $dbh->{pg_port};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_default_port" returns a number';
$result = $dbh->{pg_default_port};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_options" returns a value';
$result = $dbh->{pg_options};
ok (defined $result, $t);

$t='DB handle attribute "pg_socket" returns a value';
$result = $dbh->{pg_socket};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_pid" returns a value';
$result = $dbh->{pg_pid};
like ($result, qr/^\d+$/, $t);

SKIP: {

	if ($pgversion < 80200) {
		skip ('Cannot test standard_conforming_strings on pre 8.2 servers', 3);
	}

	$t='DB handle attribute "pg_standard_conforming_strings" returns a valid value';
	my $oldscs = $dbh->{pg_standard_conforming_strings};
	like ($oldscs, qr/^on|off$/, $t);

	$t='DB handle attribute "pg_standard_conforming_strings" returns correct value';
	$dbh->do('SET standard_conforming_strings = on');
	$result = $dbh->{pg_standard_conforming_strings};
	is ($result, 'on', $t);

	$t='DB handle attribute "pg_standard_conforming_strings" returns correct value';
	$dbh->do('SET standard_conforming_strings = off');
	$result = $dbh->{pg_standard_conforming_strings};
	$dbh->do("SET standard_conforming_strings = $oldscs");
	is ($result, 'off', $t);
}

## If Encode is available, we will insert some non-ASCII into the test table
## Since this will fail with client encodings such as BIG5, we force UTF8
my $old_encoding = $dbh->selectall_arrayref('SHOW client_encoding')->[0][0];
if ($old_encoding ne 'UTF8') {
	$dbh->do(q{SET NAMES 'UTF8'});
}

# Attempt to test whether or not we can get unicode out of the database
SKIP: {
	eval { require Encode; };
	skip ('Encode module is needed for unicode tests', 5) if $@;
	my $SQL = 'SELECT id, pname FROM dbd_pg_test WHERE id = ?';
	my $sth = $dbh->prepare($SQL);
	$sth->execute(1);
	local $dbh->{pg_enable_utf8} = 1;

	$t='Quote method returns correct utf-8 characters';
	my $utf8_str = chr(0x100).'dam'; # LATIN CAPITAL LETTER A WITH MACRON
	is ($dbh->quote( $utf8_str ),  "'$utf8_str'", $t);

	$t='Able to insert unicode character into the database';
	$SQL = "INSERT INTO dbd_pg_test (id, pname, val) VALUES (40, '$utf8_str', 'Orange')";
	is ($dbh->do($SQL), '1', $t);

	$t='Able to read unicode (utf8) data from the database';
	$sth->execute(40);
	my ($id, $name) = $sth->fetchrow_array();
	ok (Encode::is_utf8($name), $t);

	$t='Unicode (utf8) data returned from database is not corrupted';
	is (length($name), 4, $t);

	$t='ASCII text returned from database does not have utf8 bit set';
	$sth->finish();
	$sth->execute(1);
	my ($id2, $name2) = $sth->fetchrow_array();
	ok (!Encode::is_utf8($name2), $t);
	$sth->finish();
}

#
# Use the handle attribute "Warn" to check inheritance
#

undef $sth;

$t='Attribute "Warn" attribute set on by default';
ok ($dbh->{Warn}, $t);

$t='Statement handle inherits the "Warn" attribute';
$SQL = 'SELECT 123';
$sth = $dbh->prepare($SQL);
$sth->finish();
ok ($sth->{Warn}, $t);

$t='Able to turn off the "Warn" attribute in the database handle';
$dbh->{Warn} = 0;
ok (! $dbh->{Warn}, $t);

#
# Test of the the following statement handle attributes:
# NUM_OF_FIELDS, NUM_OF_PARAMS
# NAME, NAME_lc, NAME_uc, NAME_hash, NAME_lc_hash, NAME_uc_hash
# TYPE, PRECISION, SCALE, NULLABLE
#

$t='Statement handle attribute "NUM_OF_FIELDS" works correctly for SELECT';
$sth = $dbh->prepare('SELECT 123 AS "Sheep", CAST(id AS float) FROM dbd_pg_test WHERE id=?');
$sth->execute(12);
$attrib = $sth->{'NUM_OF_FIELDS'};
is ($attrib, '2', $t);

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly with one placeholder';
$attrib = $sth->{'NUM_OF_PARAMS'};
is ($attrib, '1', $t);

$t='Statement handle attribute "NAME" works correctly';
$attrib = $sth->{NAME};
my $colnames = ['Sheep', 'id'];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NAME_lc" works correctly';
$attrib = $sth->{NAME_lc};
$colnames = ['sheep', 'id'];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NAME_uc" works correctly';
$attrib = $sth->{NAME_uc};
$colnames = ['SHEEP', 'ID'];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NAME_hash" works correctly';
$attrib = $sth->{'NAME_hash'};
$colnames = {'Sheep' => 0, id => 1};
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NAME_lc_hash" works correctly';
$attrib = $sth->{'NAME_lc_hash'};
$colnames = {sheep => 0, id => 1};
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NAME_uc_hash" works correctly';
$attrib = $sth->{NAME_uc_hash};
$colnames = {SHEEP => 0, ID => 1};
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "TYPE" works correctly';
$attrib = $sth->{TYPE};
$colnames = [4, 6];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "PRECISION" works correctly';
$attrib = $sth->{PRECISION};
$colnames = [4, 8];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "SCALE" works correctly';
$attrib = $sth->{SCALE};
$colnames = [undef,undef];
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NULLABLE" works correctly';
$attrib = $sth->{NULLABLE};
$colnames = [2,2];
$sth->finish();
is_deeply ($attrib, $colnames, $t);

$t='Statement handle attribute "NUM_OF_FIELDS" works correctly for DELETE';
$sth = $dbh->prepare('DELETE FROM dbd_pg_test WHERE id=0');
$sth->execute();
$attrib = $sth->{'NUM_OF_FIELDS'};
$expected = undef;
is ($attrib, $expected, $t);

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly with no placeholder';
$attrib = $sth->{'NUM_OF_PARAMS'};
is ($attrib, '0', $t);

$t='Statement handle attribute "NAME" works correctly for DELETE';
$attrib = $sth->{NAME};
$colnames = [];
$sth->finish();
is_deeply ($attrib, $colnames, $t);

#
# Test of the statement handle attribute "CursorName"
#

$t='Statement handle attribute "CursorName" returns undef';
$attrib = $sth->{CursorName};
is ($attrib, undef, $t);

#
# Test of the statement handle attribute "Database"
#

$t='Statement handle attribute "Database" matches the database handle';
$attrib = $sth->{Database};
is ($attrib, $dbh, $t);

#
# Test of the statement handle attribute "ParamValues"
#

$t='Statement handle attribute "ParamValues" works before execute';
$sth = $dbh->prepare('SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND pname=?');
$sth->bind_param(1, 99);
$sth->bind_param(2, undef);
$sth->bind_param(3, 'Sparky');
$attrib = $sth->{ParamValues};
$expected = {1 => '99', 2 => undef, 3 => 'Sparky'};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attribute "ParamValues" works after execute';
$sth->execute();
$attrib = $sth->{ParamValues};
is_deeply ($attrib, $expected, $t);

#
# Test of the statement handle attribute "ParamTypes"
#

$t='Statement handle attribute "ParamTypes" works before execute';
$sth = $dbh->prepare('SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND lii=?');
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->bind_param(2, 'TMW', SQL_VARCHAR);
$attrib = $sth->{ParamTypes};
$expected = {1 => 'int4', 2 => 'varchar', 3 => undef};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attribute "ParamTypes" works after execute';
$sth->bind_param(3, 3, {pg_type => PG_INT4});
$sth->execute();
$attrib = $sth->{ParamTypes};
$expected->{3} = 'int4';
is_deeply ($attrib, $expected, $t);

#
# Test of the statement handle attribute "RowsInCache"
#

$t='Statement handle attribute "RowsInCache" returns undef';
$attrib = $sth->{RowsInCache};
is ($attrib, undef, $t);


#
# Test of the statement handle attribute "pg_size"
#

$t='Statement handle attribute "pg_size" works';
$SQL = q{SELECT id, pname, val, score, Fixed, pdate, "CaseTest" FROM dbd_pg_test};
$sth = $dbh->prepare($SQL);
$sth->execute();
$result = $sth->{pg_size};
$expected = [qw(4 -1 -1 8 -1 8 1)];
is_deeply ($result, $expected, $t);

#
# Test of the statement handle attribute "pg_size"
#

$t='Statement handle attribute "pg_type" works';
$sth->execute();
$result = $sth->{pg_type};
$expected = [qw(int4 varchar text float8 bpchar timestamp bool)];
is_deeply ($result, $expected, $t);
$sth->finish();

#
# Test of the statement handle attribute "pg_oid_status"
#

$t='Statement handle attribute "pg_oid_status" returned a numeric value after insert';
$SQL = q{INSERT INTO dbd_pg_test (id, val) VALUES (?, 'lemon')};
$sth = $dbh->prepare($SQL);
$sth->bind_param('$1','',SQL_INTEGER);
$sth->execute(500);
$result = $sth->{pg_oid_status};
like ($result, qr/^\d+$/, $t);

#
# Test of the statement handle attribute "pg_cmd_status"
#

## INSERT DELETE UPDATE SELECT
for (
q{INSERT INTO dbd_pg_test (id,val) VALUES (400, 'lime')},
q{DELETE FROM dbd_pg_test WHERE id=1},
q{UPDATE dbd_pg_test SET id=2 WHERE id=2},
q{SELECT * FROM dbd_pg_test},
	) {
	my $expected = substr($_,0,6);
	$t=qq{Statement handle attribute "pg_cmd_status" works for '$expected'};
	$sth = $dbh->prepare($_);
	$sth->execute();
	$result = $sth->{pg_cmd_status};
	$sth->finish();
	like ($result, qr/^$expected/, $t);
}

## From this point forward, it is safe to use the client's native encoding again
if ($old_encoding ne 'UTF8') {
	$dbh->do(qq{SET NAMES '$old_encoding'});
}

#
# Test of the handle attribute "Active"
#

$t='Database handle attribute "Active" is true while connected';
$attrib = $dbh->{Active};
is ($attrib, 1, $t);

$t='Statement handle attribute "Active" is false before SELECT';
$sth = $dbh->prepare('SELECT 123 UNION SELECT 456');
$attrib = $sth->{Active};
is ($attrib, '', $t);

$t='Statement handle attribute "Active" is true after SELECT';
$sth->execute();
$attrib = $sth->{Active};
is ($attrib, 1, $t);

$t='Statement handle attribute "Active" is true when rows remaining';
my $row = $sth->fetchrow_arrayref();
$attrib = $sth->{Active};
is ($attrib, 1, $t);

$t='Statement handle attribute "Active" is false after finish called';
$sth->finish();
$attrib = $sth->{Active};
is ($attrib, '', $t);

#
# Test of the handle attribute "Kids"
#

$t='Database handle attribute "Kids" is set properly';
$attrib = $dbh->{Kids};
is ($attrib, 3, $t);

$t='Database handle attribute "Kids" works';
my $sth2 = $dbh->prepare('SELECT 234');
$attrib = $dbh->{Kids};
is ($attrib, 4, $t);

$t='Statement handle attribute "Kids" is zero';
$attrib = $sth2->{Kids};
is ($attrib, 0, $t);

#
# Test of the handle attribute "ActiveKids"
#

$t='Database handle attribute "ActiveKids" is set properly';
$attrib = $dbh->{ActiveKids};
is ($attrib, 0, $t);

$t='Database handle attribute "ActiveKids" works';
$sth2 = $dbh->prepare('SELECT 234');
$sth2->execute();
$attrib = $dbh->{ActiveKids};
is ($attrib, 1, $t);

$t='Statement handle attribute "ActiveKids" is zero';
$attrib = $sth2->{ActiveKids};
is ($attrib, 0, $t);

#
# Test of the handle attribute "CachedKids"
#

$t='Database handle attribute "CachedKids" is set properly';
$attrib = $dbh->{CachedKids};
is (keys %$attrib, 2, $t);

#
# Test of the handle attribute "CompatMode"
#

$t='Database handle attribute "CompatMode" is set properly';
$attrib = $dbh->{CompatMode};
ok (!$attrib, $t);

#
# Test of the handle attribute PrintError
#

$t='Database handle attribute "PrintError" is set properly';
$attrib = $dbh->{PrintError};
is ($attrib, '', $t);


# Make sure that warnings are sent back to the client
# We assume that older servers are okay
my $client_level = '';
$sth = $dbh->prepare('SHOW client_min_messages');
$sth->execute();
$client_level = $sth->fetchall_arrayref()->[0][0];

if ($client_level eq 'error') {
 SKIP: {
		skip (q{Cannot test "PrintError" attribute because client_min_messages is set to 'error'}, 2);
	}
 SKIP: {
		skip (q{Cannot test "RaiseError" attribute because client_min_messages is set to 'error'}, 2);
	}
 SKIP: {
		skip (q{Cannot test "HandleError" attribute because client_min_messages is set to 'error'}, 2);
	}
}
else {
	$SQL = 'Testing the DBD::Pg modules error handling -?-';
	{
		$warning = '';
		local $SIG{__WARN__} = sub { $warning = shift; };
		$dbh->{RaiseError} = 0;

		$t='Warning thrown when database handle attribute "PrintError" is on';
		$dbh->{PrintError} = 1;
		$sth = $dbh->prepare($SQL);
		$sth->execute();
		isnt ($warning, undef, $t);

		$t='No warning thrown when database handle attribute "PrintError" is off';
		undef $warning;
		$dbh->{PrintError} = 0;
		$sth = $dbh->prepare($SQL);
		$sth->execute();
		is ($warning, undef, $t);
	}
}

#
# Test of the handle attribute RaiseError
#

if ($client_level ne 'error') {
	$t='No error produced when database handle attribute "RaiseError" is off';
	$dbh->{RaiseError} = 0;
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	is ($@, q{}, $t);

	$t='Error produced when database handle attribute "RaiseError" is off';
	$dbh->{RaiseError} = 1;
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	isnt ($@, q{}, $t);
}


#
# Test of the handle attribute HandleError
#

$t='Database handle attribute "HandleError" is set properly';
$attrib = $dbh->{HandleError};
ok (!$attrib, $t);

if ($client_level ne 'error') {

	$t='Database handle attribute "HandleError" works';
	undef $warning;
	$dbh->{HandleError} = sub { $warning = shift; };
	$sth = $dbh->prepare($SQL);
	$sth->execute();
	ok ($warning, $t);

	$t='Database handle attribute "HandleError" modifies error messages';
	undef $warning;
	$dbh->{HandleError} = sub { $_[0] = "Slonik $_[0]"; 0; };
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	like ($@, qr/^Slonik/, $t);
	$dbh->{HandleError}= undef;
	$dbh->rollback();
}


#
# Not supported yet: ShowErrorStatement
#

#
# Test of the handle attribute TraceLevel
#

$t='Database handle attribute "TraceLevel" returns a number';
$attrib = $dbh->{TraceLevel};
like ($attrib, qr/^\d$/, $t);

#
# Test of the handle attribute FetchHashKeyName
#

# The default is mixed case ("NAME");
$t='Database handle attribute "FetchHashKeyName" is set properly';
$attrib = $dbh->{FetchHashKeyName};
is ($attrib, 'NAME', $t);

$t='Database handle attribute "FetchHashKeyName" works with the default value of NAME';
$SQL = q{SELECT "CaseTest" FROM dbd_pg_test};
$sth = $dbh->prepare($SQL);
$sth->execute();
my ($colname) = keys %{$sth->fetchrow_hashref()};
$sth->finish();
is ($colname, 'CaseTest', $t);

$t='Database handle attribute "FetchHashKeyName" can be changed';
$dbh->{FetchHashKeyName} = 'NAME_lc';
$attrib = $dbh->{FetchHashKeyName};
is ($attrib, 'NAME_lc', $t);

$t='Database handle attribute "FetchHashKeyName" works with a value of NAME_lc';
$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
is ($colname, 'casetest', $t);
$sth->finish();

$t='Database handle attribute "FetchHashKeyName" works with a value of NAME_uc';
$dbh->{FetchHashKeyName} = 'NAME_uc';
$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
$sth->finish();
$dbh->{FetchHashKeyName} = 'NAME';
is ($colname, 'CASETEST', $t);

#
# Test of the handle attribute ChopBlanks
#


$t='Database handle attribute "ChopBlanks" is set properly';
$attrib = $dbh->{ChopBlanks};
ok (!$attrib, $t);

$dbh->do('DELETE FROM dbd_pg_test');
$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed, val) VALUES (3, ' Fig', ' Raspberry ')});

$t='Database handle attribute "ChopBlanks" = 0 returns correct value for fixed-length column';
$dbh->{ChopBlanks} = 0;
my ($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is ($val, ' Fig ', $t);

$t='Database handle attribute "ChopBlanks" = 0 returns correct value for variable-length column';
($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
is ($val, ' Raspberry ', $t);

$t='Database handle attribute "ChopBlanks" = 1 returns correct value for fixed-length column';
$dbh->{ChopBlanks}=1;
($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is ($val, ' Fig', $t);

$t='Database handle attribute "ChopBlanks" = 1 returns correct value for variable-length column';
($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
$dbh->do('DELETE from dbd_pg_test');
is ($val, ' Raspberry ', $t);

#
# Test of the handle attribute LongReadLen
#

$t='Handle attribute "LongReadLen" has been set properly';
$attrib = $dbh->{LongReadLen};
ok ($attrib, $t);

#
# Test of the handle attribute LongTruncOk
#

$t='Handle attribute "LongTruncOk" has been set properly';
$attrib = $dbh->{LongTruncOk};
ok (!$attrib, $t);

#
# Test of the handle attribute TaintIn
#

$t='Handle attribute "TaintIn" has been set properly';
$attrib = $dbh->{TaintIn};
is ($attrib, '', $t);

#
# Test of the handle attribute TaintOut
#

$t='Handle attribute "TaintOut" has been set properly';
$attrib = $dbh->{TaintOut};
is ($attrib, '', $t);

#
# Test of the handle attribute Taint
#

$t='Handle attribute "Taint" has been set properly';
$attrib = $dbh->{Taint};
is ($attrib, '', $t);

$t='The value of handle attribute "Taint" can be changed';
$dbh->{Taint}=1;
$attrib = $dbh->{Taint};
is ($attrib, 1, $t);

$t='Changing handle attribute "Taint" changes "TaintIn"';
$attrib = $dbh->{TaintIn};
is ($attrib, 1, $t);

$t='Changing handle attribute "Taint" changes "TaintOut"';
$attrib = $dbh->{TaintOut};
is ($attrib, 1, $t);

#
# Not tested: handle attribute Profile
#

#
# Test of the database handle attribute InactiveDestroy
# This one must be the last test performed!
#

$t='Database handle attribute "InactiveDestroy" is set properly';
$attrib = $dbh->{InactiveDestroy};
ok (!$attrib, $t);

# Disconnect in preparation for the fork tests
ok ($dbh->disconnect(), 'Disconnect from database');

$t='Database handle attribute "Active" is false after disconnect';
$attrib = $dbh->{Active};
is ($attrib, '', $t);

SKIP: {
	skip ('Cannot test database handle "InactiveDestroy" on a non-forking system', 8)
		if $^O =~ /Win/;

	require Test::Simple;

	skip ('Test::Simple version 0.47 or better required for testing of attribute "InactiveDestroy"', 8)
		if $Test::Simple::VERSION < 0.47;

	# Test of forking. Hang on to your hats

	my $answer = 42;
	$SQL = "SELECT $answer FROM dbd_pg_test WHERE id > ? LIMIT 1";

	for my $destroy (0,1) {

		$dbh = connect_database({nosetup => 1, AutoCommit => 1});
		$sth = $dbh->prepare($SQL);
		$sth->execute(1);
		$sth->finish();

		# Desired flow: parent test, child test, child kill, parent test

		if (fork) {
			$t=qq{Parent in fork test is working properly ("InactiveDestroy" = $destroy)};
			$sth->execute(1);
			my $val = $sth->fetchall_arrayref()->[0][0];
			is ($val, $answer, $t);
			# Let the child exit
			select(undef,undef,undef,0.3);
		}
		else { # Child
			$dbh->{InactiveDestroy} = $destroy;
			select(undef,undef,undef,0.1); # Age before beauty
			exit; ## Calls disconnect via DESTROY unless InactiveDestroy set
		}

		if ($destroy) {
			$t=qq{Ping works after the child has exited ("InactiveDestroy" = $destroy)};
			ok ($dbh->ping(), $t);

			$t='Successful ping returns a SQLSTATE code of 00000 (empty string)';
			my $state = $dbh->state();
			is ($state, '', $t);

			$t='Statement handle works after forking';
			$sth->execute(1);
			my $val = $sth->fetchall_arrayref()->[0][0];
			is ($val, $answer, $t);
		}
		else {
			$t=qq{Ping fails after the child has exited ("InactiveDestroy" = $destroy)};
			is ( $dbh->ping(), 0, $t);

			$t='Failed ping returns a SQLSTATE code of 22000';
			my $state = $dbh->state();
			is ($state, '22000', $t);

			$t=qq{pg_ping gives an error code of -2 after the child has exited ("InactiveDestroy" = $destroy)};
			is ( $dbh->pg_ping(), -2,$t);
		}
	}
}

cleanup_database($dbh,'test');
$dbh->disconnect();
