#-------------------------------------------------------
#
# $Id: test.pl,v 1.3 1997/04/06 08:41:15 mergl Exp $
#
#   Portions Copyright (c) 1994,1995,1996  Tim Bunce
#   Portions Copyright (c) 1997            Edmund Mergl
#
#-------------------------------------------------------

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

BEGIN { $| = 1; print "1..27\n"; }
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

$| = 1;

######################### End of black magic.

$dbmain = 'template1';
$dbname = 'pgperltest';
$dbuser = '';
$dbpass = '';

######################### create and connect to test database

( $dbh = DBI->connect($dbmain, $dbuser, $dbpass, 'Pg') )
    and print "ok 2\n"
    or die "not ok 2: $DBI::errstr";

# might fail if $dbname doesn't exist
$dbh->do( "DROP DATABASE $dbname" );

( $dbh->do( "CREATE DATABASE $dbname" ) )
    and print "ok 3\n"
    or print "not ok 3: $DBI::errstr";

( $dbh->disconnect )
    and print "ok 4\n"
    or print "not ok 4: $DBI::errstr";

( $dbh = DBI->connect($dbname, $dbuser, $dbpass, 'Pg') )
    and print "ok 5\n"
    or print "not ok 5: $DBI::errstr";

######################### create, insert, update, delete, drop

( $dbh->do( "CREATE TABLE person ( id int4, name char16 )" ) )
    and print "ok 6\n"
    or print "not ok 6: $DBI::errstr";

( $dbh->do( "INSERT INTO person VALUES( 1, 'Edmund Mergl' )" ) )
    and print "ok 7\n"
    or print "not ok 7: $DBI::errstr";

( $sth = $dbh->prepare( "INSERT INTO person VALUES( 2, 'Potz Blitz' )" ) )
    and print "ok 8\n"
    or print "not ok 8: $DBI::errstr";

( $sth->execute )
    and print "ok 9\n"
    or print "not ok 9: $DBI::errstr";

( $sth = $dbh->prepare( "SELECT * FROM person" ) )
    and print "ok 10\n"
    or print "not ok 10: $DBI::errstr";

( $numrows = $sth->execute )
    and print "ok 11\n"
    or print "not ok 11: $DBI::errstr";

print "not " if $numrows != 2;
print "ok 12\n";

print "not " if $sth->rows != 2;
print "ok 13\n";

print "not " if $DBI::rows != 2;
print "ok 14\n";

@row = $sth->fetchrow;
( join(" ", @row) eq '1 Edmund Mergl' ) 
    and print "ok 15\n"
    or print "not ok  15: $DBI::errstr";

@name = @{$sth->{'NAME'}};
( join(" ", @name) eq 'id name' )
    and print "ok 16\n"
    or print "not ok 16: $DBI::errstr";

@type = @{$sth->{'TYPE'}};
( join(" ", @type) eq '23 20' )
    and print "ok 17\n"
    or print "not ok 17: $DBI::errstr";

@size = @{$sth->{'SIZE'}};
( join(" ", @size) eq '4 16' )
    and print "ok 18\n"
    or print "not ok 18: $DBI::errstr";

( $sth->finish )
    and print "ok 19\n"
    or print "not ok 19: $DBI::errstr";

( $dbh->do( "UPDATE person SET id = 3 WHERE name = 'Edmund Mergl'" ) )
    and print "ok 20\n"
    or print "not ok 20: $DBI::errstr";

( $sth = $dbh->prepare( "UPDATE person SET id = id + 1" ) )
    and print "ok 21\n"
    or print "not ok 21: $DBI::errstr";

( $sth->execute )
    and print "ok 22\n"
    or print "not ok 22: $DBI::errstr";

( $dbh->do( "DELETE FROM person WHERE id = 3" ) )
    and print "ok 23\n"
    or print "not ok 23: $DBI::errstr";

( $dbh->do( "DROP TABLE person" ) )
    and print "ok 24\n"
    or print "not ok 24: $DBI::errstr";

######################### disconnect and drop test database

( $dbh = DBI->connect($dbmain, $dbuser, $dbpass, 'Pg') )
    and print "ok 25\n"
    or print "not ok 25: $DBI::errstr";

( $dbh->do( "DROP DATABASE $dbname" ) )
    and print "ok 26\n"
    or print "not ok 26: $DBI::errstr";

( $dbh->disconnect )
    and print "ok 27\n"
    or print "not ok 27: $DBI::errstr";


print "all tests passed.\n";

######################### EOF
