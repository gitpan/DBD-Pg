#!/usr/local/bin/perl

#---------------------------------------------------------
#
# $Id: test.pl,v 1.6 1997/08/23 06:01:20 mergl Exp $
#
# Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
# Portions Copyright (c) 1997                Edmund Mergl
#
#---------------------------------------------------------

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

BEGIN { $| = 1; print "1..43\n"; }
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

######################### destroy and create test database

( my $dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass) )
    and print "ok 2\n"
    or exit;

( $sth = $dbh->prepare( "SELECT * FROM pg_database where datname = '$dbname'" ) )
    and print "ok 3\n"
    or exit;

( $numrows = $sth->execute )
    and print "ok 4\n"
    or exit;

( $sth->finish )
    and print "ok 5\n"
    or exit;

( $dbh->do( "DROP DATABASE $dbname" ) )
    if $numrows == 1;

( $dbh->do( "CREATE DATABASE $dbname" ) )
    and print "ok 6\n"
    or exit;

( $dbh->disconnect )
    and print "ok 7\n"
    or exit;

######################### create, insert, update, delete, drop

( $dbh = DBI->connect("dbi:Pg:$dbname", $dbuser, $dbpass) )
    and print "ok 8\n"
    or exit;

( $dbh->do( "CREATE TABLE builtin ( 
  bool    bool,
  char    char,
  char16  char16,
  text    text,
  date    date,
  int4    int4,
  _int4   int4[],
  float8  float8,
  point   point,
  lseg    lseg,
  box     box
  )" ) )
    and print "ok 9\n"
    or exit;

( $dbh->do( "INSERT INTO builtin VALUES(
  't',
  'a',
  'Edmund Mergl',
  'Edmund Mergl',
  '08-03-1997',
  1234,
  '{1,2,3}',
  1.234,
  '(1.0,2.0)',
  '((1.0,2.0),(3.0,4.0))',
  '((1.0,2.0),(3.0,4.0))'
  )" ) )
    and print "ok 10\n"
    or exit;

( $dbh->do( "INSERT INTO builtin VALUES( 
  'f',
  'b',
  'Halli Hallo',
  'Halli Hallo',
  '06-01-1995',
  5678,
  '{5,6,7}',
  5.678,
  '(4.0,5.0)',
  '((4.0,5.0),(6.0,7.0))',
  '((4.0,5.0),(6.0,7.0))'
  )" ) )
    and print "ok 11\n"
    or exit;

( $sth = $dbh->prepare( "INSERT INTO builtin VALUES( 
  'f',
  'c',
  'Potz Blitz',
  'Potz Blitz',
  '05-10-1957',
  1357,
  '{1,3,5}',
  1.357,
  '(2.0,7.0)',
  '((2.0,7.0),(8.0,3.0))',
  '((2.0,7.0),(8.0,3.0))'
  )" ) )
    and print "ok 12\n"
    or exit;

( $sth->execute )
    and print "ok 13\n"
    or exit;

$oid_status = $sth->{'pg_oid_status'};
( $oid_status ne '' )
    and print "ok 14\n"
    or print "not ok 14\n";

$cmd_status = $sth->{'pg_cmd_status'};
( $cmd_status =~ /^INSERT/ )
    and print "ok 15\n"
    or print "not ok 15\n";

( $sth->finish )
    and print "ok 16\n"
    or exit;

( $sth = $dbh->prepare( "SELECT * FROM builtin" ) )
    and print "ok 17\n"
    or exit;

( $numrows = $sth->execute )
    and print "ok 18\n"
    or exit;

print "not " if $numrows != 3;
print "ok 19\n";

print "not " if $sth->rows != 3;
print "ok 20\n";

print "not " if $DBI::rows != 3;
print "ok 21\n";

@row_ary = $sth->fetchrow_array;
( join(" ", @row_ary) eq 't a Edmund Mergl Edmund Mergl 08-03-1997 1234 {1,2,3} 1.234 (1,2) [(1,2),(3,4)] (3,4),(1,2)' ) 
    and print "ok 22\n"
    or  print "not ok 22\n";

$ary_ref = $sth->fetchrow_arrayref;
( join(" ", @$ary_ref) eq 'f b Halli Hallo Halli Hallo 06-01-1995 5678 {5,6,7} 5.678 (4,5) [(4,5),(6,7)] (6,7),(4,5)' )
    and print "ok 23\n"
    or  print "not ok 23\n";

$hash_ref = $sth->fetchrow_hashref;
( join(" ", (($key,$val) = each %$hash_ref)) eq 'text Potz Blitz' )
    and print "ok 24\n"
    or  print "not ok 24\n";

@name = @{$sth->{'NAME'}};
( join(" ", @name) eq 'bool char char16 text date int4 _int4 float8 point lseg box' )
    and print "ok 25\n"
    or  print "not ok 25\n";

@type = @{$sth->{'TYPE'}};
( join(" ", @type) eq '16 18 20 25 1082 23 1007 701 600 601 603' )
    and print "ok 26\n"
    or  print "not ok 26, this test fails if version != 6.1.1\n";

@size = @{$sth->{'SIZE'}};
( join(" ", @size) eq '1 1 16 -1 4 4 -1 8 16 32 32' )
    and print "ok 27\n"
    or  print "not ok 27\n";

( $sth->execute )
    and print "ok 28\n"
    or exit;

$sth->bind_columns(undef, \$bool, \$char, \$char16, \$text, \$date, \$int4, \$_int4, \$float8, \$point, \$lseg, \$box);
$sth->fetch;
( "$bool, $char, $char16, $text, $date, $int4, $_int4, $float8, $point, $lseg, $box" eq 
  't, a, Edmund Mergl, Edmund Mergl, 08-03-1997, 1234, {1,2,3}, 1.234, (1,2), [(1,2),(3,4)], (3,4),(1,2)' )
    and print "ok 29\n"
    or  print "not ok 29\n";

( $sth->finish )
    and print "ok 30\n"
    or exit;

( $dbh->do( "UPDATE builtin SET int4 = 3 WHERE text = 'Edmund Mergl'" ) )
    and print "ok 31\n"
    or exit;;

( $sth = $dbh->prepare( "UPDATE builtin SET int4 = int4 + 1" ) )
    and print "ok 32\n"
    or exit;

( $sth->execute )
    and print "ok 33\n"
    or exit;

( $sth->finish )
    and print "ok 34\n"
    or exit;

( $dbh->do( "DELETE FROM builtin WHERE int4 = 3" ) )
    and print "ok 35\n"
    or exit;

( $sth = $dbh->tables )
    and print "ok 36\n"
    or exit;

$ary_ref = $sth->fetchrow_arrayref;
( join(" ", @ary_ref) !~ /builtin r f$/ )
    and print "ok 37\n"
    or  print "not ok 37\n";

( $sth->finish )
    and print "ok 38\n"
    or exit;

( $dbh->do( "DROP TABLE builtin" ) )
    and print "ok 39\n"
    or exit;

( $dbh->disconnect )
    and print "ok 40\n"
    or exit;

######################### disconnect and drop test database

( $dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass) )
    and print "ok 41\n"
    or exit;

( $dbh->do( "DROP DATABASE $dbname" ) )
    and print "ok 42\n"
    or exit;

( $dbh->disconnect )
    and print "ok 43\n"
    or exit;


print "test sequence finished.\n";


######################### EOF
