#!/usr/local/bin/perl -w

#---------------------------------------------------------
#
# $Id: test.pl,v 1.10 1997/10/05 18:25:56 mergl Exp $
#
# Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
# Portions Copyright (c) 1997                Edmund Mergl
#
#---------------------------------------------------------

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

BEGIN { $| = 1; print "1..33\n"; }
END {print "not ok 1\n" unless $loaded;}
use DBI;
$loaded = 1;
print "ok 1\n";

$| = 1;

######################### End of black magic.

# supply userid and password below, if access to 
# your databases is protected in data/pg_hba.conf.

$dbmain = 'template1';
$dbname = 'pgperltest';
$dbuser = '';
$dbpass = '';

######################### create test database

$dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass) or die $DBI::errstr;

$dbh->do("DROP DATABASE $dbname");
$dbh->do("CREATE DATABASE $dbname");

$dbh->disconnect;

######################### create, insert, update, delete, drop

( $dbh = DBI->connect("dbi:Pg:$dbname", $dbuser, $dbpass) )
    and print "ok 2\n"
    or  die $DBI::errstr;

( $dbh->do( "CREATE TABLE builtin ( 
  bool    bool,
  char    char,
  char16  char16,
  text    text,
  date    date,
  int4    int4,
  int4_   int4[],
  float8  float8,
  point   point,
  lseg    lseg,
  box     box
  )" ) )
    and print "ok 3\n"
    or  die $DBI::errstr;

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
    and print "ok 4\n"
    or  die $DBI::errstr;

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
    and print "ok 5\n"
    or  die $DBI::errstr;

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
    and print "ok 6\n"
    or  die $DBI::errstr;

( $sth->execute )
    and print "ok 7\n"
    or die $DBI::errstr;

$oid_status = $sth->{'pg_oid_status'};
( $oid_status ne '' )
    and print "ok 8\n"
    or  print "not ok 8: oid_status = >$oid_status<\n";

$cmd_status = $sth->{'pg_cmd_status'};
( $cmd_status =~ /^INSERT/ )
    and print "ok 9\n"
    or  print "not ok 9: cmd_status = >$cmd_status<\n";

( $sth->finish )
    and print "ok 10\n"
    or  die $DBI::errstr;

( $sth = $dbh->prepare( "SELECT * FROM builtin" ) )
    and print "ok 11\n"
    or  die $DBI::errstr;

( $sth->execute )
    and print "ok 12\n"
    or  die $DBI::errstr;

@row_ary = $sth->fetchrow_array;
( join(" ", @row_ary) eq '1 a Edmund Mergl Edmund Mergl 08-03-1997 1234 {1,2,3} 1.234 (1,2) [(1,2),(3,4)] (3,4),(1,2)' ) 
    and print "ok 13\n"
    or  print "not ok 13: row = ", join(" ", @row_ary), "\n";

$ary_ref = $sth->fetchrow_arrayref;
( join(" ", @$ary_ref) eq '0 b Halli Hallo Halli Hallo 06-01-1995 5678 {5,6,7} 5.678 (4,5) [(4,5),(6,7)] (6,7),(4,5)' )
    and print "ok 14\n"
    or  print "not ok 14: ary_ref = ", join(" ", @$ary_ref), "\n";

$hash_ref = $sth->fetchrow_hashref;
( join(" ", (($key,$val) = each %$hash_ref)) eq 'text Potz Blitz' )
    and print "ok 15\n"
    or  print "not ok 15: key = $key, val = $val\n";

@name = @{$sth->{'NAME'}};
( join(" ", @name) eq 'bool char char16 text date int4 int4_ float8 point lseg box' )
    and print "ok 16\n"
    or  print "not ok 16: name = ", join(" ", @name), "\n";

@type = @{$sth->{'TYPE'}};
( join(" ", @type) eq '16 18 20 25 1082 23 1007 701 600 601 603' )
    and print "ok 17\n"
    or  print "not ok 17: type = ", join(" ", @type), "\n";

@size = @{$sth->{'SIZE'}};
( join(" ", @size) eq '1 1 16 -1 4 4 -1 8 16 32 32' )
    and print "ok 18\n"
    or  print "not ok 18: size = ", join(" ", @size), "\n";

print "not " if $sth->rows != 3;
print "ok 19\n";

print "not " if $DBI::rows != 3;
print "ok 20\n";

( $sth->execute )
    and print "ok 21\n"
    or die $DBI::errstr;

$sth->bind_columns(undef, \$bool, \$char, \$char16, \$text, \$date, \$int4, \$int4_, \$float8, \$point, \$lseg, \$box);
$sth->fetch;
( "$bool, $char, $char16, $text, $date, $int4, $int4_, $float8, $point, $lseg, $box" eq 
  '1, a, Edmund Mergl, Edmund Mergl, 08-03-1997, 1234, {1,2,3}, 1.234, (1,2), [(1,2),(3,4)], (3,4),(1,2)' )
    and print "ok 22\n"
    or  print "not ok 22: $bool, $char, $char16, $text, $date, $int4, $int4_, $float8, $point, $lseg, $box\n";

( $sth->finish )
    and print "ok 23\n"
    or  die $DBI::errstr;

( $dbh->do( "UPDATE builtin SET int4 = 3 WHERE text = 'Edmund Mergl'" ) )
    and print "ok 24\n"
    or  die $DBI::errstr;

( $sth = $dbh->prepare( "UPDATE builtin SET int4 = int4 + 1" ) )
    and print "ok 25\n"
    or  die $DBI::errstr;

( 3 == $sth->execute )
    and print "ok 26\n"
    or  print "not ok 26\n";

( $sth->finish )
    and print "ok 27\n"
    or die $DBI::errstr;

( 1 == $dbh->do( "DELETE FROM builtin WHERE int4 = 4" ) )
    and print "ok 28\n"
    or  print "not ok 28\n";

( $sth = $dbh->tables )
    and print "ok 29\n"
    or  die $DBI::errstr;

$ary_ref = $sth->fetchrow_arrayref;
( join(" ", @ary_ref) !~ /builtin r f$/ )
    and print "ok 30\n"
    or  print "not ok 30: ary_ref = ", @ary_ref, "\n";

( $sth->finish )
    and print "ok 31\n"
    or  die $DBI::errstr;

( $dbh->do( "DROP TABLE builtin" ) )
    and print "ok 32\n"
    or  die $DBI::errstr;

( $dbh->disconnect )
    and print "ok 33\n"
    or  die $DBI::errstr;

######################### disconnect and drop test database

$dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass) or die $DBI::errstr;

$dbh->do("DROP DATABASE $dbname");

$dbh->disconnect;

print "test sequence finished.\n";


######################### EOF
