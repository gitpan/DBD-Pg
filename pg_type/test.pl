#!/usr/local/bin/perl

#---------------------------------------------------------
#
# $Id: test.pl,v 1.5 1998/03/03 21:05:02 mergl Exp $
#
#---------------------------------------------------------

use DBI;
use pg_type;

$dbmain = 'template1';
$dbname = 'pgperltest';
$dbuser = '';
$dbpass = '';

my $dbh = DBI->connect("dbi:Pg:dbname=$dbmain", $dbuser, $dbpass);
$dbh->do( "CREATE DATABASE $dbname" );
$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:dbname=$dbname", $dbuser, $dbpass);

$dbh->do( "CREATE TABLE builtin(
  bool_    bool,
  char_    char,
  char16_  char16,
  text_    text,
  date_    date,
  int4_    int4,
  int4a_   int[],
  float8_  float8,
  point_   point,
  lseg_    lseg,
  box_     box
  )" );

$dbh->do( "INSERT INTO builtin VALUES(
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
  )" );

$sth = $dbh->prepare( "SELECT * FROM builtin" );

$sth->execute;

print "NAME:\n";
@name = @{$sth->{'NAME'}};
foreach $key (@name) {
     print "$key ";
}
print "\n";

#################################################
print "TYPE:\n";
@type = @{$sth->{'TYPE'}};
foreach $key (@type) {
     print "$DBD::Pg::pg_type::pg_type[$key]  ";
}
print "\n";
#################################################

$sth->finish;

$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:dbname=$dbmain", $dbuser, $dbpass);
$dbh->do( "DROP DATABASE $dbname" );
$dbh->disconnect;

# end of test.pl
