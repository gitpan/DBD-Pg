#!/usr/local/bin/perl -w

#---------------------------------------------------------
#
# $Id: test.pl,v 1.2 1997/10/05 18:26:13 mergl Exp $
#
#---------------------------------------------------------

use DBI;
use pg_type;

$dbmain = 'template1';
$dbname = 'pgperltest';
$dbuser = '';
$dbpass = '';

my $dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass);
$dbh->do( "CREATE DATABASE $dbname" );
$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:$dbname", $dbuser, $dbpass);

$dbh->do( "CREATE TABLE builtin(
  bool    bool,
  char    char,
  char16  char16,
  text    text,
  date    date,
  int4    int4,
  _int4   int[],
  float8  float8,
  point   point,
  lseg    lseg,
  box     box
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
     print "$DBD::Pg::pg_type::pg_type[$key] ";
}
print "\n";
#################################################

$sth->finish;

$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass);
$dbh->do( "DROP DATABASE $dbname" );
$dbh->disconnect;

# end of test.pl
