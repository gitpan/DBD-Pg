#!/usr/local/bin/perl -w

#---------------------------------------------------------
#
# $Id: test.pl,v 1.3 1997/10/05 18:27:49 mergl Exp $
#
#---------------------------------------------------------

use DBI;

$dbmain = 'template1';
$dbname = 'pgperltest';
$dbuser = '';
$dbpass = '';

$cwd = `pwd`;
chop $cwd;
$lobject = $cwd . '/README';

my $dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass);
$dbh->do( "CREATE DATABASE $dbname" );
$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:$dbname", $dbuser, $dbpass);

$dbh->do("CREATE TABLE lobject ( id int4, loid oid )");

$dbh->do("INSERT INTO lobject (id, loid) VALUES (1, lo_import('$lobject') )");

$sth = $dbh->prepare("SELECT loid FROM lobject WHERE id = 1");
$sth->execute;
($lobj_id) = $sth->fetchrow_array;

$blob = $sth->blob_read($lobj_id, 200, 80);
print $blob, "\n";

$blob = $sth->blob_read($lobj_id, 0, 0);
print $blob, "\n";

$sth->finish;

$dbh->disconnect;

$dbh = DBI->connect("dbi:Pg:$dbmain", $dbuser, $dbpass);
$dbh->do( "DROP DATABASE $dbname" );
$dbh->disconnect;

# end of test.pl
