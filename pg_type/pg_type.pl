#!/usr/local/bin/perl

#---------------------------------------------------------
#
# $Id: pg_type.pl,v 1.1 1997/09/15 19:14:09 mergl Exp $
#
# get all OIDs and names from pgsql/src/include/catalog/pg_type.h 
# and write them as array into the module pg_type.pm 
#
#---------------------------------------------------------

if (! $ENV{POSTGRES_HOME}) {
    foreach(qw(/usr/local/pgsql /usr/pgsql /home/pgsql /opt/pgsql /usr/local/postgres /usr/postgres /home/postgres /opt/postgres)) {
        if (-d "$_/src") {
            $ENV{POSTGRES_HOME} = $_;
            last;
        }
    }
}

if (! -d "$ENV{POSTGRES_HOME}/src") {
    die "Unable to find PostgreSQL\n";
}

$PG_TYPE_H = "$ENV{POSTGRES_HOME}/src/include/catalog/pg_type.h";

open PG_TYPE_H,  "<$PG_TYPE_H" || die "can not open $PG_TYPE_H\n";
open PG_TYPE_PM, ">pg_type.pm" || die "can not open pg_type.pm\n";

print PG_TYPE_PM "package DBD::Pg::pg_type;\n\n";
print PG_TYPE_PM "\@pg_type;\n\n";

while (<PG_TYPE_H>) {
    if ( /^DATA/ ) {
        s/^DATA\(insert\s+OID\s+=\s+//;
        s/\(//;
        ($num, $type, $trash) = split;
        next if $type =~ /^pg_/;
        print PG_TYPE_PM "\$pg_type[$num] = '$type';\n";
    }
}

print PG_TYPE_PM "\n1;\n";

close PG_TYPE_H;
close PG_TYPE_PM;
