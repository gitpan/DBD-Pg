#---------------------------------------------------------
#
# $Id: Makefile.PL,v 1.2 1997/08/09 16:01:11 mergl Exp $
#
# Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
# Portions Copyright (c) 1997                Edmund Mergl
#
#---------------------------------------------------------

use ExtUtils::MakeMaker;
use Config;
use strict;


if (! $ENV{POSTGRES_HOME}) {
    print "\$POSTGRES_HOME not defined. Searching for Postgres...\n";
    foreach(qw(/usr/local/pgsql /usr/pgsql /home/pgsql /opt/pgsql /usr/local/postgres /usr/postgres /home/postgres /opt/postgres)) {
        if (-d "$_/lib") {
            $ENV{POSTGRES_HOME} = $_;
            last;
        }
    }
}

if (-d "$ENV{POSTGRES_HOME}/lib") {
    print "Found Postgres in $ENV{POSTGRES_HOME}\n";
} else {
    die "Unable to find Postgres !\n";
}


WriteMakefile (
    'NAME'         => 'DBD::Pg',
    'VERSION_FROM' => 'Pg.pm',
    'LIBS'	   => ["-L$ENV{POSTGRES_HOME}/lib -lpq"],
    'INC'	   =>  "-I$ENV{POSTGRES_HOME}/include -I$Config{archlib}/DBI -I$Config{sitearch}/auto/DBI",
    'OBJECT'       => 'Pg$(OBJ_EXT) dbdimp$(OBJ_EXT)',
);

# EOF
