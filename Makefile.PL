#---------------------------------------------------------
#
# $Id: Makefile.PL,v 1.9 1998/03/03 21:04:52 mergl Exp $
#
# Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
# Portions Copyright (c) 1997,1998           Edmund Mergl
#
#---------------------------------------------------------

use ExtUtils::MakeMaker;
use Config;
use strict;
use DBI 0.91;
use DBI::DBD;


print "\nConfiguring Pg\n";
print "Remember to actually read the README file !\n";
die "\nYou didn't read the README file !\n" unless ($] >= 5.002);

if (! $ENV{POSTGRES_HOME}) {
    print "\$POSTGRES_HOME not defined. Searching for PostgreSQL...\n";
    foreach(qw(/usr/local/pgsql /usr/lib/pgsql /usr/pgsql /home/pgsql /opt/pgsql /usr/local/postgres /usr/lib/postgres /usr/postgres /home/postgres /opt/postgres /usr/local/postgresql /usr/lib/postgresql /usr/postgresql /home/postgresql /opt/postgresql)) {
        if (-d "$_/lib") {
            $ENV{POSTGRES_HOME} = $_;
            last;
        }
    }
}

if (-d "$ENV{POSTGRES_HOME}/lib") {
    print "Found PostgreSQL in $ENV{POSTGRES_HOME}\n";
} else {
    die "Unable to find PostgreSQL\n";
}


my %opts = (
    NAME         => 'DBD::Pg',
    VERSION_FROM => 'Pg.pm',
    clean        => { FILES => 'Pg.xsi' },
    dist         => { DIST_DEFAULT => 'clean distcheck disttest ci tardist',
                      PREOP        => '$(MAKE) -f Makefile.old distdir' },
    INC          =>  "-I$ENV{POSTGRES_HOME}/include -I$Config{archlib}/DBI -I$Config{sitearch}/auto/DBI",
    LIBS         => ["-L$ENV{POSTGRES_HOME}/lib -lpq"],
    OBJECT       => 'Pg$(OBJ_EXT) dbdimp$(OBJ_EXT)',
);


WriteMakefile(%opts);

exit(0);

# end of Makefile.PL
