
# $Id: Makefile.PL,v 1.20 1999/02/14 20:33:24 mergl Exp $

use ExtUtils::MakeMaker;
use Config;
use strict;
use DBI 1.00;
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
    die "Unable to find PostgreSQL\nplease set environment variable POSTGRES_HOME\n";
}


my %opts = (
    NAME         => 'DBD::Pg',
    VERSION_FROM => 'Pg.pm',
    INC          => "-I$ENV{POSTGRES_HOME}/include -I/usr/local/include/pgsql -I/usr/include/pgsql -I\$(INSTALLSITEARCH)/auto/DBI -I\$(INSTALLARCHLIB)/DBI -I\$(SITEARCH)/auto/DBI -I\$(ARCHLIB)/DBI",
    OBJECT       => "Pg\$(OBJ_EXT) dbdimp\$(OBJ_EXT)",
    LIBS         => ["-L$ENV{POSTGRES_HOME}/lib -lpq -L/usr/lib -lpq"],

);

#if ($os eq 'hpux') {
#    if ($osvers < 10) {
#        print "Warning: Forced to build static not dynamic on $os $osvers.\a\n"
#        $opts{LINKTYPE} = 'static';
#    }
#}

if ($Config{dlsrc} =~ /dl_none/) {
    $opts{LINKTYPE} = 'static';
}


WriteMakefile(%opts);

exit(0);

# end of Makefile.PL
