#!perl

## Simply test that we can load the DBI and DBD::PG modules,
## Check that we have a valid version returned from the latter

use strict;
use warnings;
use Test::More tests => 3;
select(($|=1,select(STDERR),$|=1)[1]);

## For quick testing, put new tests as 000xxx.t and set this:
if (exists $ENV{DBDPG_QUICKTEST} and $ENV{DBDPG_QUICKTEST}) {
	BAIL_OUT 'Stopping due to DBDPG_QUICKTEST being set';
}

BEGIN {
	use_ok('DBI') or BAIL_OUT 'Cannot continue without DBI';
	use_ok('DBD::Pg') or BAIL_OUT 'Cannot continue without DBD::Pg';
}
use DBD::Pg;
like( $DBD::Pg::VERSION, qr/^[\d\._]+$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});
