#-------------------------------------------------------
#
# $Id: Pg.pm,v 1.2 1997/03/13 21:03:07 mergl Exp $
#
#   Portions Copyright (c) 1994,1995,1996  Tim Bunce
#   Portions Copyright (c) 1997            Edmund Mergl
#
#-------------------------------------------------------

require 5.003;


{
    package DBD::Pg;

    use DBI ();
    use DynaLoader ();
    @ISA = qw(DynaLoader);

    $VERSION = '0.1';

    require_version DBI 0.71;

    bootstrap DBD::Pg $VERSION;

    $drh = undef;	# holds driver handle once initialised
    $err = 0;		# holds error code   for DBI::err
    $errstr = "";	# holds error string for DBI::errstr

    sub driver{
	return $drh if $drh;
	my($class, $attr) = @_;

	$class .= "::dr";

	# not a 'my' since we use it above to prevent multiple drivers

	$drh = DBI::_new_drh($class, {
	    'Name' => 'Pg',
	    'Version' => $VERSION,
	    'Err'    => \$DBD::Pg::err,
	    'Errstr' => \$DBD::Pg::errstr,
	    'Attribution' => 'PostgreSQL DBD by Edmund Mergl',
	    });
	$drh;
    }

    1;
}


{   package DBD::Pg::dr; # ====== DRIVER ======
    $imp_data_size = 0;
    use strict;

    sub errstr {
	DBD::Pg::errstr(@_);
    }

    sub connect {
	my($drh, $dbname, $user, $auth)= @_;

	# create a 'blank' dbh

	my $this = DBI::_new_dbh($drh, {
	    'Name' => $dbname,
	    'USER' => $user, 'CURRENT_USER' => $user,
	    });


        # Connect to the database..
	DBD::Pg::db::_login($this, $dbname, $user, $auth)
	    or return undef;

	$this;
    }

}


{   package DBD::Pg::db; # ====== DATABASE ======
    $imp_data_size = 0;
    use strict;

    sub errstr {
	DBD::Pg::errstr(@_);
    }

    sub  rows {
        DBD::Pg::rows(@_);
    }

    sub do {
        my($dbh, $statement, $attribs, @params) = @_;

        Carp::carp "\$dbh->do() attribs unused\n" if $attribs;
	Carp::carp "\$dbh->do() params unused\n" if @params;

        DBD::Pg::db::_do($dbh, $statement);
    }

    sub prepare {
	my($dbh, $statement, $attribs)= @_;

        Carp::carp "\$sth->prepare() attribs unused\n" if $attribs;

	# create a 'blank' sth

	my $sth = DBI::_new_sth($dbh, {
	    'Statement' => $statement,
	    });

	DBD::Pg::st::_prepare($sth, $statement)
	    or return undef;

	$sth;
    }

}


{   package DBD::Pg::st; # ====== STATEMENT ======
    $imp_data_size = 0;
    use strict;

    sub errstr {
	DBD::Pg::errstr(@_);
    }

    sub rows {
        DBD::Pg::rows(@_);
    }
}

1;

__END__


=head1 NAME

DBD-Pg - Access to PostgreSQL Databases


=head1 SYNOPSIS

  use DBD::Pg;


=head1 DESCRIPTION

DBD::Pg is an extension to Perl which allows access to PostgreSQL
databases. It is built on top of the standard DBI extension and 
implements some of the methods that DBI defines.


=head2 LOADING THE MODULE

As for any DBI driver you need to load the DBI module:

	use DBI;


=head2 CONNECTING TO A DATABASE

To connect to a database, you can specify:

	$dbh = $drh->connect($database, '', '', 'Pg');

The first parameter specifies the database. The second 
and third parameter (username and password) are not 
supported by PostgreSQL. The last parameter specifies 
the driver to be used. This returns a database handle 
which can be used for subsequent database interactions.


=head2 SIMPLE STATEMENTS

Given a database connection, you can execute an arbitrary 
statement using: 

	$dbh->do($stmt);

The statement must not be a SELECT statement (except SELECT...INTO TABLE).


=head2 PREPARING AND EXECUTING STATEMENTS

You can prepare a statement for multiple uses, and you can do this for
SELECT statements which return data as well as for statements which return 
no data. You create a statement handle using:

	$sth = $dbh->prepare($stmt);

Once the statement is prepared, you can execute it:

	$numrows = $sth->execute;

For statements which return data, $numrows is the number 
of selected rows. You can also discover what the returned 
column names, types, sizes are:

	@name = @{$sth->{'NAME'}};	# Column names
	@type = @{$sth->{'TYPE'}};	# Data types
	@size = @{$sth->{'SIZE'}};	# Numeric size

If the statement returns data, you can retrieve the values in the 
following way:

	while (@row = $sth->fetchrow) {

	}

When you have fetched as many rows as required, you close the statement 
handle using:

	$sth->finish;

This frees the statement, but it does not free the related data structures. 
This is done when you destroy (undef) the statement handle:

	undef $sth;



=head2 DISCONNECTING FROM A DATABASE

You can disconnect from the database:

	$dbh->disconnect;

Note that this does not destroy the database handle. You 
need to do an explicit 'undef $dbh' to destroy the handle. 


=head2 TRANSACTIONS

PostgreSQL supports simple transactions. They can not be named 
and they can not be nested ! You start a transaction with: 

	$dbh->do('begin');

The transaction can be aborted or finished with:

	$dbh->do('abort');
	$dbh->do('end');

Note that the following functions can also be used:

	$dbh->rollback;
	$dbh->commit;


=head2 CURSORS

Although PostgreSQL has a cursor concept, it has not 
been used in the current implementation. Cursors in 
PostgreSQL can only be used inside a transaction block. 
Because transactions in PostgreSQL can not be nested, 
this would have implied the restriction, not to use 
any nested SELECT statements. 


=head2 DYANMIC ATTRIBUTES

The following attributes are supported:

	$DBI::err	# error message

	$DBI::errstr	# error code

	$DBI::rows	# row count


=head1 KNOWN RESTRICTIONS

=item *
disconnect_all is not supported.

=item *
$DBI::state is not supported

=item *
Some driver attributes cannot be queried.

=item *
Blobs are not supported.


=head1 AUTHORS

=item *
DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

=item *
DBD-Pg by Edmund Mergl (E.Mergl@bawue.de)

 Major parts of this package have been copied from DBD-Oracle.


=head1 SEE ALSO

DBI(1).

=cut
