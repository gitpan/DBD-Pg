#---------------------------------------------------------
#
# $Id: Pg.pm,v 1.3 1997/08/12 02:43:38 mergl Exp $
#
#  Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
#  Portions Copyright (c) 1997                Edmund Mergl
#
#---------------------------------------------------------

require 5.002;

{
    package DBD::Pg;

    use DBI ();
    use DynaLoader ();
    @ISA = qw(DynaLoader);

    $VERSION = '0.50';

    require_version DBI 0.85;

    bootstrap DBD::Pg $VERSION;

    $err = 0;		# holds error code   for DBI::err
    $errstr = "";	# holds error string for DBI::errst
    $drh = undef;	# holds driver handle once initialised

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
    use strict;

    sub errstr {
	return $DBD::Pg::errstr;
    }

    sub connect {
	my($drh, $dbname, $user, $auth)= @_;

	# create a 'blank' dbh

	my $this = DBI::_new_dbh($drh, {
	    'Name' => $dbname,
	    'User' => $user,
	});

        # Connect to the database..
	DBD::Pg::db::_login($this, $dbname, $user, $auth)
	    or return undef;

	$this;
    }

}


{   package DBD::Pg::db; # ====== DATABASE ======
    use strict;

    sub errstr {
	return $DBD::Pg::errstr;
    }

    sub do {
        my($dbh, $statement, @attribs) = @_;

        DBD::Pg::db::_do($dbh, $statement, @attribs);
    }

    sub prepare {
	my($dbh, $statement, @attribs)= @_;

	# create a 'blank' sth

	my $sth = DBI::_new_sth($dbh, {
	    'Statement' => $statement,
	});

	DBD::Pg::st::_prepare($sth, $statement, @attribs)
	    or return undef;

	$sth;
    }

    sub tables {
	my($dbh) = @_;
	my $sth = $dbh->prepare("
            select usename, relname, relkind, relhasrules 
	    from pg_class, pg_user 
	    where ( relkind = 'r' or relkind = 'i' or relkind = 'S' ) 
	    and relname !~ '^pg_' 
	    and relname !~ '^xin[vx][0-9]+' 
	    and usesysid = relowner 
	    ORDER BY relname 
        ");
	$sth->execute or return undef;
	$sth;
    }

}


{   package DBD::Pg::st; # ====== STATEMENT ======
    use strict;

    sub errstr {
	return $DBD::Pg::errstr;
    }

}

1;

__END__


=head1 NAME

DBD::Pg - PostgreSQL database driver for the DBI module


=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:Pg:$dbname", $user, $passwd);

  # See the DBI module documentation for full details


=head1 DESCRIPTION

DBD::Pg is a Perl module which works with the DBI module to provide
access to PostgreSQL databases.


=head1 CONNECTING TO POSTGRESQL

To connect to a database you can say:

	$dbh = DBI->connect('dbi:Pg:DB', 'username', 'password');

The first parameter specifies the driver and the database. 
The second and third parameter specify the username and 
password. This returns a database handle which can be used 
for subsequent database interactions.


=head1 SIMPLE STATEMENTS

Given a database connection, you can execute an arbitrary 
statement using: 

	$dbh->do($stmt);

The statement must not be a SELECT statement (except SELECT...INTO TABLE).


=head1 PREPARING AND EXECUTING STATEMENTS

You can prepare a statement for multiple uses, and you can do this for
SELECT statements which return data as well as for statements which return 
no data. You create a statement handle using:

	$sth = $dbh->prepare($stmt);

Once the statement is prepared, you can execute it:

	$numrows = $sth->execute;

For statements which return data, $numrows is the number 
of selected rows. You can retrieve the values in the 
following way:

	while ($ary_ref = $sth->fetch) {

	}

Another possibility is to bind the fields of a select statement to 
perl variables. Whenever a row is fetched from the database the 
corresponding perl variables will be automatically updated: 

	$sth->bind_columns(undef, @list_of_refs_to_vars_to_bind);
	while ($sth->fetch) {

	}

When you have fetched as many rows as required, you close the statement 
handle using:

	$sth->finish;

This frees the statement, but it does not free the related data structures. 
This is done when you destroy (undef) the statement handle:

	undef $sth;


=head1 DISCONNECTING FROM A DATABASE

You can disconnect from the database:

	$dbh->disconnect;

Note that this does not destroy the database handle. You 
need to do an explicit 'undef $dbh' to destroy the handle. 


=head1 DYNAMIC ATTRIBUTES

The following attributes are supported:

	$DBI::err	# error status

	$DBI::errstr	# error message

	$DBI::rows	# row count


=head1 STATEMENT HANDLE ATTRIBUTES

For statement handles of a select statement you can 
discover what the returned column names, types, sizes are:

	@name = @{$sth->{'NAME'}};	# Column names
	@type = @{$sth->{'TYPE'}};	# Data types
	@size = @{$sth->{'SIZE'}};	# Numeric size

There is also support for two PostgreSQL-specific attributes: 

	$oid_status = $sth->{'pg_oid_status'};	# oid of last insert
	$cmd_status = $sth->{'pg_cmd_status'};	# type of last command


=head1 TRANSACTIONS

PostgreSQL supports simple transactions. They can not be named 
and they can not be nested ! You start a transaction with: 

	$dbh->do('begin');

The transaction can be aborted or finished with:

	$dbh->do('abort');
	$dbh->do('end');

Note that the following functions can also be used:

	$dbh->rollback;
	$dbh->commit;


=head1 BLOBS

Blobs are not fully supported. The only way is 
to use the two registered built-in functions 
lo_import() and lo_export(). See the large_objects 
man page for further information. 


=head1 KNOWN RESTRICTIONS

=item *
PostgreSQL does not has the concept of preparing 
a statement. Here the prepare method just stores 
the statement. 

=item *
Currently PostgreSQL does not return the number of 
affected rows for non-select statements. 

=item *
Transactions can not be named and not be nested. 

=item *
Although PostgreSQL has a cursor concept, it has not 
been used in the current implementation. Cursors in 
PostgreSQL can only be used inside a transaction block. 
Because transactions in PostgreSQL can not be nested, 
this would have implied the restriction, not to use 
any nested SELECT statements. Hence the execute method 
fetches all data at once into data structures located 
in the frontend application. This has to be considered 
when selecting large amounts of data ! 

=item *
$DBI::state is not supported.

=item *
$sth->bind_param() is not supported.

=item *
Some statement handle attributes are not supported.


=head1 SEE ALSO

L<DBI>


=head1 AUTHORS

=item *
DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

=item *
DBD-Pg by Edmund Mergl (E.Mergl@bawue.de)

 Major parts of this package have been copied from DBD-Oracle.


=head1 COPYRIGHT

The DBD::Pg module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.


=head1 ACKNOWLEDGEMENTS

See also L<DBI/ACKNOWLEDGEMENTS>.

=cut
