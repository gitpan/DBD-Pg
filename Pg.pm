#---------------------------------------------------------
#
# $Id: Pg.pm,v 1.18 1998/03/06 21:18:35 mergl Exp $
#
#  Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
#  Portions Copyright (c) 1997,1998           Edmund Mergl
#
#---------------------------------------------------------

require 5.002;

{
    package DBD::Pg;

    use DBI ();
    use DynaLoader ();
    @ISA = qw(DynaLoader);

    $VERSION = '0.69';

    require_version DBI 0.91;

    bootstrap DBD::Pg $VERSION;

    $err = 0;		# holds error code   for DBI::err
    $errstr = "";	# holds error string for DBI::errstr
    $drh = undef;	# holds driver handle once initialized

    sub driver{
	return $drh if $drh;
	my($class, $attr) = @_;
	$class .= "::dr";
	# not a 'my' since we use it above to prevent multiple drivers
	($drh) = DBI::_new_drh($class, {
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
	my($this) = DBI::_new_dbh($drh, {
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

    sub ping {
        my($dbh) = @_;
        local $dbh->{RaiseError} = 0 if $dbh->{RaiseErrror};
        DBD::Pg::db::_ping($dbh);
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
            select c.relname 
	    from pg_class c, pg_user u 
	    where ( c.relkind = 'r' or c.relkind = 'i' or c.relkind = 'S' ) 
	    and c.relname !~ '^pg_' 
	    and c.relname !~ '^xin[vx][0-9]+' 
	    and c.relowner = u.usesysid 
	    ORDER BY c.relname 
        ");
	$sth->execute or return undef;
	$sth;
    }

    sub attributes {
	my($dbh,$table) = @_;
	my $sth = $dbh->prepare("
            select a.attnum, a.attname, t.typname, a.attlen 
	    from pg_class c, pg_attribute a, pg_type t 
	    where c.relname = '$table' 
	    and a.attnum > 0 
	    and a.attrelid = c.oid 
	    and a.atttypid = t.oid 
	    ORDER BY attnum 
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

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname", $user, $passwd);

  # See the DBI module documentation for full details


=head1 DESCRIPTION

DBD::Pg is a Perl module which works with the DBI module to provide
access to PostgreSQL databases.


=head1 CONNECTING TO POSTGRESQL

To connect to a database you can say:

	$dbh = DBI->connect('dbi:Pg:dbname=dbname;host=host;port=port', 'username', 'password');

The first parameter specifies the driver, the database and 
the optional host and port. The second and third parameter 
specify the username and password. Note that for these two 
parameters DBI distinguishes between empty and undefined. 
If these parameters are undefined DBI substitutes the values 
of the DBI_USER and DBI_PASS environment variables. The 
connect method returns a database handle which can be used for 
subsequent database interactions. Please refer to the pg_passwd 
man-page for the different types of authorization. 

This module also supports the ping-method, which can be used 
to check the validity of a database-handle. 


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

	$rv = $sth->execute;

$rv is the number of selected / affected rows. You can retrieve the values 
in the following way:

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

The transaction behavior is now controlled with the attribute AutoCommit. 
For a complete definition of AutoCommit please refer to the DBI documentation. 

According to the DBI specification the default for AutoCommit is TRUE. 
In this mode, any change to the database becomes valid immediately. Any 
'begin', 'commit' or 'rollback' statement will be rejected. 

If AutoCommit is switched-off, immediately a transaction will be started by 
issuing a 'begin' statement. Any 'commit' or 'rollback' will start a new 
transaction. A disconnect will issue a 'rollback' statement. 

In case your scripts do not use transactions, no changes are necessary. 
If your scripts make use of transactions, you have to adapt them to the 
AutoCommit feature. In most cases it is be sufficient, to remove the 
'begin' statements and to switch-off the AutoCommit mode. 


=head1 Meta-Information

The driver supports two simple methods to get meta-information about 
the available tables and their attributes:

	$dbh->tables;
	$dbh->DBD::Pg::db::attributes($table);

Because the second one is not (yet) supported by DBI you have to use the 
complete name including the package. The first method returns all tables 
which are owned by the current user. The second method returns for the 
given table a unique number, the name, the type, and the length of every 
attribute. 


=head1 DATA TYPE bool

The current implementation of PostgreSQL returns 't' for true and 'f' for 
false. From the perl point of view a rather unfortunate choice. The DBD-Pg 
module translates the result for the data-type bool in a perl-ish like manner: 
'f' -> '0' and 't' -> '1'. This way the application does not have to check 
the database-specific returned values for the data-type bool, because perl 
treats '0' as false and '1' as true. If you make use of the data-type bool 
you have to adapt your scripts !

Starting with version 6.3 PostgreSQL will consider 1 and '1' as input for 
the boolean data-type as true. In older versions everything except 't' is 
considerd as false. 


=head1 BLOBS

Support for blob_read is provided. For further 
information and examples please read the files 
in the blobs subdirectory.
In addition you can use the two registered built-in 
functions lo_import() and lo_export(). See the 
L<large_objects> for further information and 
examples. 


=head1 KNOWN RESTRICTIONS

=item *
PostgreSQL does not has the concept of preparing 
a statement. Here the prepare method just stores 
the statement. 

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
Some statement handle attributes are not supported.


=head1 SEE ALSO

L<DBI>


=head1 AUTHORS

=item *
DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

=item *
DBD-Pg by Edmund Mergl (E.Mergl@bawue.de)

 Major parts of this package have been copied from DBI and DBD-Oracle.


=head1 COPYRIGHT

The DBD::Pg module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.


=head1 ACKNOWLEDGMENTS

See also L<DBI/ACKNOWLEDGMENTS>.

=cut
