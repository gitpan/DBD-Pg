
#  $Id: Pg.pm,v 1.32 1999/01/16 05:56:32 mergl Exp $
#
#  Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
#  Portions Copyright (c) 1997,1998           Edmund Mergl
#
#  You may distribute under the terms of either the GNU General Public
#  License or the Artistic License, as specified in the Perl README file,
#  with the exception that it cannot be placed on a CD-ROM or similar media
#  for commercial distribution without the prior approval of the author.


require 5.003;

$DBD::Pg::VERSION = '0.90';

{
    package DBD::Pg;

    use DBI ();
    use Exporter ();
    @ISA = qw(DynaLoader Exporter);

    require_version DBI 1.00;

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

    sub data_sources {
        my $drh = shift;
        my $dbh = DBD::Pg::dr::connect($drh, 'dbname=template1') or return undef;
        $dbh->{AutoCommit} = 1;
        my $sth = $dbh->prepare("SELECT datname FROM pg_database ORDER BY datname") or return undef;
        $sth->execute or return undef;
        my (@sources, @datname);
        while (@datname = $sth->fetchrow_array) {
            push @sources, "dbi:Pg:dbname=$datname[0]";
        }
        $sth->finish;
        $dbh->disconnect;
        return @sources;
    }

    sub connect {
        my($drh, $dbname, $user, $auth)= @_;

        # create a 'blank' dbh

        my $Name = $dbname;
        $Name =~ s/^.*dbname\s*=\s*//;
        $Name =~ s/\s*;.*$//;

        $user = '' unless defined($user);
        $auth = '' unless defined($auth);

        my($dbh) = DBI::_new_dbh($drh, {
            'Name' => $Name,
            'User' => $user, 'CURRENT_USER' => $user,
        });

        # Connect to the database..
        DBD::Pg::db::_login($dbh, $dbname, $user, $auth) or return undef;

        $dbh;
    }

}


{   package DBD::Pg::db; # ====== DATABASE ======
    use strict;

    sub prepare {
        my($dbh, $statement, @attribs)= @_;

        # create a 'blank' sth

        my $sth = DBI::_new_sth($dbh, {
            'Statement' => $statement,
        });

        DBD::Pg::st::_prepare($sth, $statement, @attribs) or return undef;

        $sth;
    }

    sub ping {
        my($dbh) = @_;

        local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
        my $ret = DBD::Pg::db::_ping($dbh);

        return $ret;
    }

    sub table_info {
        my($dbh) = @_;

        my $sth = $dbh->prepare("
            SELECT c.reltype, u.usename, c.relname, 'TABLE', '' 
            FROM pg_class c, pg_user u 
            WHERE c.relkind = 'r' 
            AND   c.relhasrules = FALSE
            AND   c.relname !~ '^pg_' 
            AND   c.relname !~ '^xin[vx][0-9]+' 
            AND   c.relowner = u.usesysid 
            UNION
            SELECT c.reltype, u.usename, c.relname, 'VIEW', '' 
            FROM pg_class c, pg_user u 
            WHERE c.relkind = 'r' 
            AND   c.relhasrules = TRUE
            AND   c.relname !~ '^pg_' 
            AND   c.relname !~ '^xin[vx][0-9]+' 
            AND   c.relowner = u.usesysid 
            ORDER BY 1, 2, 3
        ") or return undef;
        $sth->execute or return undef;

        $sth;
    }

    sub tables {
        my($dbh) = @_;

        my $sth = $dbh->prepare("
            SELECT relname 
            FROM pg_class 
            WHERE relkind = 'r' 
            AND   relname !~ '^pg_' 
            AND   relname !~ '^xin[vx][0-9]+' 
            ORDER BY relname 
        ") or return undef;
        $sth->execute or return undef;
        my (@tables, @relname);
        while (@relname = $sth->fetchrow_array) {
            push @tables, $relname[0];
        }
        $sth->finish;

        return @tables;
    }

    sub attributes {
        my($dbh, $table) = @_;

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

    sub type_info_all {
        my ($dbh) = @_;
	my $names = {
          TYPE_NAME		=> 0,
          DATA_TYPE		=> 1,
          PRECISION		=> 2,
          LITERAL_PREFIX	=> 3,
          LITERAL_SUFFIX	=> 4,
          CREATE_PARAMS		=> 5,
          NULLABLE		=> 6,
          CASE_SENSITIVE	=> 7,
          SEARCHABLE		=> 8,
          UNSIGNED_ATTRIBUTE	=> 9,
          MONEY			=>10,
          AUTO_INCREMENT	=>11,
          LOCAL_TYPE_NAME	=>12,
          MINIMUM_SCALE		=>13,
          MAXIMUM_SCALE		=>14,
        };

	#  typname       |typlen|typprtlen|    SQL92
	#  --------------+------+---------+    -------
	#  bool          |     1|        1|    BOOL
	#  bpchar        |    -1|       -1|    CHAR(n)    bp=blank padded
	#  varchar       |    -1|       -1|    VARCHAR(n)
	#  int2          |     2|        5|    SMALLINT
	#  int4          |     4|       10|    INT
	#  float4        |     4|       12|    FLOAT(p)   for p<7=float4, for p<16=float8
	#  float8        |     8|       24|    REAL
	#  date          |     4|       10|    /
	#  time          |     8|       16|    /
	#  timespan      |    12|       47|    TINTERVAL
	#  timestamp     |     4|       19|    TIMESTAMP
	#  --------------+------+---------+

	my $ti = [
	  $names,
          # name         type   prec  prefix suffix    create params null case se unsign mon  incr      local   min    max
          [ 'BOOL',      undef,    1,  '\'',  '\'',             undef, 1, '0', 3, undef, '0', '0',      undef, undef, undef ],
          [ 'CHAR',          1, 4096,  '\'',  '\'',      'max length', 1, '1', 3, undef, '0', '0',   'bpchar', undef, undef ],
          [ 'VARCHAR',      12, 4096,  '\'',  '\'',      'max length', 1, '1', 3, undef, '0', '0',      undef, undef, undef ],
          [ 'SMALLINT',  undef,    5, undef, undef,             undef, 1, '0', 3,   '0', '0', '0',     'int2', undef, undef ],
          [ 'INT',       undef,   10, undef, undef,             undef, 1, '0', 3,   '0', '0', '0',     'int4', undef, undef ],
          [ 'FLOAT',     undef,   12, undef, undef,       'precision', 1, '0', 3,   '0', '0', '0',   'float4', undef, undef ],
          [ 'REAL',      undef,   24, undef, undef,             undef, 1, '0', 3,   '0', '0', '0',   'float8', undef, undef ],
          [ 'DATE',      undef,   10,  '\'',  '\'',             undef, 1, '0', 3, undef, '0', '0',      undef,   '0',   '0' ],
          [ 'TIME',      undef,   16,  '\'',  '\'',             undef, 1, '0', 3, undef, '0', '0',      undef,   '0',   '0' ],
          [ 'TINTERVAL', undef,   47,  '\'',  '\'',             undef, 1, '0', 3, undef, '0', '0', 'timespan',   '0',   '0' ],
          [ 'TIMESTAMP', undef,   19,  '\'',  '\'',             undef, 1, '0', 3, undef, '0', '0',      undef,   '0',   '0' ]
        ];
	return $ti;
    }

    sub quote {
	my ($dbh, $str, $data_type) = @_;

        return "NULL" unless defined $str;
       
        $str =~ s/\\/\\\\/g;
        $str=~s/'/''/g;

        return "'$str'";
    }
}


{   package DBD::Pg::st; # ====== STATEMENT ======

    # all done in XS
}

1;

__END__


=head1 NAME

DBD::Pg - PostgreSQL database driver for the DBI module


=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname");

  # See the DBI module documentation for full details


=head1 DESCRIPTION

DBD::Pg is a Perl module which works with the DBI module to provide
access to PostgreSQL databases.


=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions. 
It is not supposed to be used as the only reference for the user. In any 
case consult the DBI documentation first !


=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

To connect to a database with a minimum of parameters, use the 
following syntax: 

   $dbh = DBI->connect("dbi:Pg:dbname=$dbname");

This connects to the database $dbname at localhost without any user 
authentication. This is sufficient for the defaults of PostgreSQL. 

The following connect statement shows all possible parameters: 

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port;options=$options;tty=$tty", "$username", "$password");

If a parameter is undefined PostgreSQL first looks for specific environment 
variables and then it uses hard coded defaults: 

    parameter  environment variable  hard coded default
    --------------------------------------------------
    dbname     PGDATABASE            current userid
    host       PGHOST                localhost
    port       PGPORT                5432
    options    PGOPTIONS             ""
    tty        PGTTY                 ""
    username   PGUSER                current userid
    password   PGPASSWORD            ""

If a host is specified, the postmaster on this host needs to be 
started with the C<-i> option (TCP/IP sockets). 

The options parameter specifies runtime options for the Postgres 
backend. Common usage is to increase the number of buffers with 
the C<-B> option. For further details please refer to the L<postgres>. 

For authentication with username and password appropriate entries have 
to be made in pg_hba.conf. Please refer to the L<pg_hba.conf> and the 
L<pg_passwd> for the different types of authentication. Note that for 
these two parameters DBI distinguishes between empty and undefined. If 
these parameters are undefined DBI substitutes the values of the environment 
variables DBI_USER and DBI_PASS if present. 

=item B<available_drivers>

  @driver_names = DBI->available_drivers;

Implemented by DBI, no driver-specific impact.

=item B<data_sources>

  @data_sources = DBI->data_sources('Pg');

The driver supports this method. Note, that the necessary database 
connect to the database template1 will be done on the localhost 
without any user-authentication. Other preferences can only be set 
with the environment variables PGHOST, DBI_USER and DBI_PASS. 

=item B<trace>

  DBI->trace($trace_level, $trace_file)

Implemented by DBI, no driver-specific impact.

=back


=head2 DBI Dynamic Attributes

See Common Methods. 


=head1 METHODS COMMON TO ALL HANDLES

=over 4

=item B<err>

  $rv = $h->err;

Supported by the driver as proposed by DBI. For the connect 
method it returns PQstatus. In all other cases it returns 
PQresultStatus of the current handle. 

=item B<errstr>

  $str = $h->errstr;

Supported by the driver as proposed by DBI. It returns the 
PQerrorMessage related to the current handle. 

=item B<state>

  $str = $h->state;

This driver does not (yet) support the state method. 

=item B<trace>

  $h->trace($trace_level, $trace_filename);

Implemented by DBI, no driver-specific impact.

=item B<trace_msg>

  $h->trace_msg($message_text);

Implemented by DBI, no driver-specific impact.

=back


=head1 ATTRIBUTES COMMON TO ALL HANDLES

=over 4

=item B<Warn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Active> (boolean, read-only)

Supported by the driver as proposed by DBI. A database 
handle is active while it is connected and  statement 
handle is active until it is finished. 

=item B<Kids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<ActiveKids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<CachedKids> (hash ref)

Implemented by DBI, no driver-specific impact.

=item B<CompatMode> (boolean, inherited)

Not used by this driver. 

=item B<InactiveDestroy> (boolean)

Implemented by DBI, no driver-specific impact.

=item B<PrintError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<RaiseError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<ChopBlanks> (boolean, inherited)

Supported by the driver as proposed by DBI. This 
method is similar to the SQL-function RTRIM. 

=item B<LongReadLen> (integer, inherited)

Implemented by DBI, not used by the driver.

=item B<LongTruncOk> (boolean, inherited)

Implemented by DBI, not used by the driver.

=item B<private_*>

Implemented by DBI, no driver-specific impact.

=back


=head1 DBI DATABASE HANDLE OBJECTS

=head2 Database Handle Methods

=over 4

=item B<selectrow_array>

  @row_ary = $dbh->selectrow_array($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectall_arrayref>

  $ary_ref = $dbh->selectall_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<prepare>

  $sth = $dbh->prepare($statement, \%attr);

PostgreSQL does not have the concept of preparing 
a statement. Hence the prepare method just stores 
the statement after checking for place-holders. 
No information about the statement is available 
after preparing it. 

=item B<prepare_cached>

  $sth = $dbh->prepare_cached($statement, \%attr);

Implemented by DBI, no driver-specific impact. 
This method is not useful for this driver, because 
preparing a statement has no database interaction. 

=item B<do>

  $rv  = $dbh->do($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact. See the 
notes for the execute method elsewhere in this document. 

=item B<commit>

  $rc  = $dbh->commit;

Supported by the driver as proposed by DBI. See also the 
notes about B<Transactions> elsewhere in this document. 

=item B<rollback>

  $rc  = $dbh->rollback;

Supported by the driver as proposed by DBI. See also the 
notes about B<Transactions> elsewhere in this document. 

=item B<disconnect>

  $rc  = $dbh->disconnect;

Supported by the driver as proposed by DBI. 

=item B<ping>

  $rc = $dbh->ping;

This driver supports the ping-method, which can be used to check the 
validity of a database-handle. The ping method issues an empty query 
and checks the result status. 

=item B<table_info>

  $sth = $dbh->table_info;

Supported by the driver as proposed by DBI. This 
method returns all tables and views which are owned by the 
current user. It does not select any indices and sequences. 
Also System tables are not selected. As TABLE_QUALIFIER the 
reltype attribute is returned and the REMARKS are undefined. 

=item B<tables>

  @names = $dbh->tables;

Supported by the driver as proposed by DBI. This 
method returns all tables and views which are owned by the 
current user. It does not select any indices and sequences. 
Also system tables are not selected. 

=item B<attributes>

  $sth = $dbh->DBD::Pg::db::attributes($table);

This method is PostgreSQL specific and returns for the given 
table the following data: 

  unique number
  attribute name
  attribute type
  attribute length (-1 for variable length attributes)

Because this method is not supported by DBI you have to use the 
complete name including the package. 

=item B<type_info_all>

  $type_info_all = $dbh->type_info_all;

Supported by the driver as proposed by DBI. 
Only for SQL data-types and for frequently used data-types 
information is provided. The mapping between the PostgreSQL typename 
and the SQL92 data-type has been done according to the following 
table: 

	+---------------+------------------------------------+
	| typname       | SQL92                              |
	|---------------+------------------------------------|
	| bool          | BOOL                               |
	| bpchar        | CHAR(n)                            |
	| varchar       | VARCHAR(n)                         |
	| int2          | SMALLINT                           |
	| int4          | INT                                |
	| float4        | FLOAT(p)   p<7=float4, p<16=float8 |
	| float8        | REAL                               |
	| date          | /                                  |
	| time          | /                                  |
	| timespan      | TINTERVAL                          |
	| timestamp     | TIMESTAMP                          |
	+---------------+------------------------------------+

For further details concerning the PostgreSQL specific data-types 
please read the L<pgbuiltin>. 

=item B<type_info>

  @type_info = $dbh->type_info($data_type);

Implemented by DBI, no driver-specific impact. 

=item B<quote>

  $sql = $dbh->quote($value, $data_type);

This module implements it's own quote method. In addition to the 
DBI method it doubles also the backslash, because PostgreSQL treats 
a backslash as an escape character. The optional data_type parameter 
is not used. 

=back


=head2 Database Handle Attributes

=over 4

=item B<AutoCommit>  (boolean)

Supported by the driver as proposed by DBI. According to the 
classification of DBI, PostgreSQL is a database, in which a 
transaction must be explicitly started. Without starting a 
transaction, every change to the database becomes immediately 
permanent. The default of AutoCommit is on, which corresponds 
to the default behavior of PostgreSQL. When setting AutoCommit 
to off, a transaction will be started and every commit or rollback 
will automatically start a new transaction. For details see the 
notes about B<Transactions> elsewhere in this document. 

=item B<Name>  (string, read-only)

The default method of DBI is overridden by a driver specific 
method, which returns only the database name. Anything else 
from the connection string is stripped off. Note, that here 
the method is read-only in contrast to the DBI specs. 

=item B<pg_auto_escape> (boolean)

PostgreSQL specific attribute. If true, then quotes and backslashes in all 
parameters will be escaped in the following way: 

  escape quote with a quote (SQL)
  escape backslash with a backslash except for octal presentation

The default is on. Note, that PostgreSQL also accepts quotes, which 
are escaped by a backslash. Any other ASCII character can be used 
directly in a string constant. 

=back


=head1 DBI STATEMENT HANDLE OBJECTS

=head2 Statement Handle Methods

=over 4

=item B<bind_param>

  $rv = $sth->bind_param($param_num, $bind_value, \%attr);

Supported by the driver as proposed by DBI. 

=item B<bind_param_inout>

Not supported by this driver. 

=item B<execute>

  $rv = $sth->execute(@bind_values);

Supported by the driver as proposed by DBI. 
In addition to 'UPDATE', 'DELETE', 'INSERT' statements, for 
which it returns always the number of affected rows, the execute 
method can also be used for 'SELECT ... INTO table' statements. 

=item B<fetchrow_arrayref>

  $ary_ref = $sth->fetchrow_arrayref;

Supported by the driver as proposed by DBI. 

=item B<fetchrow_array>

  @ary = $sth->fetchrow_array;

Supported by the driver as proposed by DBI. 

=item B<fetchrow_hashref>

  $hash_ref = $sth->fetchrow_hashref;

Supported by the driver as proposed by DBI. 

=item B<fetchall_arrayref>

  $tbl_ary_ref = $sth->fetchall_arrayref;

Implemented by DBI, no driver-specific impact. 

=item B<finish>

  $rc = $sth->finish;

Supported by the driver as proposed by DBI. 

=item B<rows>

  $rv = $sth->rows;

Supported by the driver as proposed by DBI. 
In contrast to many other drivers the number of rows is 
available immediately after executing the statement. 

=item B<bind_col>

  $rc = $sth->bind_col($column_number, \$var_to_bind, \%attr);

Supported by the driver as proposed by DBI. 

=item B<bind_columns>

  $rc = $sth->bind_columns(\%attr, @list_of_refs_to_vars_to_bind);

Supported by the driver as proposed by DBI. 

=item B<dump_results>

  $rows = $sth->dump_results($maxlen, $lsep, $fsep, $fh);

Implemented by DBI, no driver-specific impact. 

=item B<blob_read>

  $blob = $sth->blob_read($id, $offset, $len);

Supported by this driver as proposed by DBI. Implemented by DBI 
but not documented, so this method might change. 

This method seems to be heavily influenced by the current implementation 
of blobs in Oracle. Nevertheless we try to be as compatible as possible. 
Whereas Oracle suffers from the limitation that blobs are related to tables 
and every table can have only one blob (data-type LONG), PostgreSQL handles 
its blobs independent of any table by using so called object identifiers. This 
explains why the blob_read method is blessed into the STATEMENT package and 
not part of the DATABASE package. Here the field parameter has been used to 
handle this object identifier. 

The offset and len parameter may be set to zero, in which case the driver 
fetches the whole blob at once. See also the B<blob_copy_to_file> method, 
which also supported by DBI, but not documented. 

For further information and examples about blobs, specifically about the 
two built-in functions lo_import() and lo_export(), please read the 
L<large_objects>. 

=back


=head2 Statement Handle Attributes

=over 4

=item B<NUM_OF_FIELDS>  (integer, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<NUM_OF_PARAMS>  (integer, read-only)

Implemented by DBI, no driver-specific impact. 

=item B<NAME>  (array-ref, read-only)

Supported by the driver as proposed by DBI. 

=item B<TYPE>  (array-ref, read-only)

Supported by the driver as proposed by DBI, with 
the restriction, that the types are PostgreSQL 
specific data-types which do not correspond to 
international standards. 

=item B<PRECISION>  (array-ref, read-only)

Not supported by the driver. 

=item B<SCALE>  (array-ref, read-only)

Not supported by the driver. 

=item B<NULLABLE>  (array-ref, read-only)

Not supported by the driver. 

=item B<CursorName>  (string, read-only)

Not supported by the driver. See the note about 
B<Cursors> elsewhere in this document. 

=item B<Statement>  (string, read-only)

Supported by the driver as proposed by DBI. 

=item B<pg_size>  (array-ref, read-only)

PostgreSQL specific attribute. It returns a reference to an 
array of integer values for each column. The integer shows 
the size of the column in bytes. Variable length columns 
are indicated by -1. 

=item B<pg_type>  (hash-ref, read-only)

PostgreSQL specific attribute. It returns a reference to an 
array of strings for each column. The string shows the name 
of the data_type. 

=item B<pg_oid_status> (integer, read-only)

PostgreSQL specific attribute. It returns the OID of the last 
INSERT command. 

=item B<pg_cmd_status> (integer, read-only)

PostgreSQL specific attribute. It returns the type of the last 
command. Possible types are: INSERT, DELETE, UPDATE, SELECT. 

=back


=head1 FURTHER INFORMATION

=head2 Transactions

The transaction behavior is now controlled with the attribute AutoCommit. 
For a complete definition of AutoCommit please refer to the DBI documentation. 

According to the DBI specification the default for AutoCommit is TRUE. 
In this mode, any change to the database becomes valid immediately. Any 
'begin', 'commit' or 'rollback' statement will be rejected. 

If AutoCommit is switched-off, immediately a transaction will be started by 
issuing a 'begin' statement. Any 'commit' or 'rollback' will start a new 
transaction. A disconnect will issue a 'rollback' statement. 


=head2 Cursors

Although PostgreSQL has a cursor concept, it has not 
been used in the current implementation. Cursors in 
PostgreSQL can only be used inside a transaction block. 
Because only one transaction block at a time is allowed, 
this would have implied the restriction, not to use 
any nested SELECT statements. Hence the execute method 
fetches all data at once into data structures located 
in the frontend application. This has to be considered 
when selecting large amounts of data ! 


=head2 Data-Type bool

The current implementation of PostgreSQL returns 't' for true and 'f' for 
false. From the perl point of view a rather unfortunate choice. The DBD-Pg 
module translates the result for the data-type bool in a perl-ish like manner: 
'f' -> '0' and 't' -> '1'. This way the application does not have to check 
the database-specific returned values for the data-type bool, because perl 
treats '0' as false and '1' as true. 

PostgreSQL Version 6.2 considers the input 't' as true 
and anything else as false. 
PostgreSQL Version 6.3 considers the input 't', '1' and 1 as true 
and anything else as false. 
PostgreSQL Version 6.4 considers the input 't', '1' and 'y' as true 
and any other character as false. 


=head1 SEE ALSO

L<DBI>


=head1 AUTHORS

=item *
DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

=item *
DBD-Pg by Edmund Mergl (E.Mergl@bawue.de)

 Major parts of this package have been copied from DBI and DBD-Oracle.


=head1 COPYRIGHT

The DBD::Pg module is free software. 
You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file,
with the exception that it cannot be placed on a CD-ROM or similar media
for commercial distribution without the prior approval of the author.


=head1 ACKNOWLEDGMENTS

See also B<DBI/ACKNOWLEDGMENTS>.

=cut
