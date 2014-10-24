use strict;
use DBI;
use Test::More;
use Data::Dumper;

if (defined $ENV{DBI_DSN}) {
    plan tests => 1; ## 16 when done
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 1}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

## Until all the array stuff is working, skip all tests
my $skip_em_all = q{


# Insert into array
my $values = [["a,b", 'c","d', "e'", '\\"'], ['f', 'g', 'h']];
print Data::Dumper::Dumper($values);

ok($dbh->do(q{INSERT INTO dbd_pg_test (id, name, testarray) VALUES (?, ?, ?)}, {}, 1, 'array1', $values),
	'insert statement with references'
  );


print Data::Dumper::Dumper($values);

my $sql = <<SQL;
   SELECT testarray[1][1],testarray[1][2],testarray[1][3],
   testarray[2][1],testarray[2][2],testarray[2][3]
   FROM dbd_pg_test
   WHERE id = ?
   AND name = ?
SQL
my $sth = $dbh->prepare($sql);
ok(defined $sth,
   "prepare: $sql"
  );

ok($sth->bind_param(1, '1'),
   'bind parameter 1',
  );
ok($sth->bind_param(2, 'array1'),
   'bind parameter 2'
   );

ok($sth->execute,
	'execute statement with references'
   );

my @result = $sth->fetchrow_array;

ok(scalar(@result) == 6,
	'fetch 6 columns'
  );

ok($result[0] eq 'a,b',
	'values are equal'
  );

ok($result[1] eq 'c","d',
	'values are equal'
  );

ok($result[2] eq "e'",
    'values are equal'
  );
ok($result[2] eq q{\\\\\"}, 
    'values are equal'
  );

ok($result[3] eq 'f',
	'values are equal'
  );

ok($result[4] eq 'g',
	'values are equal'
  );

ok($result[5] eq 'h',
	'values are equal'
  );

ok($sth->finish(),
   'finish'
   );

ok($dbh->disconnect(),
   'disconnect'
  );
}; ## end skip_em_all
