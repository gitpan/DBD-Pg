/*
	$Id: dbdimp.h,v 1.37 2005/05/14 03:02:38 turnstep Exp $
	
	Copyright (c) 2000-2005 PostgreSQL Global Development Group
	Portions Copyright (c) 1997-2000 Edmund Mergl
	Portions Copyright (c) 1994-1997 Tim Bunce
	
	You may distribute under the terms of either the GNU General Public
	License or the Artistic License, as specified in the Perl README file.
*/

#include "types.h"

#ifdef WIN32
#define snprintf _snprintf
#endif

#define sword  signed int
#define sb2    signed short
#define ub2    unsigned short

/* Define drh implementor data structure */
struct imp_drh_st {
	dbih_drc_t com; /* MUST be first element in structure */
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
	dbih_dbc_t com;            /* MUST be first element in structure */

	bool    pg_bool_tf;        /* do bools return 't'/'f'? Set by user, default is 0 */
	bool    pg_enable_utf8;    /* should we attempt to make utf8 strings? Set by user, default is 0 */
	bool    prepare_now;       /* force immediate prepares, even with placeholders. Set by user, default is 0 */
	bool    done_begin;        /* have we done a begin? (e.g. are we in a transaction?) */

	int     pg_protocol;       /* value of PQprotocolVersion, usually 0, 2, or 3 */
	int     pg_server_version; /* Server version e.g. 80100 */
	int     prepare_number;    /* internal prepared statement name modifier */
	int     copystate;         /* 0=none PGRES_COPY_IN PGRES_COPY_OUT */
	char    pg_errorlevel;     /* PQsetErrorVerbosity. Set by user, defaults to 1 */
	char    server_prepare;    /* do we want to use PQexecPrepared? 0=no 1=yes 2=smart. Can be changed by user */

	AV      *savepoints;       /* list of savepoints */
	PGconn  *conn;             /* connection structure */
	char    *sqlstate;         /* from the last result */
};


/* Each statement is broken up into segments */
struct seg_st {
	char *segment;          /* non-placeholder string segment */
	int placeholder;        /* which placeholder this points to, 0=none */
	struct ph_st *ph;       /* points to the relevant ph structure */
	struct seg_st *nextseg; /* linked lists are fun */
};
typedef struct seg_st seg_t;

/* The placeholders are also a linked list */
struct ph_st {
	char  *fooname;             /* Name if using :foo style */
	char  *value;               /* the literal passed-in value, may be binary */
	STRLEN valuelen;            /* length of the value */
	char  *quoted;              /* quoted version of the value, for PQexec only */
	STRLEN quotedlen;           /* length of the quoted value */
	bool   referenced;          /* used for PREPARE AS construction */
	bool   defaultval;          /* is it using a generic 'default' value? */
	sql_type_info_t* bind_type; /* type information for this placeholder */
	struct ph_st *nextph;       /* more linked list goodness */
};
typedef struct ph_st ph_t;

/* Define sth implementor data structure */
struct imp_sth_st {
	dbih_stc_t com;         /* MUST be first element in structure */

	bool   prepare_now;      /* prepare this statement right away, even if it has placeholders */
	bool   prepared_by_us;   /* false if {prepare_name} set directly */
	bool   direct;           /* allow bypassing of the statement parsing */
	bool   is_dml;           /* is this SELECT/INSERT/UPDATE/DELETE? */
	bool   has_binary;       /* does it have one or more binary placeholders? */

	char   server_prepare;   /* inherited from dbh. 3 states: 0=no 1=yes 2=smart */
	char   placeholder_type; /* which style is being used 1=? 2=$1 3=:foo */

	STRLEN totalsize;        /* total string length of the statement (with no placeholders)*/

	int    numsegs;          /* how many segments this statement has */
	int    numphs;           /* how many placeholders this statement has */
	int    numbound;         /* how many placeholders were explicitly bound by the client, not us */
	int    cur_tuple;        /* current tuple being fetched */
	int    rows;             /* number of affected rows */

  char   *statement;       /* the rewritten statement, for passing to PQexecP.. */
	char   *prepare_name;    /* name of the prepared query; NULL if not prepared */
	char   *firstword;       /* first word of the statement */

	PGresult  *result;       /* result structure from the executed query */
	sql_type_info_t **type_info; /* type of each column in result */

	seg_t  *seg;             /* linked list of segments */
	ph_t   *ph;              /* linked list of placeholders */
};

/* Other functions we have added to dbdimp.c */

SV * dbd_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);
int dbd_db_ping ();
int dbd_db_getfd ();
int pg_db_putline ();
int pg_db_getline ();
int pg_db_endcopy ();
int pg_db_savepoint ();
int pg_db_rollback_to ();
int pg_db_release ();
void pg_db_pg_server_trace ();
void pg_db_pg_server_untrace ();
void pg_db_server_trace ();
void pg_db_no_server_trace ();

int pg_db_lo_open ();
int pg_db_lo_close ();
int pg_db_lo_read ();
int pg_db_lo_write ();
int pg_db_lo_lseek ();
unsigned int pg_db_lo_creat ();
int pg_db_lo_tell ();
int pg_db_lo_unlink ();
unsigned int pg_db_lo_import ();
int pg_db_lo_export ();

/* end of dbdimp.h */
