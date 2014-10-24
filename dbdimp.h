/*
   $Id: dbdimp.h,v 1.7 2003/03/25 22:17:00 bmomjian Exp $

   Copyright (c) 1997,1998,1999,2000 Edmund Mergl
   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.
*/

#ifdef WIN32
#define snprintf _snprintf
#endif

/* Define drh implementor data structure */
struct imp_drh_st {
    dbih_drc_t com;		/* MUST be first element in structure	*/
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
    dbih_dbc_t com;		/* MUST be first element in structure	*/

    PGconn    * conn;		/* connection structure */
    int         init_commit;	/* initialize AutoCommit */
    int         pg_auto_escape;	/* initialize AutoEscape */
    int         pg_bool_tf;     /* do bools return 't'/'f' */
#ifdef SvUTF8_off
    int         pg_enable_utf8;	/* should we attempt to make utf8 strings? */
#endif
};

/* Define sth implementor data structure */
struct imp_sth_st {
    dbih_stc_t com;		/* MUST be first element in structure	*/

    PGresult* result;		/* result structure */
    int cur_tuple;		/* current tuple */
    int rows;			/* number of affected rows */

    /* Input Details	*/
    char      *statement;	/* sql (see sth_scan)		*/
    HV        *all_params_hv;	/* all params, keyed by name	*/
    AV        *out_params_av;	/* quick access to inout params	*/
    int        pg_pad_empty;	/* convert ""->" " when binding	*/
    int        all_params_len;  /* length-sum of all params     */

    /* (In/)Out Parameter Details */
    bool  has_inout_params;
};


#define sword  signed int
#define sb2    signed short
#define ub2    unsigned short

typedef struct phs_st phs_t;    /* scalar placeholder   */

struct phs_st {  	/* scalar placeholder EXPERIMENTAL	*/
    sword ftype;        /* external OCI field type		*/

    SV	*sv;		/* the scalar holding the value		*/
    int sv_type;	/* original sv type at time of bind	*/
    bool is_inout;

    IV  maxlen;		/* max possible len (=allocated buffer)	*/

    /* these will become an array */
    sb2 indp;		/* null indicator			*/
    char *progv;
    ub2 arcode;
    IV alen;		/* effective length ( <= maxlen )	*/

    int alen_incnull;	/* 0 or 1 if alen should include null	*/
    char name[1];	/* struct is malloc'd bigger as needed	*/
};


SV * dbd_db_pg_notifies (SV *dbh, imp_dbh_t *imp_dbh);

/* end of dbdimp.h */
