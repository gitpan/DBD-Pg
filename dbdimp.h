/*-------------------------------------------------------
 *
 * $Id: dbdimp.h,v 1.3 1997/04/24 20:26:54 mergl Exp $
 *
 *  Portions Copyright (c) 1994,1995,1996  Tim Bunce
 *  Portions Copyright (c) 1997            Edmund Mergl
 *
 *-------------------------------------------------------
 */

/* Define drh implementor data structure */
struct imp_drh_st {
    dbih_drc_t com;		/* MUST be first element in structure	*/
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
    dbih_dbc_t com;		/* MUST be first element in structure	*/
    PGconn* conn;
};

/* Define sth implementor data structure */
struct imp_sth_st {
    dbih_stc_t com;		/* MUST be first element in structure	*/
    PGresult* result;
    int cur_tuple;
};

/* EOF */

