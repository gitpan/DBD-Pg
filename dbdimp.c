/*-------------------------------------------------------
 *
 * $Id: dbdimp.c,v 1.5 1997/04/24 20:26:54 mergl Exp $
 *
 *  Portions Copyright (c) 1994,1995,1996  Tim Bunce
 *  Portions Copyright (c) 1997            Edmund Mergl
 *
 *-------------------------------------------------------
 */


#include "Pg.h"


DBISTATE_DECLARE;


void
dbd_init(dbistate)
    dbistate_t *dbistate;
{
    DBIS = dbistate;
}


void
dbd_error(h, error_num, error_msg)
    SV * h;
    int error_num;
    char *error_msg;
{
    D_imp_xxh(h);

    sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);
    sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)error_msg);
}


/* ================================================================== */

int
dbd_db_login(dbh, dbname, uid, pwd)
    SV *dbh;
    char *dbname;
    char *uid;
    char *pwd;
{
    D_imp_dbh(dbh);

    /* make a connection to the database */
    imp_dbh->conn = PQsetdb(NULL, NULL, NULL, NULL, dbname);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(imp_dbh->conn) != CONNECTION_OK) {
	dbd_error(dbh, PQstatus(imp_dbh->conn), "login failed");
	return 0;
    }

    DBIc_IMPSET_on(imp_dbh);	/* imp_dbh set up now			*/
    DBIc_ACTIVE_on(imp_dbh);	/* call disconnect before freeing	*/
    return 1;
}


int
dbd_db_do(dbh, statement, attribs, params)
    SV * dbh;
    char * statement;
    char * attribs;
    SV *params;
{
    D_imp_dbh(dbh);
    PGresult* result = 0;
    ExecStatusType status;

    /* execute command */
    result = PQexec(imp_dbh->conn, statement);
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);

    if (status != PGRES_COMMAND_OK) {
        dbd_error(dbh, status, PQerrorMessage(imp_dbh->conn));
        return 0;
    }

    return 1;
}


int
dbd_db_commit(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    PGresult* result = 0;
    ExecStatusType status;

    /* execute commit */
    result = PQexec(imp_dbh->conn, "commit");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);

    if (status != PGRES_COMMAND_OK) {
        dbd_error(dbh, status, "commit failed");
        return 0;
    }

    return 1;
}

int
dbd_db_rollback(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    PGresult* result = 0;
    ExecStatusType status;

    /* execute rollback */
    result = PQexec(imp_dbh->conn, "rollback");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);

    if (status != PGRES_COMMAND_OK) {
        dbd_error(dbh, status, "rollback failed");
        return 0;
    }

    return 1;
}


int
dbd_db_disconnect(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);

    /* We assume that disconnect will always work	*/
    /* since most errors imply already disconnected.	*/
    DBIc_ACTIVE_off(imp_dbh);

    /* We don't free imp_dbh since a reference still exists	*/
    /* The DESTROY method is the only one to 'free' memory.	*/
    /* Note that statement objects may still exists for this dbh!	*/
    return 1;
}


void
dbd_db_destroy(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);

    if (DBIc_ACTIVE(imp_dbh)) {
	dbd_db_disconnect(dbh);
    }

#ifdef PGDEBUG
    fprintf(stderr, "destroy database handle\n");
#endif

    PQfinish(imp_dbh->conn);

    /* Nothing in imp_dbh to be freed	*/
    DBIc_IMPSET_off(imp_dbh);
}


int
dbd_db_STORE(dbh, keysv, valuesv)
    SV *dbh;
    SV *keysv;
    SV *valuesv;
{
    D_imp_dbh(dbh);

    return FALSE;
}


SV *
dbd_db_FETCH(dbh, keysv)
    SV *dbh;
    SV *keysv;
{
    D_imp_dbh(dbh);

    return Nullsv;
}



/* ================================================================== */


int
dbd_st_prepare(sth, statement, attribs)
    SV *sth;
    char *statement;
    SV *attribs;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_prepare\n");
#endif

    /* initialize new statement handle */

    imp_sth->result    = 0;
    imp_sth->cur_tuple = 0;

    DBIc_IMPSET_on(imp_sth);

    return 1;
}


int
dbd_st_execute(sth, statement)
    SV *sth;
    char *statement;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    ExecStatusType status = -1;
    int ret = -1;

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_execute\n");
#endif

    if (! statement) {
        /* are we prepared ? */
        dbd_error(sth, -1, "statement not prepared");
    } else {
        /* execute statement if not already done */
        if (! imp_sth->result) {
            imp_sth->result = PQexec(imp_dbh->conn, statement);
        }
        /* check status */
        status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;
    }

    if (PGRES_TUPLES_OK == status) {
        /* select statement */
        imp_sth->cur_tuple = 0;
        DBIc_NUM_FIELDS(imp_sth) = PQnfields(imp_sth->result);
        DBIc_ACTIVE_on(imp_sth);
        ret = PQntuples(imp_sth->result);
    } else if (PGRES_COMMAND_OK == status) {
        /* non-select statement */
	PQclear(imp_sth->result);
        imp_sth->result = 0;
        ret = 0;
    } else {
        dbd_error(sth, status, PQerrorMessage(imp_dbh->conn));
    }

    return ret;
}



AV *
dbd_st_fetch(sth)
    SV *	sth;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    int num_fields;
    int i;
    AV *av;

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_fetch\n");
#endif

    /* Check that execute() was executed sucessfully */
    if ( !DBIc_ACTIVE(imp_sth) ) {
	dbd_error(sth, 1, "no statement executing");
	return Nullav;
    }

    if ( imp_sth->cur_tuple == PQntuples(imp_sth->result) ) {
        imp_sth->cur_tuple = 0;
        return Nullav; /* we reached the last tuple */
    }

    av = DBIS->get_fbav(imp_sth);
    num_fields = AvFILL(av)+1;

    for(i = 0; i < num_fields; ++i) {

	SV *sv = AvARRAY(av)[i];
	sv_setpv(sv, (char*)PQgetvalue(imp_sth->result, imp_sth->cur_tuple, i));
    }

    imp_sth->cur_tuple += 1;

    return av;
}


int
dbd_st_rows(sth)
    SV *sth;
{
    D_imp_sth(sth);

    return PQntuples(imp_sth->result);
}


int
dbd_st_finish(sth)
    SV *sth;
{
    D_imp_sth(sth);

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_finish\n");
#endif

    if (imp_sth->result) {
	PQclear(imp_sth->result);
        imp_sth->result = 0;
    }

    DBIc_ACTIVE_off(imp_sth);

    return 1;
}


void
dbd_st_destroy(sth)
    SV *sth;
{
    D_imp_sth(sth);

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_destroy\n");
#endif

    DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */
}


int
dbd_st_STORE(sth, keysv, valuesv)
    SV *sth;
    SV *keysv;
    SV *valuesv;
{
    return FALSE;
}


SV *
dbd_st_FETCH(sth, keysv)
    SV *sth;
    SV *keysv;
{
    D_imp_sth(sth);
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    int i;
    SV *retsv = NULL;

    if (kl==13 && strEQ(key, "NUM_OF_PARAMS")) { /* handled by DBI */
	return Nullsv;	
    }

    if (kl == 4 && strEQ(key, "NAME")) {
	AV *av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	for (i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++) {
	    av_store(av, i, newSVpv(PQfname(imp_sth->result, i),0));
        }
    } else if ( kl== 4 && strEQ(key, "TYPE")) {
	AV *av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	for (i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++) {
	    av_store(av, i, newSViv(PQftype(imp_sth->result, i)));
        }
    } else if (kl==4 && strEQ(key, "SIZE")) {
	AV *av = newAV();
	retsv = newRV(sv_2mortal((SV*)av));
	for (i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++) {
	    av_store(av, i, newSViv(PQfsize(imp_sth->result, i)));
        }
    } else {
	return Nullsv;
    }

    return sv_2mortal(retsv);
}



/* EOF */
