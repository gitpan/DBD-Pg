/*---------------------------------------------------------
 *
 * $Id: dbdimp.c,v 1.3 1997/08/12 02:43:39 mergl Exp $
 *
 * Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
 * Portions Copyright (c) 1997                Edmund Mergl
 *
 *---------------------------------------------------------
 */


#include "Pg.h"


DBISTATE_DECLARE;

static SV *dbd_pad_empty;


void
dbd_init(dbistate)
    dbistate_t *dbistate;
{
    DBIS = dbistate;

    if (getenv("DBD_PAD_EMPTY"))
	sv_setiv(dbd_pad_empty, atoi(getenv("DBD_PAD_EMPTY")));
}


void
dbd_error(h, error_num, error_msg)
    SV * h;
    int error_num;
    char *error_msg;
{
    D_imp_xxh(h);

    sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);		/* set err early */
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
    char *conn_str;
    D_imp_dbh(dbh);

#ifdef PGDEBUG
    fprintf(stderr, "dbd_db_login\n");
#endif

    /* make a connection to the database */
    conn_str = (char *)malloc(strlen(dbname) + strlen(uid) + strlen(pwd) + 48);
    if (! conn_str) {
        return 0;
    }
    strcpy(conn_str, "dbname=");
    strcat(conn_str, dbname);
    if (strlen(uid) && strlen(pwd)) {
        strcat(conn_str, " authtype=password user=");
        strcat(conn_str, uid);
        strcat(conn_str, " password=");
        strcat(conn_str, pwd);
    }
    imp_dbh->conn = PQconnectdb(conn_str);
    free(conn_str);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(imp_dbh->conn) != CONNECTION_OK) {
	dbd_error(dbh, PQstatus(imp_dbh->conn), "login failed\n");
	return 0;
    }

    DBIc_IMPSET_on(imp_dbh);	/* imp_dbh set up now			*/
    DBIc_ACTIVE_on(imp_dbh);	/* call disconnect before freeing	*/
    return 1;
}


int
dbd_db_do(dbh, statement, attribs)
    SV * dbh;
    char * statement;
    SV * attribs;
{
    D_imp_dbh(dbh);
    PGresult* result = 0;
    ExecStatusType status;

#ifdef PGDEBUG
    fprintf(stderr, "dbd_db_do\n");
#endif

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
        dbd_error(dbh, status, "commit failed\n");
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
        dbd_error(dbh, status, "rollback failed\n");
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

    PQfinish(imp_dbh->conn);

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

    /* Nothing in imp_dbh to be freed	*/
    DBIc_IMPSET_off(imp_dbh);
}


int
dbd_db_STORE(dbh, keysv, valuesv)
    SV *dbh;
    SV *keysv;
    SV *valuesv;
{
    return FALSE;
}


SV *
dbd_db_FETCH(dbh, keysv)
    SV *dbh;
    SV *keysv;
{
    D_imp_dbh(dbh);
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    SV *retsv = NULL;

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

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_prepare\n");
#endif

    /* initialize new statement handle */

    imp_sth->result    = 0;
    imp_sth->cur_tuple = 0;

    DBIc_IMPSET_on(imp_sth);
    return 1;
}


static int 
_dbd_rebind_ph(sth, imp_sth, phs) 
    SV *sth;
    imp_sth_t *imp_sth;
    phs_t *phs;
{
    STRLEN value_len;

/*	for strings, must be a PV first for ptr to be valid? */
/*    sv_insert +4	*/
/*    sv_chop(phs->sv, SvPV(phs->sv,na)+4);	XXX */

    if (dbis->debug >= 2) {
	char *text = neatsvpv(phs->sv,0);
	fprintf(DBILOGFP, "bind %s <== %s (size %d/%d/%ld, ptype %ld, otype %d)\n",
	    phs->name, text, SvCUR(phs->sv),SvLEN(phs->sv),phs->maxlen,
	    SvTYPE(phs->sv), phs->ftype);
    }

    /* At the moment we always do sv_setsv() and rebind.	*/
    /* Later we may optimise this so that more often we can	*/
    /* just copy the value & length over and not rebind.	*/

    if (phs->is_inout) {	/* XXX */
	if (SvREADONLY(phs->sv))
	    croak(no_modify);
	/* phs->sv _is_ the real live variable, it may 'mutate' later	*/
	/* pre-upgrade high to reduce risk of SvPVX realloc/move	*/
	(void)SvUPGRADE(phs->sv, SVt_PVNV);
	/* ensure room for result, 28 is magic number (see sv_2pv)	*/
	SvGROW(phs->sv, (phs->maxlen < 28) ? 28 : phs->maxlen+1);
	if (imp_sth->dbd_pad_empty)
	    croak("Can't use dbd_pad_empty with bind_param_inout");
    }
    else {
	/* phs->sv is copy of real variable, upgrade to at least string	*/
	(void)SvUPGRADE(phs->sv, SVt_PV);
    }

    /* At this point phs->sv must be at least a PV with a valid buffer,	*/
    /* even if it's undef (null)					*/
    /* Here we set phs->progv, phs->indp, and value_len.		*/
    if (SvOK(phs->sv)) {
	phs->progv = SvPV(phs->sv, value_len);
	phs->indp  = 0;
    }
    else {	/* it's null but point to buffer incase it's an out var	*/
	phs->progv = SvPVX(phs->sv);
	phs->indp  = -1;
	value_len  = 0;
    }
    if (imp_sth->dbd_pad_empty && value_len==0) {
	sv_setpv(phs->sv, " ");
	phs->progv = SvPV(phs->sv, value_len);
    }
    phs->sv_type = SvTYPE(phs->sv);	/* part of mutation check	*/
    phs->alen    = value_len + phs->alen_incnull;
    phs->maxlen  = SvLEN(phs->sv)-1;	/* avail buffer space	*/

    return 1;
}


int
dbd_bind_ph(sth, ph_namesv, newvalue, attribs, is_inout, maxlen)
    SV *sth;
    SV *ph_namesv;
    SV *newvalue;
    SV *attribs;
    int is_inout;
    IV maxlen;
{
    D_imp_sth(sth);
    SV **phs_svp;
    STRLEN name_len;
    char *name;
    char namebuf[30];
    phs_t *phs;

    /* check if placeholder was passed as a number	*/
    if (SvNIOK(ph_namesv) || (SvPOK(ph_namesv) && isDIGIT(*SvPVX(ph_namesv)))) {
	name = namebuf;
	sprintf(name, ":p%d", (int)SvIV(ph_namesv));
	name_len = strlen(name);
    }
    else {		/* use the supplied placeholder name directly */
	name = SvPV(ph_namesv, name_len);
    }

    if (SvTYPE(newvalue) > SVt_PVMG)	/* hook for later array logic	*/
	croak("Can't bind non-scalar value (currently)");

    if (dbis->debug >= 2)
	fprintf(DBILOGFP, "bind %s <== %s (attribs: %s)\n",
		name, neatsvpv(newvalue,0), attribs ? SvPV(attribs,na) : "" );

    phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
    if (phs_svp == NULL)
	croak("Can't bind unknown placeholder '%s'", name);
    phs = (phs_t*)(void*)SvPVX(*phs_svp);	/* placeholder struct	*/

    if (phs->sv == &sv_undef) {	/* first bind for this placeholder	*/
	phs->ftype    = 1;		/* our default type VARCHAR2	*/
	phs->maxlen   = maxlen;		/* 0 if not inout		*/
	phs->is_inout = is_inout;
	if (is_inout) {
	    phs->sv = SvREFCNT_inc(newvalue);	/* point to live var	*/
	    ++imp_sth->has_inout_params;
	    /* build array of phs's so we can deal with out vars fast	*/
	    if (!imp_sth->out_params_av)
		imp_sth->out_params_av = newAV();
	    av_push(imp_sth->out_params_av, SvREFCNT_inc(*phs_svp));
	}
    }
	/* check later rebinds for any changes */
    else if (is_inout || phs->is_inout) {
	croak("Can't rebind or change param %s in/out mode after first bind", phs->name);
    }
    else if (maxlen && maxlen != phs->maxlen) {
	croak("Can't change param %s maxlen (%ld->%ld) after first bind",
			phs->name, phs->maxlen, maxlen);
    }

    if (!is_inout) {	/* normal bind to take a (new) copy of current value	*/
	if (phs->sv == &sv_undef)	/* (first time bind) */
	    phs->sv = newSV(0);
	sv_setsv(phs->sv, newvalue);
    }

    return _dbd_rebind_ph(sth, imp_sth, phs);
}


int
dbd_st_execute(sth)	/* <= -2:error, >=0:ok row count, (-1=unknown count) */
    SV *sth;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    ExecStatusType status = -1;
    int ret = -2;

    SV** svp = hv_fetch((HV *)SvRV(sth), "Statement", 9, FALSE);
    char *statement = SvPV(*svp, na);

#ifdef PGDEBUG
    fprintf(stderr, "dbd_st_execute\n");
#endif

    if (! statement) {
        /* are we prepared ? */
        dbd_error(sth, -1, "statement not prepared\n");
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
        ret = -1;
    } else {
        dbd_error(sth, status, PQerrorMessage(imp_dbh->conn));
        ret = -2;
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
	dbd_error(sth, 1, "no statement executing\n");
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

    /* Free off contents of imp_sth	*/

    if (imp_sth->out_params_av)
	sv_free((SV*)imp_sth->out_params_av);

    if (imp_sth->all_params_hv) {
	HV *hv = imp_sth->all_params_hv;
	SV *sv;
	char *key;
	I32 retlen;
	hv_iterinit(hv);
	while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
	    if (sv != &sv_undef) {
		phs_t *phs_tpl = (phs_t*)(void*)SvPVX(sv);
		sv_free(phs_tpl->sv);
	    }
	}
	sv_free((SV*)imp_sth->all_params_hv);
    }

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
    SV *retsv = NULL;
    int i;

    if (kl==13 && strEQ(key, "NUM_OF_PARAMS")) { /* handled by DBI */
	return Nullsv;	
    }

    if (! imp_sth->result) {
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
    } else if (kl==13 && strEQ(key, "pg_oid_status")) {
        retsv = newSVpv((char *)PQoidStatus(imp_sth->result), 0);
    } else if (kl==13 && strEQ(key, "pg_cmd_status")) {
        retsv = newSVpv((char *)PQcmdStatus(imp_sth->result), 0);
    } else {
	return Nullsv;
    }

    return sv_2mortal(retsv);
}



/* EOF */
