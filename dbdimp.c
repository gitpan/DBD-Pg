/*---------------------------------------------------------
 *
 * $Id: dbdimp.c,v 1.17 1998/02/19 20:28:54 mergl Exp $
 *
 * Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
 * Portions Copyright (c) 1997,1998           Edmund Mergl
 *
 *---------------------------------------------------------
 */


#include "Pg.h"

#define BUFSIZE 1024

DBISTATE_DECLARE;

static void dbd_preparse  (imp_sth_t *imp_sth, char *statement);
static int _dbd_rebind_ph (SV *sth, imp_sth_t *imp_sth, phs_t *phs);

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
dbd_discon_all(drh, imp_drh)
    SV *drh;
    imp_drh_t *imp_drh;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_discon_all\n"); }

    return FALSE;
}


int
dbd_db_login(dbh, imp_dbh, dbname, uid, pwd)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    char *dbname;
    char *uid;
    char *pwd;
{
    char *conn_str;
    char *src;
    char *dest;
    int len;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_login\n"); }

    /* build connect string */
    /* DBD-Pg syntax: 'dbname=dbname;host=host;port=port' */
    /* pgsql  syntax: 'dbname=dbname host=host port=port user=uid authtype=password password=pwd' */

    conn_str = (char *)malloc(strlen(dbname) + strlen(uid) + strlen(pwd) + 34 + 1);
    if (! conn_str) {
        return 0;
    }

    src  = dbname;
    dest = conn_str;
    while (*src) {
	if (*src != ';') {
	    *dest++ = *src++;
	    continue;
	}
        *dest++ = ' ';
        src++;
    }
    *dest = '\0';

    if (strlen(uid)) {
        strcat(conn_str, " user=");
        strcat(conn_str, uid);
    }
    if (strlen(uid) && strlen(pwd)) {
        strcat(conn_str, " authtype=password password=");
        strcat(conn_str, pwd);
    }

    if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_db_login: conn_str = >%s<\n", conn_str); }

    /* make a connection to the database */
    imp_dbh->conn = PQconnectdb(conn_str);
    free(conn_str);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(imp_dbh->conn) != CONNECTION_OK) {
        dbd_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
	return 0;
    }

    imp_dbh->init_auto = 1;			/* initialize AutoCommit */

    DBIc_IMPSET_on(imp_dbh);			/* imp_dbh set up now */
    DBIc_ACTIVE_on(imp_dbh);			/* call disconnect before freeing */
    return 1;
}


int
dbd_db_ping(dbh)
    SV *dbh;
{
    int id;
    D_imp_dbh(dbh);

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_ping\n"); }

    if (imp_dbh->conn->status != CONNECTION_OK) {
        dbd_error(dbh, imp_dbh->conn->status, "dbd_db_ping: no connection to the backend\n");
        return 0;
    }

    if (1 == pqPuts("Q ", imp_dbh->conn->Pfout, imp_dbh->conn->Pfdebug)) {
        dbd_error(dbh, imp_dbh->conn->status, "dbd_db_ping: sending query to the backend failed\n");
        imp_dbh->conn->status = CONNECTION_BAD;
        return 0;
    }

    if ('I' != pqGetc(imp_dbh->conn->Pfin, imp_dbh->conn->Pfdebug) || '\0' != pqGetc(imp_dbh->conn->Pfin, imp_dbh->conn->Pfdebug)) {
        dbd_error(dbh, imp_dbh->conn->status, "dbd_db_ping: backend closed the channel before responding\n");
        imp_dbh->conn->status = CONNECTION_BAD;
        return 0;
    }

    return 1;
}


int
dbd_db_do(dbh, statement)
    SV * dbh;
    char *statement;
{
    D_imp_dbh(dbh);
    PGresult* result = 0;
    ExecStatusType status;
    char *cmdStatus;
    char *cmdTuples;
    int ret = -2;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_do: statement = >%s<\n", statement); }

    /* execute command */
    result = PQexec(imp_dbh->conn, statement);
    status    = result ? PQresultStatus(result)      : -1;
    cmdStatus = result ? (char *)PQcmdStatus(result) : NULL;
    cmdTuples = result ? (char *)PQcmdTuples(result) : NULL;
    PQclear(result);

    /* check also for PGRES_TUPLES_OK in case of 'SELECT INTO TABLE' */
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        dbd_error(dbh, status, PQerrorMessage(imp_dbh->conn));
        return -2;
    }

    if (! strncmp(cmdStatus, "DELETE", 6) || ! strncmp(cmdStatus, "INSERT", 6) || ! strncmp(cmdStatus, "UPDATE", 6)) {
        ret = atoi(cmdTuples);
    } else {
        ret = -1;
    }

    return ret;
}


int
dbd_db_commit(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    PGresult* result = 0;
    ExecStatusType status;
    int retval = 1;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_commit\n"); }

    /* no commit if AutoCommit = on */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) != FALSE) {
        return 0;
    }

    /* execute commit */
    result = PQexec(imp_dbh->conn, "commit");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);

    if (status != PGRES_COMMAND_OK) {
        dbd_error(dbh, status, "commit failed\n");
        return 0;
    }

    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) == FALSE) {
        result = PQexec(imp_dbh->conn, "begin");
        status = result ? PQresultStatus(result) : -1;
        PQclear(result);
        if (status != PGRES_COMMAND_OK) {
            dbd_error(dbh, status, "begin failed\n");
            return 0;
        }
    }

    return retval;
}


int
dbd_db_rollback(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    PGresult* result = 0;
    ExecStatusType status;
    int retval = 1;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_rollback\n"); }

    /* no rollback if AutoCommit = on */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) != FALSE) {
        return 0;
    }

    /* execute rollback */
    result = PQexec(imp_dbh->conn, "rollback");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);

    if (status != PGRES_COMMAND_OK) {
        dbd_error(dbh, status, "rollback failed\n");
        return 0;
    }

    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) == FALSE) {
        result = PQexec(imp_dbh->conn, "begin");
        status = result ? PQresultStatus(result) : -1;
        PQclear(result);
        if (status != PGRES_COMMAND_OK) {
            dbd_error(dbh, status, "begin failed\n");
            return 0;
        }
    }

    return retval;
}


int
dbd_db_disconnect(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_disconnect\n"); }

    /* We assume that disconnect will always work	*/
    /* since most errors imply already disconnected.	*/
    DBIc_ACTIVE_off(imp_dbh);

    /* rollback if AutoCommit = off */
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit) == FALSE) {
        PGresult* result = 0;
        ExecStatusType status;
        result = PQexec(imp_dbh->conn, "rollback");
        status = result ? PQresultStatus(result) : -1;
        PQclear(result);
        if (status != PGRES_COMMAND_OK) {
            dbd_error(dbh, status, "rollback failed\n");
            return 0;
        }
        if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_db_disconnect: AutoCommit=off -> rollback\n"); }
    }

    PQfinish(imp_dbh->conn);

    /* We don't free imp_dbh since a reference still exists	*/
    /* The DESTROY method is the only one to 'free' memory.	*/
    /* Note that statement objects may still exists for this dbh!	*/
    return 1;
}


void
dbd_db_destroy(dbh, imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_destroy\n"); }

    if (DBIc_ACTIVE(imp_dbh)) {
	dbd_db_disconnect(dbh, imp_dbh);
    }

    /* Nothing in imp_dbh to be freed	*/
    DBIc_IMPSET_off(imp_dbh);
}


int
dbd_db_STORE_attrib(dbh, imp_dbh, keysv, valuesv)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SV *keysv;
    SV *valuesv;
{
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    int retval = TRUE;
    int newval = SvTRUE(valuesv);

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_STORE\n"); }

    if (kl==10 && strEQ(key, "AutoCommit")) {
        int oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
        DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
        if (oldval == FALSE && newval != FALSE && imp_dbh->init_auto) {
            /* do nothing, fall through */
            if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_db_STORE: initialize AutoCommit to on\n"); }
        } else if (oldval == FALSE && newval != FALSE) {
            /* commit any outstanding changes */
            PGresult* result = 0;
            ExecStatusType status;
            result = PQexec(imp_dbh->conn, "commit");
            status = result ? PQresultStatus(result) : -1;
            PQclear(result);
            if (status != PGRES_COMMAND_OK) {
                dbd_error(dbh, status, "commit failed\n");
                return 0;
            }
            if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_db_STORE: switch AutoCommit to on: commit\n"); }
	} else if ((oldval != FALSE && newval == FALSE) || (oldval == FALSE && newval == FALSE && imp_dbh->init_auto)) {
            /* start new transaction */
            PGresult* result = 0;
            ExecStatusType status;
            result = PQexec(imp_dbh->conn, "begin");
            status = result ? PQresultStatus(result) : -1;
            PQclear(result);
            if (status != PGRES_COMMAND_OK) {
                dbd_error(dbh, status, "begin failed\n");
                return 0;
            }
            if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_db_STORE: switch AutoCommit to off: begin\n"); }
        }
        imp_dbh->init_auto = 0;
    } else {
        retval = FALSE;
    }
}


SV *
dbd_db_FETCH_attrib(dbh, imp_dbh, keysv)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SV *keysv;
{
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    SV *retsv = Nullsv;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_db_FETCH\n"); }

    if (kl==10 && strEQ(key, "AutoCommit")) {
        retsv = newSViv((IV)DBIc_is(imp_dbh, DBIcf_AutoCommit));
    }

    return sv_2mortal(retsv);
}


/* ================================================================== */


int
dbd_st_prepare(sth, imp_sth, statement, attribs)
    SV *sth;
    imp_sth_t *imp_sth;
    char *statement;
    SV *attribs;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_prepare: statement = >%s<\n", statement); }

    /* scan statement for '?', ':1' and/or ':foo' style placeholders */
    dbd_preparse(imp_sth, statement);

    /* initialize new statement handle */
    imp_sth->result    = 0;
    imp_sth->cur_tuple = 0;

    DBIc_IMPSET_on(imp_sth);
    return 1;
}


static void
dbd_preparse(imp_sth, statement)
    imp_sth_t *imp_sth;
    char *statement;
{
    bool in_literal = FALSE;
    char *src, *start, *dest;
    phs_t phs_tpl;
    SV *phs_sv;
    int idx=0, style=0, laststyle=0;
    STRLEN namelen;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_preparse: statement = >%s<\n", statement); }

    /* allocate room for copy of statement with spare capacity	*/
    /* for editing '?' or ':1' into ':p1' so we can use obndrv.	*/
    imp_sth->statement = (char*)safemalloc(strlen(statement) * 3);

    /* initialise phs ready to be cloned per placeholder	*/
    memset(&phs_tpl, 0, sizeof(phs_tpl));
    phs_tpl.ftype = 1;	/* VARCHAR2 */

    src  = statement;
    dest = imp_sth->statement;
    while(*src) {
	if (*src == '\'')
	    in_literal = ~in_literal;
	if ((*src != ':' && *src != '?') || in_literal) {
	    *dest++ = *src++;
	    continue;
	}
	start = dest;			/* save name inc colon	*/ 
	*dest++ = *src++;
	if (*start == '?') {		/* X/Open standard	*/
	    sprintf(start,":p%d", ++idx); /* '?' -> ':p1' (etc)	*/
	    dest = start+strlen(start);
	    style = 3;

	} else if (isDIGIT(*src)) {	/* ':1'		*/
	    idx = atoi(src);
	    *dest++ = 'p';		/* ':1'->':p1'	*/
	    if (idx <= 0)
		croak("Placeholder :%d must be a positive number", idx);
	    while(isDIGIT(*src))
		*dest++ = *src++;
	    style = 1;

	} else if (isALNUM(*src)) {	/* ':foo'	*/
	    while(isALNUM(*src))	/* includes '_'	*/
		*dest++ = *src++;
	    style = 2;
	} else {			/* perhaps ':=' PL/SQL construct */
	    continue;
	}
	*dest = '\0';			/* handy for debugging	*/
	namelen = (dest-start);
	if (laststyle && style != laststyle)
	    croak("Can't mix placeholder styles (%d/%d)",style,laststyle);
	laststyle = style;
	if (imp_sth->all_params_hv == NULL)
	    imp_sth->all_params_hv = newHV();
	phs_tpl.sv = &sv_undef;
	phs_sv = newSVpv((char*)&phs_tpl, sizeof(phs_tpl)+namelen+1);
	hv_store(imp_sth->all_params_hv, start, namelen, phs_sv, 0);
	strcpy( ((phs_t*)(void*)SvPVX(phs_sv))->name, start);
	/* warn("params_hv: '%s'\n", start);	*/
    }
    *dest = '\0';
    if (imp_sth->all_params_hv) {
	DBIc_NUM_PARAMS(imp_sth) = (int)HvKEYS(imp_sth->all_params_hv);
	if (dbis->debug >= 2)
	    fprintf(DBILOGFP, "    dbd_preparse scanned %d distinct placeholders\n",
		(int)DBIc_NUM_PARAMS(imp_sth));
    }
}


int
dbd_st_rows(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_rows\n"); }

    return imp_sth->rows;
}


int
dbd_st_execute(sth, imp_sth)        /* <= -2:error, >=0:ok row count, (-1=unknown count) */
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    ExecStatusType status = -1;
    char *cmdStatus;
    char *cmdTuples;
    char *statement;
    int ret = -2;
    int num_fields;
    int i;
    bool in_literal = FALSE;
    char *src;
    char *dest;
    char *val;
    char namebuf[30];
    phs_t *phs;
    SV **svp;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_execute\n"); }

    /*
    here we get the statement from the statement handle where
    it has been stored when creating a blank sth during prepare
    svp = hv_fetch((HV *)SvRV(sth), "Statement", 9, FALSE);
    statement = SvPV(*svp, na);
    */

    statement = imp_sth->statement;
    if (! statement) {
        /* are we prepared ? */
        dbd_error(sth, -1, "statement not prepared\n");
        return -2;
    }

    /* do we have input parameters ? */
    if ((int)DBIc_NUM_PARAMS(imp_sth) > 0) {
        statement = (char*)safemalloc(strlen(imp_sth->statement) + imp_sth->all_params_len);
        dest = statement;
        src  = imp_sth->statement;
        while(*src) {
            if (*src == '\'') {
                in_literal = ~in_literal;
            }
            if (*src != ':' || in_literal) {
                *dest++ = *src++;
                continue;
            }
            i = 0;
            namebuf[i++] = *src++; /* ':' */
            namebuf[i++] = *src++; /* 'p' */
            while (isDIGIT(*src)) {
                namebuf[i++] = *src++;
            }
            namebuf[i] = '\0';
            svp = hv_fetch(imp_sth->all_params_hv, namebuf, i, 0);
            if (svp == NULL) {
                dbd_error(sth, -1, "parameter unknown\n");
                return -2;
            }
            phs = (phs_t*)(void*)SvPVX(*svp);
            val = neatsvpv(phs->sv, 0);
            while (*val) {
                *dest++ = *val++;
            }
        }
        *dest = '\0';
    }

    if (dbis->debug >= 2) { fprintf(DBILOGFP, "dbd_st_execute: statement = >%s<\n", statement); }

    /* execute statement */
    imp_sth->result = PQexec(imp_dbh->conn, statement);

    /* free statement string in case of input parameters */
    if ((int)DBIc_NUM_PARAMS(imp_sth) > 0) {
        Safefree(statement);
    }

    /* check status */
    status    = imp_sth->result ? PQresultStatus(imp_sth->result)      : -1;
    cmdStatus = imp_sth->result ? (char *)PQcmdStatus(imp_sth->result) : "";
    cmdTuples = imp_sth->result ? (char *)PQcmdTuples(imp_sth->result) : "";

    if (PGRES_TUPLES_OK == status) {
        /* select statement */
        num_fields = PQnfields(imp_sth->result);
        imp_sth->is_bool = (char *)malloc(num_fields);
        if (! imp_sth->is_bool) {
            return -2;
        }
        for(i = 0; i < num_fields; ++i) { /* store the columns with datatype = bool */
            if (16 == PQftype(imp_sth->result, i)) {
               imp_sth->is_bool[i] = '1';
            } else {
               imp_sth->is_bool[i] = '0';
            }
        }
        imp_sth->cur_tuple = 0;
        DBIc_NUM_FIELDS(imp_sth) = num_fields;
        DBIc_ACTIVE_on(imp_sth);

        ret = PQntuples(imp_sth->result);
    } else if (PGRES_COMMAND_OK == status) {
        /* non-select statement */
        if (! strncmp(cmdStatus, "DELETE", 6) || ! strncmp(cmdStatus, "INSERT", 6) || ! strncmp(cmdStatus, "UPDATE", 6)) {
            ret = atoi(cmdTuples);
        } else {
            ret = -1;
        }
    } else {
        dbd_error(sth, status, PQerrorMessage(imp_dbh->conn));
        ret = -2;
    }

    /* store the number of affected rows */
    imp_sth->rows = ret;

    return ret;
}


AV *
dbd_st_fetch(sth, imp_sth)
    SV *        sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    int num_fields;
    int i;
    AV *av;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_fetch\n"); }

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

        SV *sv  = AvARRAY(av)[i];
        if (PQgetisnull(imp_sth->result, imp_sth->cur_tuple, i)) {
            sv_setsv(sv, &sv_undef);
        } else {
            char *val = (char*)PQgetvalue(imp_sth->result, imp_sth->cur_tuple, i);
            if ('1' == imp_sth->is_bool[i]) {
               *val = (*val == 'f') ? '0' : '1'; /* bool: translate postgres into perl */
            }
            sv_setpv(sv, val);
        }
    }

    imp_sth->cur_tuple += 1;

    return av;
}


int
dbd_st_finish(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_finish\n"); }

    if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
        PQclear(imp_sth->result);
        imp_sth->result = 0;
        imp_sth->rows   = 0;
    }

    DBIc_ACTIVE_off(imp_sth);
    return 1;
}


void
dbd_st_destroy(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_destroy\n"); }

    /* Free off contents of imp_sth        */

    Safefree(imp_sth->statement);
    if (imp_sth->is_bool) {
        free(imp_sth->is_bool);
        imp_sth->is_bool = 0;
    }

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
dbd_st_blob_read (sth, imp_sth, lobjId, offset, len, destrv, destoffset)
    SV *sth;
    imp_sth_t *imp_sth;
    int lobjId;
    long offset;
    long len;
    SV *destrv;
    long destoffset;
{
    D_imp_dbh_from_sth;
    char err[64];
    int ret, lobj_fd, nbytes, nread;
    PGresult* result;
    ExecStatusType status;
    SV *bufsv;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_blob_read\n"); }

    /* safety check */
    if (! SvROK(destrv)) {
        dbd_error(sth, -1, "dbd_st_blob_read: destrv not a reference");
        return 0;
    }
    bufsv = SvRV(destrv);                       /* dereference destination        */
    if (! destoffset) {
        sv_setpvn(bufsv, "", 0);        /* ensure it's writable string        */
    }

    /* execute begin */
    result = PQexec(imp_dbh->conn, "begin");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);
    if (status != PGRES_COMMAND_OK) {
        dbd_error(sth, status, PQerrorMessage(imp_dbh->conn));
        return 0;
    }

    /* open large object */
    lobj_fd = lo_open(imp_dbh->conn, lobjId, INV_READ);
    if (lobj_fd < 0) {
        sprintf(&err[0], "lo_open: can't open large object %d", lobjId);
        dbd_error(sth, -1, err);
        return 0;
    }

    /* seek on large object */
    if (offset > 0) {
        ret = lo_lseek(imp_dbh->conn, lobj_fd, offset, SEEK_SET);
        if (ret < 0) {
            sprintf(&err[0], "lo_seek: can't seek large object %d", lobjId);
            dbd_error(sth, -1, err);
            return 0;
        }
    }

    /* read from large object */
    nread  = 0;
    nbytes = 1;
    while (nbytes > 0) {
        SvGROW(bufsv, destoffset + nread + BUFSIZE + 1);        /* SvGROW doesn't do +1        */
        nbytes = lo_read(imp_dbh->conn, lobj_fd, ((char*)SvPVX(bufsv)) + destoffset + nread, BUFSIZE);
        if (nbytes < 0) {
            sprintf(&err[0], "lo_read: can't read from large object %d", lobjId);
            dbd_error(sth, -1, err);
            return 0;
        }
        nread += nbytes;
        /* break if user wants only a specified chunk */
        if (len && nread > len) {
            break;
        }
    }

    /* close large object */
    ret = lo_close(imp_dbh->conn, lobj_fd);
    if (ret < 0) {
        sprintf(&err[0], "lo_close: can't close large object %d", lobjId);
        dbd_error(sth, -1, err);
        return 0;
    }

    /* execute end */
    result = PQexec(imp_dbh->conn, "end");
    status = result ? PQresultStatus(result) : -1;
    PQclear(result);
    if (status != PGRES_COMMAND_OK) {
        dbd_error(sth, status, PQerrorMessage(imp_dbh->conn));
        return 0;
    }

    /* terminate string */
    if (len && nread > len) {
        nread = len;
    }
    SvCUR_set(bufsv, destoffset + nread);
    *SvEND(bufsv) = '\0';

    return nread;
}


int
dbd_st_STORE_attrib(sth, imp_sth, keysv, valuesv)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *keysv;
    SV *valuesv;
{
    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_STORE\n"); }

    return FALSE;
}


SV *
dbd_st_FETCH_attrib(sth, imp_sth, keysv)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *keysv;
{
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    SV *retsv = Nullsv;
    int i;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_FETCH\n"); }

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


static int 
_dbd_rebind_ph(sth, imp_sth, phs) 
    SV *sth;
    imp_sth_t *imp_sth;
    phs_t *phs;
{
    STRLEN value_len;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_st_rebind\n"); }

/*        for strings, must be a PV first for ptr to be valid? */
/*    sv_insert +4        */
/*    sv_chop(phs->sv, SvPV(phs->sv,na)+4);        XXX */

    if (dbis->debug >= 2) {
        char *text = neatsvpv(phs->sv,0);
        fprintf(DBILOGFP, "bind %s <== %s (size %d/%d/%ld, ptype %ld, otype %d)\n",
        phs->name, text, SvCUR(phs->sv),SvLEN(phs->sv),phs->maxlen,
        SvTYPE(phs->sv), phs->ftype);
    }

    /* At the moment we always do sv_setsv() and rebind.        */
    /* Later we may optimise this so that more often we can        */
    /* just copy the value & length over and not rebind.        */

    if (phs->is_inout) {        /* XXX */
        if (SvREADONLY(phs->sv))
            croak(no_modify);
        /* phs->sv _is_ the real live variable, it may 'mutate' later        */
        /* pre-upgrade high to reduce risk of SvPVX realloc/move        */
        (void)SvUPGRADE(phs->sv, SVt_PVNV);
        /* ensure room for result, 28 is magic number (see sv_2pv)        */
        SvGROW(phs->sv, (phs->maxlen < 28) ? 28 : phs->maxlen+1);
        if (imp_sth->dbd_pad_empty)
            croak("Can't use dbd_pad_empty with bind_param_inout");
    }
    else {
        /* phs->sv is copy of real variable, upgrade to at least string        */
        (void)SvUPGRADE(phs->sv, SVt_PV);
    }

    /* At this point phs->sv must be at least a PV with a valid buffer,        */
    /* even if it's undef (null)                                        */
    /* Here we set phs->progv, phs->indp, and value_len.                */
    if (SvOK(phs->sv)) {
        phs->progv = SvPV(phs->sv, value_len);
        phs->indp  = 0;
    }
    else {        /* it's null but point to buffer incase it's an out var        */
        phs->progv = SvPVX(phs->sv);
        phs->indp  = -1;
        value_len  = 0;
    }
    if (imp_sth->dbd_pad_empty && value_len==0) {
        sv_setpv(phs->sv, " ");
        phs->progv = SvPV(phs->sv, value_len);
    }
    phs->sv_type = SvTYPE(phs->sv);        /* part of mutation check        */
    phs->alen    = value_len + phs->alen_incnull;
    phs->maxlen  = SvLEN(phs->sv)-1;        /* avail buffer space        */

    imp_sth->all_params_len += phs->alen;

    if (dbis->debug >= 3) {
        fprintf(DBILOGFP, "bind %s <== '%.100s' (size %d/%ld, indp %d)\n",
           phs->name, phs->progv, phs->alen, (long)phs->maxlen, phs->indp);
    }

    return 1;
}


int
dbd_bind_ph(sth, imp_sth, param, value, sql_type, attribs, is_inout, maxlen)
    SV *sth;
    imp_sth_t *imp_sth;
    SV *param;
    SV *value;
    IV sql_type;
    SV *attribs;
    int is_inout;
    IV maxlen;
{
    SV **phs_svp;
    STRLEN name_len;
    char *name;
    char namebuf[30];
    phs_t *phs;

    if (dbis->debug >= 1) { fprintf(DBILOGFP, "dbd_bind_ph\n"); }

    /* check if placeholder was passed as a number        */
    if (SvNIOK(param) || (SvPOK(param) && isDIGIT(*SvPVX(param)))) {
        name = namebuf;
        sprintf(name, ":p%d", (int)SvIV(param));
        name_len = strlen(name);
    }
    else {                /* use the supplied placeholder name directly */
        name = SvPV(param, name_len);
    }

    if (SvTYPE(value) > SVt_PVMG)        /* hook for later array logic        */
        croak("Can't bind non-scalar value (currently)");

    if (dbis->debug >= 2) {
        fprintf(DBILOGFP, "bind %s <== %s (attribs: %s)\n", name, neatsvpv(value,0), attribs ? SvPV(attribs,na) : "" );
    }

    phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
    if (phs_svp == NULL)
        croak("Can't bind unknown placeholder '%s'", name);
    phs = (phs_t*)(void*)SvPVX(*phs_svp);        /* placeholder struct        */

    if (phs->sv == &sv_undef) {        /* first bind for this placeholder        */
        phs->ftype    = 1;                /* our default type VARCHAR2        */
        phs->maxlen   = maxlen;                /* 0 if not inout                */
        phs->is_inout = is_inout;
        if (is_inout) {
            phs->sv = SvREFCNT_inc(value);        /* point to live var        */
            ++imp_sth->has_inout_params;
            /* build array of phs's so we can deal with out vars fast        */
            if (!imp_sth->out_params_av) {
                imp_sth->out_params_av = newAV();
            }
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

    if (!is_inout) {        /* normal bind to take a (new) copy of current value        */
        if (phs->sv == &sv_undef)        /* (first time bind) */
            phs->sv = newSV(0);
        sv_setsv(phs->sv, value);
    }

    return _dbd_rebind_ph(sth, imp_sth, phs);
}


/* end of dbdimp.c */
