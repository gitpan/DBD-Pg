/*-------------------------------------------------------
 *
 * $Id: Pg.xs,v 1.4 1997/04/24 20:26:54 mergl Exp $
 *
 *  Portions Copyright (c) 1994,1995,1996  Tim Bunce
 *  Portions Copyright (c) 1997            Edmund Mergl
 *
 *-------------------------------------------------------
 */


#include "Pg.h"


/* --- Variables --- */

DBISTATE_DECLARE;


MODULE = DBD::Pg    PACKAGE = DBD::Pg

PROTOTYPES: DISABLE

BOOT:
    items = 0;	/* avoid 'unused variable' warning */
    DBISTATE_INIT;
    dbd_init(DBIS);


void
errstr(h)
    SV *	h
    CODE:
    /* called from DBI::var TIESCALAR code for $DBI::errstr     */
    D_imp_xxh(h);
    ST(0) = sv_mortalcopy(DBIc_ERRSTR(imp_xxh));


MODULE = DBD::Pg    PACKAGE = DBD::Pg::dr

PROTOTYPES: DISABLE


void
disconnect_all(drh)
    SV *	drh
    CODE:
    if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
	D_imp_drh(drh);
	sv_setiv(DBIc_ERR(imp_drh), (IV)1);
	sv_setpv(DBIc_ERRSTR(imp_drh),
		(char*)"disconnect_all not implemented");
	DBIh_EVENT2(drh, ERROR_event,
		DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	XSRETURN(0);
    }
    /* perl_destruct with perl_destruct_level and $SIG{__WARN__} set	*/
    /* to a code ref core dumps when sv_2cv triggers warn loop.		*/
    if (perl_destruct_level)
	perl_destruct_level = 0;
    XST_mIV(0, 1);


MODULE = DBD::Pg    PACKAGE = DBD::Pg::db

PROTOTYPES: DISABLE

void
_login(dbh, dbname, uid, pwd)
    SV *	dbh
    char *	dbname
    char *	uid
    char *	pwd
    CODE:
    ST(0) = dbd_db_login(dbh, dbname, uid, pwd) ? &sv_yes : &sv_no;


int
_do(dbh, statement, attribs="", params=Nullsv)
    SV *        dbh
    char *      statement
    char *      attribs
    SV *        params
    CODE:
        RETVAL = dbd_db_do(dbh, statement, attribs, params);
    OUTPUT:
        RETVAL


void
commit(dbh)
    SV *	dbh
    CODE:
    ST(0) = dbd_db_commit(dbh) ? &sv_yes : &sv_no;

void
rollback(dbh)
    SV *	dbh
    CODE:
    ST(0) = dbd_db_rollback(dbh) ? &sv_yes : &sv_no;


void
STORE(dbh, keysv, valuesv)
    SV *	dbh
    SV *	keysv
    SV *	valuesv
    CODE:
    ST(0) = &sv_yes;
    if (!dbd_db_STORE(dbh, keysv, valuesv))
	if (!DBIS->set_attr(dbh, keysv, valuesv))
	    ST(0) = &sv_no;

void
FETCH(dbh, keysv)
    SV *	dbh
    SV *	keysv
    CODE:
    SV *valuesv = dbd_db_FETCH(dbh, keysv);
    if (!valuesv)
	valuesv = DBIS->get_attr(dbh, keysv);
    ST(0) = valuesv;	/* dbd_db_FETCH did sv_2mortal	*/


void
disconnect(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    if ( !DBIc_ACTIVE(imp_dbh) ) {
	XSRETURN_YES;
    }
    /* Check for disconnect() being called whilst refs to cursors	*/
    /* still exists. This needs some more thought.			*/
    if (DBIc_ACTIVE_KIDS(imp_dbh) && DBIc_WARN(imp_dbh) && !dirty) {
	warn("disconnect(%s) invalidates %d active cursor(s)",
	    SvPV(dbh,na), (int)DBIc_ACTIVE_KIDS(imp_dbh));
    }
    ST(0) = dbd_db_disconnect(dbh) ? &sv_yes : &sv_no;


void
DESTROY(dbh)
    SV *	dbh
    PPCODE:
    D_imp_dbh(dbh);
    ST(0) = &sv_yes;
    if (!DBIc_IMPSET(imp_dbh)) {	/* was never fully set up	*/
	if (DBIc_WARN(imp_dbh) && !dirty && dbis->debug >= 2)
	     warn("Database handle %s DESTROY ignored - never set up",
		SvPV(dbh,na));
    }
    else {
	if (DBIc_ACTIVE(imp_dbh)) {
	    if (DBIc_WARN(imp_dbh) && !dirty)
		 warn("Database handle destroyed without explicit disconnect");
	    dbd_db_disconnect(dbh);
	}
	dbd_db_destroy(dbh);
    }


MODULE = DBD::Pg    PACKAGE = DBD::Pg::st

PROTOTYPES: DISABLE

void
_prepare(sth, statement, attribs=Nullsv)
    SV *	sth
    char *	statement
    SV *	attribs
    CODE:
    DBD_ATTRIBS_CHECK("_prepare", sth, attribs);
    ST(0) = dbd_st_prepare(sth, statement, attribs) ? &sv_yes : &sv_no;


void
rows(sth)
    SV *	sth
    CODE:
    XST_mIV(0, dbd_st_rows(sth));


void
bind_param(sth, param, value, attribs=Nullsv)
    SV *	sth
    SV *	param
    SV *	value
    SV *	attribs
    CODE:
    croak("DBD::Pg::bind_param is not implemented\n");
    /*
    DBD_ATTRIBS_CHECK("bind_param", sth, attribs);
    ST(0) = dbd_bind_ph(sth, param, value, attribs, FALSE, 0) ? &sv_yes : &sv_no;
    */


void
bind_param_inout(sth, param, value_ref, maxlen, attribs=Nullsv)
    SV *	sth
    SV *	param
    SV *	value_ref
    IV 		maxlen
    SV *	attribs
    CODE:
    croak("DBD::Pg::bind_param is not implemented\n");
    /*
    DBD_ATTRIBS_CHECK("bind_param_inout", sth, attribs);
	if (!SvROK(value_ref))
		croak("bind_param_inout needs a reference to the value");
    ST(0) = dbd_bind_ph(sth, param, SvRV(value_ref), attribs, TRUE, maxlen) ? &sv_yes : &sv_no;
    */


void
execute(sth)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    int retval;
    SV** svp;
    char* statement;
    svp = hv_fetch((HV *)SvRV(sth), "Statement", 9, FALSE);
    statement = SvPV(*svp, na);
    retval = dbd_st_execute(sth, statement);
    if (retval < 0)
	XST_mUNDEF(0);		/* error        		*/
    else if (retval == 0)
	XST_mPV(0, "0E0");	/* true but zero		*/
    else
	XST_mIV(0, retval);	/* typically 1 or rowcount	*/


void
fetch(sth)
    SV *	sth
    CODE:
    AV *av = dbd_st_fetch(sth);
    ST(0) = (av) ? sv_2mortal(newRV((SV *)av)) : &sv_undef;


void
fetchrow(sth)
    SV *	sth
    PPCODE:
    D_imp_sth(sth);
    AV *av;
    av = dbd_st_fetch(sth);
    if (av) {
	int num_fields = AvFILL(av)+1;
	int i;
	EXTEND(sp, num_fields);
	for(i=0; i < num_fields; ++i) {
	    PUSHs(AvARRAY(av)[i]);
	}
    }



void
STORE(sth, keysv, valuesv)
    SV *	sth
    SV *	keysv
    SV *	valuesv
    CODE:
    ST(0) = &sv_yes;
    if (!dbd_st_STORE(sth, keysv, valuesv))
	if (!DBIS->set_attr(sth, keysv, valuesv))
	    ST(0) = &sv_no;


void
FETCH(sth, keysv)
    SV *	sth
    SV *	keysv
    CODE:
    SV *valuesv = dbd_st_FETCH(sth, keysv);
    if (!valuesv)
	valuesv = DBIS->get_attr(sth, keysv);
    ST(0) = valuesv;	/* dbd_st_FETCH did sv_2mortal	*/


void
finish(sth)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    if (!DBIc_ACTIVE(imp_dbh)) {
	/* Either an explicit disconnect() or global destruction	*/
	/* has disconnected us from the database. Finish is meaningless	*/
	/* XXX warn */
	XSRETURN_YES;
    }
    if (!DBIc_ACTIVE(imp_sth)) {
	/* No active statement to finish	*/
	XSRETURN_YES;
    }
    ST(0) = dbd_st_finish(sth) ? &sv_yes : &sv_no;


void
DESTROY(sth)
    SV *	sth
    PPCODE:
    D_imp_sth(sth);
    ST(0) = &sv_yes;
    if (!DBIc_IMPSET(imp_sth)) {	/* was never fully set up	*/
	if (DBIc_WARN(imp_sth) && !dirty && dbis->debug >= 2)
	     warn("Statement handle %s DESTROY ignored - never set up",
		SvPV(sth,na));
    }
    else {
	if (DBIc_ACTIVE(imp_sth))
	    dbd_st_finish(sth);
	dbd_st_destroy(sth);
    }


# EOF
