/*---------------------------------------------------------
 *
 * $Id: Pg.h,v 1.2 1997/08/09 16:01:11 mergl Exp $
 *
 * Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
 * Portions Copyright (c) 1997                Edmund Mergl
 *
 *---------------------------------------------------------
 */


#define NEED_DBIXS_VERSION 8

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "libpq-fe.h"

/* read in our implementation details */

#include "dbdimp.h"

void dbd_init _((dbistate_t *dbistate));
void dbd_error _((SV *h, int error_num, char *error_msg));

int  dbd_db_login _((SV *dbh, char *dbname, char *uid, char *pwd));
int  dbd_db_do _((SV *dbh, char *statement, SV *attribs));
int  dbd_db_commit _((SV *dbh));
int  dbd_db_rollback _((SV *dbh));
int  dbd_db_disconnect _((SV *dbh));
void dbd_db_destroy _((SV *dbh));
int  dbd_db_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_db_FETCH _((SV *dbh, SV *keysv));

int  dbd_st_prepare _((SV *sth, char *statement, SV *attribs));
int  dbd_st_rows _((SV *sv));
int  dbd_bind_ph _((SV *h, SV *param, SV *value, SV *attribs, int is_inout, IV maxlen));
int  dbd_st_execute _((SV *sv));
AV  *dbd_st_fetch _((SV *sv));
int  dbd_st_finish _((SV *sth));
void dbd_st_destroy _((SV *sth));
int  dbd_st_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_st_FETCH _((SV *dbh, SV *keysv));


/* EOF */
