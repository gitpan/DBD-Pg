/*-------------------------------------------------------
 *
 * $Id: Pg.h,v 1.3 1997/04/24 20:26:53 mergl Exp $
 *
 *  Portions Copyright (c) 1994,1995,1996  Tim Bunce
 *  Portions Copyright (c) 1997            Edmund Mergl
 *
 *-------------------------------------------------------
 */


#define NEED_DBIXS_VERSION 7

#include <DBIXS.h>		/* installed by the DBI module	*/


#ifdef bool
#undef bool
#endif

#ifdef DEBUG
#undef DEBUG
#endif

#ifdef ABORT
#undef ABORT
#endif

#include "postgres.h"
#include "libpq-fe.h"


/* read in our implementation details */

#include "dbdimp.h"

void dbd_init _((dbistate_t *dbistate));
void dbd_error _((SV * h, int error_num, char *error_msg));

int  dbd_db_login _((SV *dbh, char *dbname, char *uid, char *pwd));
int  dbd_db_do _((SV *dbh, char *statement, char *attribs, SV *params));
int  dbd_db_commit _((SV *dbh));
int  dbd_db_rollback _((SV *dbh));
int  dbd_db_disconnect _((SV *dbh));
void dbd_db_destroy _((SV *dbh));
int  dbd_db_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_db_FETCH _((SV *dbh, SV *keysv));

int  dbd_st_prepare _((SV *sth, char *statement, SV *attribs));
int  dbd_st_execute _((SV *sv, char *statement));
AV  *dbd_st_fetch _((SV *sv));
int  dbd_st_rows _((SV *sv));
int  dbd_st_finish _((SV *sth));
void dbd_st_destroy _((SV *sth));
int  dbd_st_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_st_FETCH _((SV *dbh, SV *keysv));


/* EOF */
