/*---------------------------------------------------------
 *
 * $Id: Pg.h,v 1.13 1998/04/20 20:05:59 mergl Exp $
 *
 * Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
 * Portions Copyright (c) 1997,1998           Edmund Mergl
 *
 *---------------------------------------------------------
 */


#include "libpq-fe.h"

#ifdef NEVER
#include<sys/stat.h>
#include "libpq/libpq-fs.h"
#endif
#define INV_READ 0x00040000


#define NEED_DBIXS_VERSION 9

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "dbdimp.h"		/* read in our implementation details */

#include <dbd_xsh.h>		/* installed by the DBI module	*/

int dbd_db_ping(SV *dbh);

/* end of Pg.h */
