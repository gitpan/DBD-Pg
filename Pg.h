/*---------------------------------------------------------
 *
 * $Id: Pg.h,v 1.9 1997/10/05 18:25:55 mergl Exp $
 *
 * Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
 * Portions Copyright (c) 1997                Edmund Mergl
 *
 *---------------------------------------------------------
 */


#include "libpq-fe.h"
#include <string.h>

#if 0
#include<sys/stat.h>
#include "libpq/libpq-fs.h"
#endif
#define INV_READ 0x00040000


#define NEED_DBIXS_VERSION 9

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "dbdimp.h"		/* read in our implementation details */

#include <dbd_xsh.h>		/* installed by the DBI module	*/


/* end of Pg.h */
