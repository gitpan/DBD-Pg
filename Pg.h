/*
   $Id: Pg.h,v 1.15 1998/10/11 17:40:36 mergl Exp $

   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
   Portions Copyright (c) 1997,1998           Edmund Mergl

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file,
   with the exception that it cannot be placed on a CD-ROM or similar media
   for commercial distribution without the prior approval of the author.

*/


#include "libpq-fe.h"

#ifdef NEVER
#include<sys/stat.h>
#include "libpq/libpq-fs.h"
#endif
#ifndef INV_READ
#define INV_READ 0x00040000
#endif

#ifndef BUFSIZ
#define BUFSIZ 1024
#endif


#define NEED_DBIXS_VERSION 9

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "dbdimp.h"		/* read in our implementation details */

#include <dbd_xsh.h>		/* installed by the DBI module	*/

int dbd_db_ping(SV *dbh);

/* end of Pg.h */
