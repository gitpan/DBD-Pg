/*
   $Id: Pg.h,v 1.17 1999/06/16 18:55:29 mergl Exp $

   Copyright (c) 1997,1998,1999 Edmund Mergl
   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "libpq-fe.h"

#ifdef NEVER
#include<sys/stat.h>
#include "libpq/libpq-fs.h"
#endif
#ifndef INV_READ
#define INV_READ 0x00040000
#endif

#ifdef BUFSIZ
#undef BUFSIZ
#endif
/* this should improve I/O performance for large objects */
#define BUFSIZ 32768


#define NEED_DBIXS_VERSION 9

#include <DBIXS.h>		/* installed by the DBI module	*/

#include "dbdimp.h"		/* read in our implementation details */

#include <dbd_xsh.h>		/* installed by the DBI module	*/

int dbd_db_ping(SV *dbh);

/* end of Pg.h */
