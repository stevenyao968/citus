/*-------------------------------------------------------------------------
 *
 * datautils.h
 *
 * Declarations for public utility functions related to data types.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DATAUTILS_H
#define CITUS_DATAUTILS_H

#include "postgres.h"
#include "c.h"


/* utility functions declaration shared within this module */
extern uint64 * AllocateUint64(uint64 value);


#endif /* CITUS_DATAUTILS_H */
