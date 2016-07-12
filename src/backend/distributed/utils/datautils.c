/*-------------------------------------------------------------------------
 *
 * datautils.c
 *
 * This file contains functions to perform useful operations on data types.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "distributed/datautils.h"


/*
 * Allocates a new uint64 on the heap and copies the input to that space,
 * returning a pointer to allocated memory. Useful for lists with 64-bit
 * integers.
 */
uint64 *
AllocateUint64(uint64 value)
{
	uint64 *allocatedValue = (uint64 *) palloc0(sizeof(uint64));
	Assert(sizeof(uint64) >= 8);

	(*allocatedValue) = value;

	return allocatedValue;
}
