/*
 * multi_router_executor.h
 *
 * Function declarations used in executing distributed execution
 * plan.
 *
 */

#ifndef MULTI_ROUTER_EXECUTOR_H_
#define MULTI_ROUTER_EXECUTOR_H_

#include "distributed/multi_physical_planner.h"
#include "executor/execdesc.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255


/*
 * XactParticipantKey acts as the key to index into the (transaction-local)
 * hash keeping track of transaction connections and shards.
 */
typedef struct XactParticipantKey
{
	char nodeName[MAX_NODE_LENGTH + 1]; /* hostname of host to connect to */
	int32 nodePort;                     /* port of host to connect to */
} XactParticipantKey;


struct pg_conn; /* forward declared, to avoid having to include libpq-fe.h */

/* XactParticipantEntry keeps track of connections and shards themselves. */
typedef struct XactParticipantEntry
{
	XactParticipantKey cacheKey; /* hash entry key */
	struct pg_conn *connection;  /* connection to remote server, if any */
	List *shardIds;              /* shard IDs touched during the transaction */
} XactParticipantEntry;


/* Config variables managed via guc.c */
extern bool AllModificationsCommutative;


extern void RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task);
extern void RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
extern void RouterExecutorFinish(QueryDesc *queryDesc);
extern void RouterExecutorEnd(QueryDesc *queryDesc);
extern void InstallRouterExecutorShmemHook(void);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
