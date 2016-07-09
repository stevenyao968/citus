/*
 * multi_router_executor.h
 *
 * Function declarations used in executing distributed execution
 * plan.
 *
 */

#ifndef MULTI_ROUTER_EXECUTOR_H_
#define MULTI_ROUTER_EXECUTOR_H_

#include "libpq-fe.h"

#include "distributed/multi_physical_planner.h"
#include "executor/execdesc.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255

extern bool AllModificationsCommutative;

/*
 * XactParticipantKey acts as the key to index into the (process-local) hash
 * keeping track of open connections. Node name and port are sufficient.
 */
typedef struct XactParticipantKey
{
	char nodeName[MAX_NODE_LENGTH + 1]; /* hostname of host to connect to */
	int32 nodePort;                     /* port of host to connect to */
} XactParticipantKey;


/* XactParticipantEntry keeps track of connections themselves. */
typedef struct XactParticipantEntry
{
	XactParticipantKey cacheKey; /* hash entry key */
	PGconn *connection;          /* connection to remote server, if any */
	List *shardIds;              /* shard IDs touched during the transaction */
} XactParticipantEntry;

extern void RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task);
extern void RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
extern void RouterExecutorFinish(QueryDesc *queryDesc);
extern void RouterExecutorEnd(QueryDesc *queryDesc);
extern void InstallRouterExecutorShmemHook(void);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
