/*
 * multi_router_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * with synchronous connections. The routines utilize the connection cache.
 * Therefore, only a single connection is opened for each worker. Also, router
 * executor does not require a master table and a master query. In other words,
 * the results that are fetched from a single worker is sent to the output console
 * directly. Lastly, router executor can only execute a single task.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_cache.h"
#include "distributed/datautils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/int8.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#if (PG_VERSION_NUM >= 90500)
#include "utils/ruleutils.h"
#endif


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

/*
 * The following static variables are necessary to track the progression of
 * multi-statement transactions managed by the router executor. After the first
 * modification within a transaction (whether started by the use of a function
 * or the use of BEGIN), the executor populates a hash with the transaction's
 * initial participants (the nodes hit by that initial modification). Beyond
 * that, there's a flag to track when a user tries to roll back to a savepoint
 * (not allowed) and a backend startup hook to register xact callbacks.
 */
static HTAB *xactParticipantHash = NULL;
static bool subXactAbortAttempted = false;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void InitTransactionStateForTask(Task *task);
static LOCKMODE CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery);
static void AcquireExecutorShardLock(Task *task, LOCKMODE lockMode);
static bool ExecuteTaskAndStoreResults(QueryDesc *queryDesc,
									   Task *task,
									   bool isModificationQuery,
									   bool expectResults);
static uint64 ReturnRowsFromTuplestore(uint64 tupleCount, TupleDesc tupleDescriptor,
									   DestReceiver *destination,
									   Tuplestorestate *tupleStore);
static void DeparseShardQuery(Query *query, Task *task, StringInfo queryString);
static bool SendQueryInSingleRowMode(PGconn *connection, char *query);
static bool StoreQueryResult(MaterialState *routerState, PGconn *connection,
							 TupleDesc tupleDescriptor, int64 *rows);
static bool ConsumeQueryResult(PGconn *connection, int64 *rows);
static void RegisterRouterExecutorXactCallbacks(void);
static void ExecuteTransactionEnd(bool commit);
static void RouterTransactionCallback(XactEvent event, void *arg);
static void RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
										 SubTransactionId parentSubid, void *arg);
static HTAB * CreateXactParticipantHash(void);
static PGconn * GetConnectionForPlacement(ShardPlacement *placement,
										  bool isModificationQuery);
static void PurgeConnectionForPlacement(ShardPlacement *placement);
static void RecordParticipatingShardId(uint64 newShardId,
									   XactParticipantEntry *participant);
static void MarkParticipantShardsUnhealthy(XactParticipantEntry *participant);


/*
 * RouterExecutorStart sets up the executor state and queryDesc for router
 * execution.
 */
void
RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task)
{
	LOCKMODE lockMode = NoLock;
	EState *executorState = NULL;
	CmdType commandType = queryDesc->operation;

	/* ensure that the task is not NULL */
	Assert(task != NULL);

	/* disallow triggers during distributed modify commands */
	if (commandType != CMD_SELECT)
	{
		eflags |= EXEC_FLAG_SKIP_TRIGGERS;

		if (IsTransactionBlock() && xactParticipantHash == NULL)
		{
			InitTransactionStateForTask(task);
		}
	}

	/* signal that it is a router execution */
	eflags |= EXEC_FLAG_CITUS_ROUTER_EXECUTOR;

	/* build empty executor state to obtain per-query memory context */
	executorState = CreateExecutorState();
	executorState->es_top_eflags = eflags;
	executorState->es_instrument = queryDesc->instrument_options;

	queryDesc->estate = executorState;

	/*
	 * As it's similar to what we're doing, use a MaterialState node to store
	 * our state. This is used to store our tuplestore, so cursors etc. can
	 * work.
	 */
	queryDesc->planstate = (PlanState *) makeNode(MaterialState);

#if (PG_VERSION_NUM < 90500)

	/* make sure that upsertQuery is false for versions that UPSERT is not available */
	Assert(task->upsertQuery == false);
#endif

	lockMode = CommutativityRuleToLockMode(commandType, task->upsertQuery);

	if (lockMode != NoLock)
	{
		AcquireExecutorShardLock(task, lockMode);
	}
}


/*
 * InitTransactionStateForTask is called during executor start with the first
 * modifying (INSERT/UPDATE/DELETE) task during a transaction. It creates the
 * transaction participant hash, opens connections to this task's nodes, and
 * populates the hash with those connections after sending BEGIN commands to
 * each. If a node fails to respond, its connection is set to NULL to prevent
 * further interaction with it during the transaction.
 */
static void
InitTransactionStateForTask(Task *task)
{
	MemoryContext oldContext = NULL;
	ListCell *placementCell = NULL;

	xactParticipantHash = CreateXactParticipantHash();

	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	foreach(placementCell, task->taskPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		XactParticipantKey participantKey;
		XactParticipantEntry *participantEntry = NULL;
		bool entryFound = false;

		PGconn *connection = NULL;
		uint64 *shardIdPtr = NULL;

		MemSet(&participantKey, 0, sizeof(participantKey));
		strlcpy(participantKey.nodeName, placement->nodeName,
				MAX_NODE_LENGTH + 1);
		participantKey.nodePort = placement->nodePort;

		participantEntry = hash_search(xactParticipantHash, &participantKey,
									   HASH_ENTER, &entryFound);
		Assert(!entryFound);

		connection = GetOrEstablishConnection(placement->nodeName,
											  placement->nodePort);
		if (connection != NULL)
		{
			PGresult *result = PQexec(connection, "BEGIN");
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				WarnRemoteError(connection, result);
				PurgeConnection(connection);

				connection = NULL;
			}

			PQclear(result);
		}

		participantEntry->connection = connection;

		shardIdPtr = AllocateUint64(placement->shardId);

		participantEntry->shardIds = list_make1(shardIdPtr);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CommutativityRuleToLockMode determines the commutativity rule for the given
 * command and returns the appropriate lock mode to enforce that rule. The
 * function assumes a SELECT doesn't modify state and therefore is commutative
 * with all other commands. The function also assumes that an INSERT commutes
 * with another INSERT, but not with an UPDATE/DELETE/UPSERT; and an
 * UPDATE/DELETE/UPSERT doesn't commute with an INSERT, UPDATE, DELETE or UPSERT.
 *
 * Note that the above comment defines INSERT INTO ... ON CONFLICT type of queries
 * as an UPSERT. Since UPSERT is not defined as a separate command type in postgres,
 * we have to pass it as a second parameter to the function.
 *
 * The above mapping is overridden entirely when all_modifications_commutative
 * is set to true. In that case, all commands just claim a shared lock. This
 * allows the shard repair logic to lock out modifications while permitting all
 * commands to otherwise commute.
 */
static LOCKMODE
CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery)
{
	LOCKMODE lockMode = NoLock;

	/* bypass commutativity checks when flag enabled */
	if (AllModificationsCommutative)
	{
		return ShareLock;
	}

	if (commandType == CMD_SELECT)
	{
		lockMode = NoLock;
	}
	else if (upsertQuery)
	{
		lockMode = ExclusiveLock;
	}
	else if (commandType == CMD_INSERT)
	{
		lockMode = ShareLock;
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		lockMode = ExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	return lockMode;
}


/*
 * AcquireExecutorShardLock: acquire shard lock needed for execution of
 * a single task within a distributed plan.
 */
static void
AcquireExecutorShardLock(Task *task, LOCKMODE lockMode)
{
	int64 shardId = task->anchorShardId;

	LockShardResource(shardId, lockMode);
}


/*
 * RouterExecutorRun actually executes a single task on a worker.
 */
void
RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	MultiPlan *multiPlan = GetMultiPlan(planStatement);
	List *taskList = multiPlan->workerJob->taskList;
	Task *task = NULL;
	EState *estate = queryDesc->estate;
	CmdType operation = queryDesc->operation;
	MemoryContext oldcontext = NULL;
	DestReceiver *destination = queryDesc->dest;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	bool sendTuples = operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning;

	/* router executor can only execute distributed plans with a single task */
	Assert(list_length(taskList) == 1);
	task = (Task *) linitial(taskList);

	Assert(estate != NULL);
	Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));
	Assert(task != NULL);

	/* we only support default scan direction and row fetch count */
	if (!ScanDirectionIsForward(direction))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("scan directions other than forward scans "
							   "are unsupported")));
	}

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	if (queryDesc->totaltime != NULL)
	{
		InstrStartNode(queryDesc->totaltime);
	}

	estate->es_processed = 0;

	/* startup the tuple receiver */
	if (sendTuples)
	{
		(*destination->rStartup)(destination, operation, queryDesc->tupDesc);
	}

	/*
	 * If query has not yet been executed, do so now. The main reason why the
	 * query might already have been executed is cursors.
	 */
	if (!routerState->eof_underlying)
	{
		bool resultsOK = false;
		bool isModificationQuery = false;

		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE)
		{
			isModificationQuery = true;
		}
		else if (operation != CMD_SELECT)
		{
			ereport(ERROR, (errmsg("unrecognized operation code: %d",
								   (int) operation)));
		}

		resultsOK = ExecuteTaskAndStoreResults(queryDesc, task,
											   isModificationQuery,
											   sendTuples);
		if (!resultsOK)
		{
			ereport(ERROR, (errmsg("could not receive query results")));
		}

		/* mark underlying query as having executed */
		routerState->eof_underlying = true;
	}

	/* if the underlying query produced output, return it */
	if (routerState->tuplestorestate != NULL)
	{
		TupleDesc resultTupleDescriptor = queryDesc->tupDesc;
		int64 returnedRows = 0;

		/* return rows from the tuplestore */
		returnedRows = ReturnRowsFromTuplestore(count, resultTupleDescriptor,
												destination,
												routerState->tuplestorestate);

		/*
		 * Count tuples processed, if this is a SELECT.  (For modifications
		 * it'll already have been increased, as we want the number of
		 * modified tuples, not the number of RETURNed tuples.)
		 */
		if (operation == CMD_SELECT)
		{
			estate->es_processed += returnedRows;
		}
	}

	/* shutdown tuple receiver, if we started it */
	if (sendTuples)
	{
		(*destination->rShutdown)(destination);
	}

	if (queryDesc->totaltime != NULL)
	{
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
	}

	MemoryContextSwitchTo(oldcontext);
}


/*
 * ExecuteTaskAndStoreResults executes the task on the remote node, retrieves
 * the results and stores them, if SELECT or RETURNING is used, in a tuple
 * store.
 *
 * If the task fails on one of the placements, the function retries it on
 * other placements (SELECT), reraises the remote error (constraint violation
 * in DML), marks the affected placement as invalid (DML on some placement
 * failed), or errors out (DML failed on all placements).
 */
static bool
ExecuteTaskAndStoreResults(QueryDesc *queryDesc, Task *task,
						   bool isModificationQuery,
						   bool expectResults)
{
	TupleDesc tupleDescriptor = queryDesc->tupDesc;
	EState *executorState = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	bool resultsOK = false;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	ListCell *failedPlacementCell = NULL;
	int64 affectedTupleCount = -1;
	bool gotResults = false;
	char *queryString = task->queryString;

	if (isModificationQuery && task->requiresMasterEvaluation)
	{
		PlannedStmt *planStatement = queryDesc->plannedstmt;
		MultiPlan *multiPlan = GetMultiPlan(planStatement);
		Query *query = multiPlan->workerJob->jobQuery;
		StringInfo queryStringInfo = makeStringInfo();

		ExecuteMasterEvaluableFunctions(query);
		DeparseShardQuery(query, task, queryStringInfo);
		queryString = queryStringInfo->data;

		elog(DEBUG4, "query before master evaluation: %s", task->queryString);
		elog(DEBUG4, "query after master evaluation:  %s", queryString);
	}

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		bool queryOK = false;
		int64 currentAffectedTupleCount = 0;
		PGconn *connection = GetConnectionForPlacement(taskPlacement,
													   isModificationQuery);

		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, queryString);
		if (!queryOK)
		{
			PurgeConnectionForPlacement(taskPlacement);
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		/*
		 * If caller is interested, store query results the first time
		 * through. The output of the query's execution on other shards is
		 * discarded if we run there (because it's a modification query).
		 */
		if (!gotResults && expectResults)
		{
			queryOK = StoreQueryResult(routerState, connection, tupleDescriptor,
									   &currentAffectedTupleCount);
		}
		else
		{
			queryOK = ConsumeQueryResult(connection, &currentAffectedTupleCount);
		}

		if (queryOK)
		{
			if ((affectedTupleCount == -1) ||
				(affectedTupleCount == currentAffectedTupleCount))
			{
				affectedTupleCount = currentAffectedTupleCount;
			}
			else
			{
				ereport(WARNING,
						(errmsg("modified "INT64_FORMAT " tuples, but expected "
														"to modify "INT64_FORMAT,
								currentAffectedTupleCount, affectedTupleCount),
						 errdetail("modified placement on %s:%d",
								   taskPlacement->nodeName, taskPlacement->nodePort)));
			}

#if (PG_VERSION_NUM < 90600)

			/* before 9.6, PostgreSQL used a uint32 for this field, so check */
			Assert(currentAffectedTupleCount <= 0xFFFFFFFF);
#endif

			resultsOK = true;
			gotResults = true;

			/*
			 * Modifications have to be executed on all placements, but for
			 * read queries we can stop here.
			 */
			if (!isModificationQuery)
			{
				break;
			}
		}
		else
		{
			PurgeConnectionForPlacement(taskPlacement);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);

			continue;
		}
	}

	if (isModificationQuery)
	{
		/* if all placements failed, error out */
		if (list_length(failedPlacementList) == list_length(task->taskPlacementList))
		{
			ereport(ERROR, (errmsg("could not modify any active placements")));
		}

		/* otherwise, mark failed placements as inactive: they're stale */
		foreach(failedPlacementCell, failedPlacementList)
		{
			ShardPlacement *failedPlacement =
				(ShardPlacement *) lfirst(failedPlacementCell);
			uint64 shardLength = 0;

			DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
									failedPlacement->nodePort);
			InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
									failedPlacement->nodeName, failedPlacement->nodePort);
		}

		executorState->es_processed = affectedTupleCount;
	}

	return resultsOK;
}


static void
DeparseShardQuery(Query *query, Task *task, StringInfo queryString)
{
	uint64 shardId = task->anchorShardId;
	Oid relid = ((RangeTblEntry *) linitial(query->rtable))->relid;

	deparse_shard_query(query, relid, shardId, queryString);
}


/*
 * ReturnRowsFromTuplestore moves rows from a given tuplestore into a
 * receiver. It performs the necessary limiting to support cursors.
 */
static uint64
ReturnRowsFromTuplestore(uint64 tupleCount, TupleDesc tupleDescriptor,
						 DestReceiver *destination, Tuplestorestate *tupleStore)
{
	TupleTableSlot *tupleTableSlot = NULL;
	uint64 currentTupleCount = 0;

	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		(*destination->receiveSlot)(tupleTableSlot, destination);

		ExecClearTuple(tupleTableSlot);

		currentTupleCount++;

		/*
		 * If numberTuples is zero fetch all tuples, otherwise stop after
		 * count tuples.
		 */
		if (tupleCount > 0 && tupleCount == currentTupleCount)
		{
			break;
		}
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	return currentTupleCount;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(PGconn *connection, char *query)
{
	int querySent = 0;
	int singleRowMode = 0;

	querySent = PQsendQuery(connection, query);
	if (querySent == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection);
	if (singleRowMode == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	return true;
}


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results, and stores them in the a newly created
 * tuple-store. If the function can't receive query results, it returns
 * false. Note that this function assumes the query has already been sent on
 * the connection.
 */
static bool
StoreQueryResult(MaterialState *routerState, PGconn *connection,
				 TupleDesc tupleDescriptor, int64 *rows)
{
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	Tuplestorestate *tupleStore = NULL;
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	bool commandFailed = false;
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"StoreQueryResult",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);
	*rows = 0;

	if (routerState->tuplestorestate == NULL)
	{
		routerState->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
	}
	else
	{
		/* might have failed query execution on another placement before */
		tuplestore_clear(routerState->tuplestorestate);
	}

	tupleStore = routerState->tuplestorestate;

	for (;;)
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection);
		if (result == NULL)
		{
			break;
		}

		resultStatus = PQresultStatus(result);
		if ((resultStatus != PGRES_SINGLE_TUPLE) && (resultStatus != PGRES_TUPLES_OK))
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool raiseError = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			raiseError = SqlStateMatchesCategory(sqlStateString, category);

			if (raiseError)
			{
				ReraiseRemoteError(connection, result);
			}
			else
			{
				WarnRemoteError(connection, result);
			}

			PQclear(result);

			commandFailed = true;

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);
		Assert(columnCount == expectedColumnCount);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);
			(*rows)++;
		}

		PQclear(result);
	}

	pfree(columnArray);

	return !commandFailed;
}


/*
 * ConsumeQueryResult gets a query result from a connection, counting the rows
 * and checking for errors, but otherwise discarding potentially returned
 * rows.  Returns true if a non-error result has been returned, false if there
 * has been an error.
 */
static bool
ConsumeQueryResult(PGconn *connection, int64 *rows)
{
	bool commandFailed = false;
	bool gotResponse = false;

	*rows = 0;

	/*
	 * Due to single row mode we have to do multiple PQgetResult() to finish
	 * processing of this query, even without RETURNING. For single-row mode
	 * we have to loop until all rows are consumed.
	 */
	while (true)
	{
		PGresult *result = PQgetResult(connection);
		ExecStatusType status = PGRES_COMMAND_OK;

		if (result == NULL)
		{
			break;
		}

		status = PQresultStatus(result);

		if (status != PGRES_COMMAND_OK &&
			status != PGRES_SINGLE_TUPLE &&
			status != PGRES_TUPLES_OK)
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool raiseError = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			raiseError = SqlStateMatchesCategory(sqlStateString, category);

			if (raiseError)
			{
				ReraiseRemoteError(connection, result);
			}
			else
			{
				WarnRemoteError(connection, result);
			}
			PQclear(result);

			commandFailed = true;

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		if (status == PGRES_COMMAND_OK)
		{
			char *currentAffectedTupleString = PQcmdTuples(result);
			int64 currentAffectedTupleCount = 0;

			scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
			Assert(currentAffectedTupleCount >= 0);

#if (PG_VERSION_NUM < 90600)

			/* before 9.6, PostgreSQL used a uint32 for this field, so check */
			Assert(currentAffectedTupleCount <= 0xFFFFFFFF);
#endif
			*rows += currentAffectedTupleCount;
		}
		else
		{
			*rows += PQntuples(result);
		}

		PQclear(result);
		gotResponse = true;
	}

	return gotResponse && !commandFailed;
}


/*
 * ExecuteTransactionEnd ends any remote transactions still taking place on
 * remote nodes. It uses txnPlacementList to know which placements still need
 * final COMMIT or ABORT commands. Any failures are added to the other list,
 * failedTxnPlacementList, to eventually be marked as failed.
 */
static void
ExecuteTransactionEnd(bool commit)
{
	char *sqlCommand = commit ? "COMMIT TRANSACTION" : "ABORT TRANSACTION";
	HASH_SEQ_STATUS scan;
	XactParticipantEntry *participant;

	hash_seq_init(&scan, xactParticipantHash);
	while ((participant = (XactParticipantEntry *) hash_seq_search(&scan)))
	{
		PGconn *connection = participant->connection;
		PGresult *result = NULL;

		if (PQstatus(connection) != CONNECTION_OK)
		{
			continue;
		}

		result = PQexec(connection, sqlCommand);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			WarnRemoteError(connection, result);
			PurgeConnection(connection);

			if (commit)
			{
				MarkParticipantShardsUnhealthy(participant);
			}
		}

		PQclear(result);
	}
}


/*
 * RouterExecutorFinish cleans up after a distributed execution.
 */
void
RouterExecutorFinish(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	Assert(estate != NULL);

	estate->es_finished = true;
}


/*
 * RouterExecutorEnd cleans up the executor state after a distributed
 * execution.
 */
void
RouterExecutorEnd(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;

	if (routerState->tuplestorestate)
	{
		tuplestore_end(routerState->tuplestorestate);
	}

	Assert(estate != NULL);

	FreeExecutorState(estate);
	queryDesc->estate = NULL;
	queryDesc->totaltime = NULL;
}


/*
 * InstallRouterExecutorShmemHook simply installs a hook (intended to be called
 * once during backend startup), which will itself register all the transaction
 * callbacks needed by this executor.
 */
void
InstallRouterExecutorShmemHook(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = RegisterRouterExecutorXactCallbacks;
}


/*
 * RegisterRouterExecutorXactCallbacks registers (sub-)transaction callbacks
 * needed by this executor before calling any previous shmem startup hooks.
 */
static void
RegisterRouterExecutorXactCallbacks(void)
{
	RegisterXactCallback(RouterTransactionCallback, NULL);
	RegisterSubXactCallback(RouterSubtransactionCallback, NULL);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * RouterTransactionCallback handles committing or aborting remote transactions
 * after the local one has committed or aborted. It only sends COMMIT or ABORT
 * commands to still-healthy remotes; the failed ones are marked as inactive if
 * after a successful COMMIT (no need to mark on ABORTs).
 */
static void
RouterTransactionCallback(XactEvent event, void *arg)
{
	if (xactParticipantHash == NULL)
	{
		return;
	}

	switch (event)
	{
#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_COMMIT:
#endif
		case XACT_EVENT_COMMIT:
		{
			bool commit = true;

			ExecuteTransactionEnd(commit);

			break;
		}

#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_ABORT:
#endif
		case XACT_EVENT_ABORT:
		{
			bool commit = false;

			ExecuteTransactionEnd(commit);

			break;
		}

		/* no support for prepare with long-running transactions */
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_PREPARE:
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot prepare a transaction that modified "
								   "distributed tables")));

			break;
		}

#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif
		case XACT_EVENT_PRE_COMMIT:
		{
			if (subXactAbortAttempted)
			{
				subXactAbortAttempted = false;

				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot ROLLBACK TO SAVEPOINT in transactions "
									   "which modify distributed tables")));
			}

			/* leave early to avoid resetting transaction state */
			return;
		}
	}

	/* reset transaction state */
	hash_destroy(xactParticipantHash);
	xactParticipantHash = NULL;
	subXactAbortAttempted = false;
}


/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
							 SubTransactionId parentSubid, void *arg)
{
	if ((xactParticipantHash != NULL) && (event == SUBXACT_EVENT_ABORT_SUB))
	{
		subXactAbortAttempted = true;
	}
}


/*
 * CreateXactParticipantHash initializes the map used to store connections and
 * dirty shard information needed to process distributed transactions. As sub-
 * sequent operations touch new shards, they are added to a list within each
 * map entry, identified by host name and port. At transaction end, if a node
 * rejects the final COMMIT command, this list is used to its shards unhealthy.
 */
static HTAB *
CreateXactParticipantHash(void)
{
	HTAB *xactParticipantHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(XactParticipantKey);
	info.entrysize = sizeof(XactParticipantEntry);
	info.hcxt = TopTransactionContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT);

#if (PG_VERSION_NUM >= 90500)
	hashFlags |= HASH_BLOBS;
#else
	hashFlags |= HASH_FUNCTION;
	info.hash = tag_hash;
#endif

	xactParticipantHash = hash_create("citus xact participant hash", 32, &info,
									  hashFlags);

	return xactParticipantHash;
}


/*
 * GetConnectionForPlacement is the main entry point for acquiring a connection
 * within the router executor. By using placements (rather than node names and
 * ports) to identify connections, the router executor keep track of shards hit
 * by a long-running transaction (and error out if a transaction attempts to
 * reach a new node altogether). In the context of single-statement commands,
 * GetConnectionForPlacement simply falls through to  GetOrEstablishConnection.
 */
static PGconn *
GetConnectionForPlacement(ShardPlacement *placement, bool isModificationQuery)
{
	XactParticipantKey participantKey;
	XactParticipantEntry *participantEntry = NULL;
	bool entryFound = false;

	/* if not in a transaction, fall through to connection cache */
	if (xactParticipantHash == NULL)
	{
		PGconn *connection = GetOrEstablishConnection(placement->nodeName,
													  placement->nodePort);

		return connection;
	}

	Assert(IsTransactionBlock());

	MemSet(&participantKey, 0, sizeof(participantKey));
	strlcpy(participantKey.nodeName, placement->nodeName, MAX_NODE_LENGTH + 1);
	participantKey.nodePort = placement->nodePort;

	participantEntry = hash_search(xactParticipantHash, &participantKey, HASH_FIND,
								   &entryFound);

	if (entryFound)
	{
		if (isModificationQuery)
		{
			RecordParticipatingShardId(placement->shardId, participantEntry);
		}

		return participantEntry->connection;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
						errmsg("no transaction participant matches %s:%d",
							   placement->nodeName, placement->nodePort),
						errdetail("Transactions which modify distributed tables may only "
								  "target nodes affected by the modification command "
								  "which began the transaction.")));
	}
}


/*
 * PurgeConnectionForPlacement provides a way to purge an invalid connection
 * from all relevant connection hashes using the placement involved in the
 * query at the time of the error. If a transaction is ongoing, this function
 * ensures the right node's connection is set to NULL in the participant map
 * for the transaction in addition to purging the connection cache's entry.
 */
static void
PurgeConnectionForPlacement(ShardPlacement *placement)
{
	NodeConnectionKey nodeKey;
	char *currentUser = CurrentUserName();

	MemSet(&nodeKey, 0, sizeof(NodeConnectionKey));
	strlcpy(nodeKey.nodeName, placement->nodeName, MAX_NODE_LENGTH + 1);
	nodeKey.nodePort = placement->nodePort;
	strlcpy(nodeKey.nodeUser, currentUser, NAMEDATALEN);

	PurgeConnectionByKey(&nodeKey);

	if (xactParticipantHash != NULL)
	{
		XactParticipantKey participantKey;
		XactParticipantEntry *participantEntry = NULL;
		bool entryFound = false;

		Assert(IsTransactionBlock());

		memcpy(&participantKey, &nodeKey, sizeof(XactParticipantKey));
		participantEntry = hash_search(xactParticipantHash, &participantKey, HASH_FIND,
									   &entryFound);

		Assert(entryFound);

		participantEntry->connection = NULL;
	}
}


/*
 * RecordParticipatingShardId simply adds a shard identifier to the list of
 * dirtied shards in a transaction participant entry. Conceptually similar to
 * list_append_uniq, but with uint64 and memory context handling.
 */
static void
RecordParticipatingShardId(uint64 newShardId, XactParticipantEntry *participant)
{
	ListCell *shardCell = NULL;
	MemoryContext oldContext = NULL;
	uint64 *newShardIdPtr = NULL;

	foreach(shardCell, participant->shardIds)
	{
		uint64 *shardIdPointer = (uint64 *) lfirst(shardCell);
		uint64 shardId = (*shardIdPointer);

		if (shardId == newShardId)
		{
			return;
		}
	}

	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	newShardIdPtr = AllocateUint64(newShardId);
	participant->shardIds = lappend(participant->shardIds, newShardIdPtr);

	MemoryContextSwitchTo(oldContext);
}


/*
 * MarkParticipantShardsUnhealthy marks all of a transaction participant's
 * shards as inactive. Used after a final COMMIT has failed (otherwise shards
 * are just marked inactive as each modification command fails).
 */
static void
MarkParticipantShardsUnhealthy(XactParticipantEntry *participant)
{
	ListCell *shardCell = NULL;
	XactParticipantKey *key = &participant->cacheKey;

	foreach(shardCell, participant->shardIds)
	{
		uint64 *shardIdPointer = (uint64 *) lfirst(shardCell);
		uint64 shardId = (*shardIdPointer);
		uint64 shardLength = 0;

		DeleteShardPlacementRow(shardId, key->nodeName, key->nodePort);
		InsertShardPlacementRow(shardId, FILE_INACTIVE, shardLength,
								key->nodeName, key->nodePort);
	}
}
