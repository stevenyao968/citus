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
#include "distributed/listutils.h"
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
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/int8.h"
#if (PG_VERSION_NUM >= 90500)
#include "utils/ruleutils.h"
#endif


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

static uint64 shardForTransaction = INVALID_SHARD_ID;
static bool subXactAbortAttempted = false;
static List *participantList = NIL;
static List *failedParticipantList = NIL;
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
static void RegisterRouterExecutorXactCallbacks();
static void ExecuteTransactionEnd(char *sqlCommand);
static void RouterTransactionCallback(XactEvent event, void *arg);
static void RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
										 SubTransactionId parentSubid, void *arg);


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

		if (IsTransactionBlock())
		{
			if (shardForTransaction == INVALID_SHARD_ID)
			{
				InitTransactionStateForTask(task);
			}
			else if (shardForTransaction != task->anchorShardId)
			{
				ereport(ERROR, (errmsg("CAN'T CHANGE SHARDS	")));
			}
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


/* TODO: write comment */
static void
InitTransactionStateForTask(Task *task)
{
	MemoryContext oldContext = NULL;
	ListCell *placementCell = NULL;

	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	foreach(placementCell, task->taskPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		PGconn *connection = GetOrEstablishConnection(placement->nodeName,
													  placement->nodePort);
		TxnParticipant *txnParticipant = NULL;
		bool participantHealthy = true;
		txnParticipant = (TxnParticipant *) palloc(sizeof(TxnParticipant));

		CopyShardPlacement(placement, (ShardPlacement *) txnParticipant);
		txnParticipant->connection = connection;

		if (txnParticipant->connection != NULL)
		{
			PGresult *result = PQexec(connection, "BEGIN");
			if (PQresultStatus(result) == PGRES_COMMAND_OK)
			{
				participantHealthy = true;
			}
			else
			{
				WarnRemoteError(connection, result);
			}
		}

		if (participantHealthy)
		{
			participantList = lappend(participantList, txnParticipant);
		}
		else
		{
			failedParticipantList = lappend(participantList, txnParticipant);
		}
	}

	MemoryContextSwitchTo(oldContext);

	shardForTransaction = task->anchorShardId;
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
	List *placementList = task->taskPlacementList;
	int totalPlacements = list_length(placementList);
	int totalFailures = list_length(failedParticipantList);
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

	/* if taking part in a transaction, use static transaction list */
	if (shardForTransaction != INVALID_SHARD_ID)
	{
		placementList = participantList;
	}

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, placementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;
		bool queryOK = false;
		int64 currentAffectedTupleCount = 0;
		PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);

		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, queryString);
		if (!queryOK)
		{
			PurgeConnection(connection);
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
			/* use try/catch to maintain txn participant list during fail-fast errors */
			PG_TRY();
			{
				queryOK = StoreQueryResult(routerState, connection, tupleDescriptor,
										   &currentAffectedTupleCount);
			}
			PG_CATCH();
			{
				/* this placement is already aborted; remove it from participant list */
				participantList = list_delete_ptr(participantList, taskPlacement);

				PG_RE_THROW();
			}
			PG_END_TRY();
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
								   nodeName, nodePort)));
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
			PurgeConnection(connection);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);

			continue;
		}
	}

	if (isModificationQuery)
	{
		/* total failures should count previously failed placements */
		totalFailures += list_length(failedPlacementList);

		if (shardForTransaction != INVALID_SHARD_ID)
		{
			ListCell *placementCell;
			MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

			/*
			 * Placements themselves are already in TopTransactionContext, so they
			 * will live on to the next command; just need to ensure the txn's list
			 * of placements is built in the same context.
			 */
			foreach(placementCell, failedPlacementList)
			{
				ShardPlacement *failedPlacement = lfirst(placementCell);
				failedParticipantList = lappend(failedParticipantList, failedPlacement);

				/* additionally, remove newly failed placements from txn's list */
				participantList = list_delete_ptr(participantList, failedPlacement);
			}

			MemoryContextSwitchTo(oldContext);

			/* invalidation occurs later for txns: clear local failure list */
			failedPlacementList = NIL;
		}

		/* if all placements have failed, error out */
		if (totalFailures == totalPlacements)
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
ExecuteTransactionEnd(char *sqlCommand)
{
	ListCell *participantCell = NULL;

	foreach(participantCell, participantList)
	{
		TxnParticipant *participant = (TxnParticipant *) lfirst(participantCell);
		PGconn *connection = participant->connection;
		PGresult *result = NULL;

		result = PQexec(connection, sqlCommand);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			WarnRemoteError(connection, result);

			failedParticipantList = lappend(failedParticipantList, participant);
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


/* TODO: write comment */
void
InstallRouterExecutorShmemHook()
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = RegisterRouterExecutorXactCallbacks;
}


/* TODO: Write comment */
static void
RegisterRouterExecutorXactCallbacks()
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
	if (shardForTransaction == INVALID_SHARD_ID)
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
			ListCell *failedPlacementCell = NULL;

			/* after the local transaction commits, send COMMIT to healthy remotes */
			ExecuteTransactionEnd("COMMIT TRANSACTION");

			/* now mark all failed placements inactive */
			foreach(failedPlacementCell, failedParticipantList)
			{
				ShardPlacement *failedPlacement = NULL;
				failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
				uint64 shardLength = 0;

				DeleteShardPlacementRow(failedPlacement->shardId,
										failedPlacement->nodeName,
										failedPlacement->nodePort);
				InsertShardPlacementRow(failedPlacement->shardId,
										FILE_INACTIVE, shardLength,
										failedPlacement->nodeName,
										failedPlacement->nodePort);
			}

			break;
		}

#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_ABORT:
#endif
		case XACT_EVENT_ABORT:
		{
			/* if the local transaction is aborting, send ABORT to healthy remotes */
			ExecuteTransactionEnd("ABORT TRANSACTION");

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
	shardForTransaction = INVALID_SHARD_ID;
	participantList = NIL;
	failedParticipantList = NIL;
	subXactAbortAttempted = false;
}


/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
							 SubTransactionId parentSubid, void *arg)
{
	if ((shardForTransaction != INVALID_SHARD_ID) && (event == SUBXACT_EVENT_ABORT_SUB))
	{
		/* TODO: warn here */
		subXactAbortAttempted = true;
	}
}
