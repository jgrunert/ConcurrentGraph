package mthesis.concurrent_graph.master;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class MasterQuery<Q extends BaseQueryGlobalValues> {

	private static final Logger logger = LoggerFactory.getLogger(MasterQuery.class);

	public final Q BaseQuery;
	public final long StartTime;
	public long LastStepTime;

	public int SuperstepNo;
	// Value aggregation of one superstep
	public Q QueryStepAggregator;
	// Value aggregation of all supersteps
	public Q QueryTotalAggregator;
	public int ActiveWorkers;
	public final Set<Integer> workersWaitingFor;
	public boolean IsComputing = true;


	public MasterQuery(Q query, Collection<Integer> workersToWait, BaseQueryGlobalValues.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		super();
		BaseQuery = query;
		SuperstepNo = -2;
		StartTime = System.nanoTime();
		LastStepTime = StartTime;
		workersWaitingFor = new HashSet<>(workersToWait.size());
		nextSuperstep(workersToWait);
		resetValueAggregator(queryFactory);
		QueryTotalAggregator = queryFactory.createClone(BaseQuery);
	}

	public void nextSuperstep(Collection<Integer> workersToWait) {
		workersWaitingFor.addAll(workersToWait);
		SuperstepNo++;
	}

	public void resetValueAggregator(BaseQueryGlobalValues.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		QueryStepAggregator = queryFactory.createClone(BaseQuery);
		QueryStepAggregator.setVertexCount(0);
		QueryStepAggregator.setActiveVertices(0);
		ActiveWorkers = 0;
	}

	public void aggregateQuery(Q workerQueryMsg) {
		QueryStepAggregator.combine(workerQueryMsg);
		QueryTotalAggregator.combine(workerQueryMsg);
		if (workerQueryMsg.getActiveVertices() > 0) ActiveWorkers++;
	}

	public void workersFinished(Collection<Integer> workersToWait) {
		workersWaitingFor.addAll(workersToWait);
		IsComputing = false;
		if (ActiveWorkers != 0) logger.warn("Finishing query with active workers: " + ActiveWorkers);
		if (QueryStepAggregator.getActiveVertices() != 0)
			logger.warn("Finishing query with active vertices: " + QueryStepAggregator.getActiveVertices());
	}
}
