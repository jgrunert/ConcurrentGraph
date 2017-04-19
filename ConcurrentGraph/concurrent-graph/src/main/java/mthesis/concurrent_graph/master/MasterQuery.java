package mthesis.concurrent_graph.master;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;

public class MasterQuery<Q extends BaseQuery> {

	private static final Logger logger = LoggerFactory.getLogger(MasterQuery.class);

	public final Q BaseQuery;
	public final long StartTime;
	public long LastStepTime;
	private final BaseQueryGlobalValuesFactory<Q> queryValueFactory;

	/** Number of the last superstep finished by all workers */
	public int LastFinishedSuperstepNo;
	/** Number of the current superstep, latest started superstep */
	public int StartedSuperstepNo;
	// Value aggregation of one superstep
	public Q QueryStepAggregator;
	// Value aggregation of all supersteps
	public Q QueryTotalAggregator;
	// Aggregates active workers for next superstep
	public Set<Integer> ActiveWorkers = new HashSet<>();
	// Active workers for this superstep
	//	public List<Integer> ActiveWorkersNow = new ArrayList<>();
	public final Set<Integer> workersWaitingFor;
	public boolean IsComputing = true;


	public MasterQuery(Q query, Collection<Integer> workersToWait, BaseQuery.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		super();
		this.queryValueFactory = queryFactory;
		BaseQuery = query;
		StartedSuperstepNo = -2;
		LastFinishedSuperstepNo = -2;
		StartTime = System.nanoTime();
		LastStepTime = StartTime;
		workersWaitingFor = new HashSet<>(workersToWait.size());
		beginStartNextSuperstep(workersToWait);
		resetValueAggregator(queryFactory);
		QueryTotalAggregator = queryFactory.createClone(BaseQuery);
	}

	public void beginStartNextSuperstep(Collection<Integer> workersToWait) {
		workersWaitingFor.addAll(workersToWait);
		StartedSuperstepNo++;
	}

	public void finishStartNextSuperstep() {
		resetValueAggregator(queryValueFactory);
		LastStepTime = System.nanoTime();
		logger.debug("Workers finished superstep " + BaseQuery.QueryId + ":" + (StartedSuperstepNo - 1) + " after "
				+ ((System.nanoTime() - LastStepTime) / 1000000) + "ms. Total " + ((System.nanoTime() - StartTime) / 1000000)
				+ "ms. Active: " + QueryStepAggregator.getActiveVertices());
		logger.trace("Next master superstep query " + BaseQuery.QueryId + ":" + StartedSuperstepNo);
	}

	private void resetValueAggregator(BaseQuery.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		QueryStepAggregator = queryFactory.createClone(BaseQuery);
		QueryStepAggregator.setVertexCount(0);
		QueryStepAggregator.setActiveVertices(0);
		//		ActiveWorkersNow.clear();
		//		ActiveWorkersNow.addAll(ActiveWorkersAggregator);
		ActiveWorkers.clear();
	}

	public void aggregateQuery(Q workerQueryMsg, int workerMachineId) {
		QueryStepAggregator.combine(workerQueryMsg);
		QueryTotalAggregator.combine(workerQueryMsg);
		if (workerQueryMsg.getActiveVertices() > 0) ActiveWorkers.add(workerMachineId);
	}

	public void workersFinished(Collection<Integer> workersToWait) {
		workersWaitingFor.addAll(workersToWait);
		IsComputing = false;
		if (!ActiveWorkers.isEmpty()) logger.warn("Finishing query with active workers: " + ActiveWorkers);
		if (QueryStepAggregator.getActiveVertices() != 0)
			logger.warn("Finishing query with active vertices: " + QueryStepAggregator.getActiveVertices());
	}
}
