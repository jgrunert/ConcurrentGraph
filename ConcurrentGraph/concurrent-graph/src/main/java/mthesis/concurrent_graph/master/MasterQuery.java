package mthesis.concurrent_graph.master;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class MasterQuery<Q extends BaseQueryGlobalValues> {

	private static final Logger logger = LoggerFactory.getLogger(MasterQuery.class);

	public final Q Query;
	public final long StartTime;

	public int SuperstepNo;
	public Q QueryAggregator;
	public int ActiveWorkers;
	public final Set<Integer> workersWaitingFor;
	public boolean IsComputing = true;


	public MasterQuery(Q query, Collection<Integer> workersToWait, BaseQueryGlobalValues.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		super();
		Query = query;
		SuperstepNo = -1;
		StartTime = System.currentTimeMillis();
		workersWaitingFor = new HashSet<>(workersToWait.size());
		nextSuperstep(workersToWait, queryFactory);
	}

	public void nextSuperstep(Collection<Integer> workersToWait, BaseQueryGlobalValues.BaseQueryGlobalValuesFactory<Q> queryFactory) {
		workersWaitingFor.addAll(workersToWait);
		QueryAggregator = queryFactory.createClone(Query);
		QueryAggregator.setVertexCount(0);
		QueryAggregator.setActiveVertices(0);
		ActiveWorkers = 0;
		SuperstepNo++;
	}

	public void aggregateQuery(Q workerQueryMsg) {
		QueryAggregator.add(workerQueryMsg);
		if (workerQueryMsg.getActiveVertices() > 0) ActiveWorkers++;
	}

	public void workersFinished(Collection<Integer> workersToWait) {
		workersWaitingFor.addAll(workersToWait);
		IsComputing = false;
		if (ActiveWorkers != 0) logger.warn("Finishing query with active workers: " + ActiveWorkers);
		if (QueryAggregator.getActiveVertices() != 0)
			logger.warn("Finishing query with active vertices: " + QueryAggregator.getActiveVertices());
	}
}
