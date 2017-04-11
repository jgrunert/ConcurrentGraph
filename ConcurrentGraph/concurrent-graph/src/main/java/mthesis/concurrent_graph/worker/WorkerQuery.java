package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	protected final Logger logger = LoggerFactory.getLogger(WorkerQuery.class);

	public final int QueryId;
	private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;
	/** Global query, values from master, aggregated from local queries from last frame */
	public Q Query;
	/** Local query values, are sent to the master after superstep and then aggregated */
	public Q QueryLocal;

	// Supersteps: Start with -1, the prepare step
	private static final int firstSuperstep = -1;
	// Last superstep with finished worker barrier sync
	private volatile int workerBarrierSyncSuperstepNo = firstSuperstep - 1;
	// Last superstep completely finished: compute, worker and master sync
	private volatile int finishedSuperstepNo = firstSuperstep - 1;
	// Number of next superstep to compute, incremented when superstep compute is finished.
	private volatile int nextComputeSuperstepNo = firstSuperstep;

	public final Set<Integer> BarrierSyncWaitSet = new HashSet<>();

	// Active vertices for next superstep
	public ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> ActiveVerticesNext = new ConcurrentHashMap<>();
	// Active vertices this superstep
	public ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> ActiveVerticesThis = new ConcurrentHashMap<>();


	public WorkerQuery(Q globalQueryValues, BaseQueryGlobalValuesFactory<Q> globalValueFactory,
			Collection<Integer> vertexIds) {
		this.globalValueFactory = globalValueFactory;
		QueryId = globalQueryValues.QueryId;
		Query = globalQueryValues;
		QueryLocal = globalValueFactory.createClone(globalQueryValues);
	}


	public void finishedCompute() {
		// Compute can be finished before or after barrier sync
		assert nextComputeSuperstepNo == workerBarrierSyncSuperstepNo || nextComputeSuperstepNo == workerBarrierSyncSuperstepNo + 1;
		assert nextComputeSuperstepNo == finishedSuperstepNo + 1;
		nextComputeSuperstepNo++;
	}

	public void finishedWorkerBarrierSync() {
		assert workerBarrierSyncSuperstepNo == finishedSuperstepNo;
		workerBarrierSyncSuperstepNo++;
	}

	/**
	 * Finished a superstep completely: compute, worker and master sync
	 */
	public void finishedSuperstep() {
		assert nextComputeSuperstepNo == finishedSuperstepNo + 2;
		assert workerBarrierSyncSuperstepNo == finishedSuperstepNo + 1;
		finishedSuperstepNo++;
	}


	/**
	 * Number of next superstep to compute, incremented when superstep is finished.
	 */
	public int getNextComputeSuperstep() {
		return nextComputeSuperstepNo;
	}

	/**
	 * Returns last superstep with all worker barrier syncs finished
	 */
	public int getWorkerBarrierFinishedSuperstep() {
		return workerBarrierSyncSuperstepNo;
	}

	/**
	 * Returns last completely finished superstep: finished compute, master and worker barrier sync
	 */
	public int getFinishedSuperstepNo() {
		return finishedSuperstepNo;
	}

	/**
	 * Returns if next superstep is ready for notify master, if compute worker barrier sync finished
	 */
	public boolean isSuperstepReadyForMaster() {
		return nextComputeSuperstepNo == finishedSuperstepNo + 2
				&& workerBarrierSyncSuperstepNo == finishedSuperstepNo + 1;
	}
}
