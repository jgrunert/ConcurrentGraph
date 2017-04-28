package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.communication.Messages.WorkerQueryExecutionMode;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	protected final Logger logger = LoggerFactory.getLogger(WorkerQuery.class);

	public final int QueryId;
	/** Global query, values from master, aggregated from local queries from last frame */
	public Q Query;
	/** Local query values, are sent to the master after superstep and then aggregated */
	public Q QueryLocal;

	// Supersteps: Start with -1, the prepare step
	private static final int firstSuperstep = -1;
	// Last superstep with finished worker barrier sync
	private volatile int barrierSyncedSuperstepNo = firstSuperstep - 1;
	// Number of last superstep compute, incremented when superstep compute is finished.
	private volatile int finishedComputeSuperstepNo = firstSuperstep - 1;
	// Last superstep locally finished: compute, and barrier sync
	private volatile int localFinishedSuperstepNo = firstSuperstep - 1;
	// Number of next superstep to compute, incremented when master starts next superstep.
	private volatile int masterStartedSuperstepNo = firstSuperstep;

	/** Workers to wait for barrier sync */
	public final Set<Integer> BarrierSyncWaitSet = new HashSet<>();
	/** Workers received barrier sync from before waiting for it */
	public final Set<Integer> BarrierSyncPostponedSet = new HashSet<>();

	// Active vertices for next superstep
	public Int2ObjectMap<AbstractVertex<V, E, M, Q>> ActiveVerticesNext = new Int2ObjectOpenHashMap<>();
	// Active vertices this superstep
	public Int2ObjectMap<AbstractVertex<V, E, M, Q>> ActiveVerticesThis = new Int2ObjectOpenHashMap<>();

	// All vertices ever active for this query on this worker
	public IntSet VerticesEverActive = new IntOpenHashSet();

	public boolean localExecution;
	private WorkerQueryExecutionMode executionMode;


	public WorkerQuery(Q globalQueryValues, BaseQueryGlobalValuesFactory<Q> globalValueFactory,
			Collection<Integer> vertexIds) {
		QueryId = globalQueryValues.QueryId;
		Query = globalQueryValues;
		QueryLocal = globalValueFactory.createClone(globalQueryValues);
	}


	/**
	 * Finished compute superstep
	 */
	public void onFinishedSuperstepCompute(int superstepFinished) {
		// Compute can be finished before or after barrier sync
		assert superstepFinished == finishedComputeSuperstepNo + 1;
		assert superstepFinished == masterStartedSuperstepNo;
		finishedComputeSuperstepNo = superstepFinished;
	}

	/**
	 * Finished barrier sync with other workers
	 */
	public void onFinishedWorkerSuperstepBarrierSync(int superstepSynced) {
		assert superstepSynced == barrierSyncedSuperstepNo + 1;
		barrierSyncedSuperstepNo = superstepSynced;
	}

	/**
	 * Finished superstep locally and notifying master: both barrier sync and compute finished
	 */
	public void onLocalFinishSuperstep(int superstepFinished) {
		assert superstepFinished == localFinishedSuperstepNo + 1;
		assert superstepFinished == finishedComputeSuperstepNo;
		assert barrierSyncedSuperstepNo == superstepFinished || barrierSyncedSuperstepNo == superstepFinished + 1;
		localFinishedSuperstepNo = superstepFinished;
	}

	/**
	 * Master sent message to start next superstep
	 */
	public void onMasterNextSuperstep(int nextSuperstep, WorkerQueryExecutionMode queryExecutionMode) {
		assert nextSuperstep == masterStartedSuperstepNo + 1;
		assert nextSuperstep == localFinishedSuperstepNo + 1;
		//		assert finishedComputeSuperstepNo2 == lastFinishedSuperstepNo + 1;
		//		assert workerBarrierSyncSuperstepNo == lastFinishedSuperstepNo + 1
		//				|| workerBarrierSyncSuperstepNo == lastFinishedSuperstepNo + 2;
		//		assert workerBarrierSyncSuperstepNo == finishedComputeSuperstepNo2;
		//		assert superstep == lastFinishedSuperstepNo + 1;
		//		assert superstep == nextComputeSuperstepNo;
		this.executionMode = queryExecutionMode;
		if (executionMode == WorkerQueryExecutionMode.LocalOnThis) localExecution = true;
		masterStartedSuperstepNo = nextSuperstep;
	}


	/**
	 * Number of last finished superstep, incremented when superstep is finished.
	 */
	public int getLastFinishedComputeSuperstep() {
		return finishedComputeSuperstepNo;
	}

	/**
	 * Returns last superstep with all worker barrier syncs finished
	 */
	public int getBarrierSyncedSuperstep() {
		return barrierSyncedSuperstepNo;
	}

	/**
	 * Returns last locally finished superstep: finished compute, master and worker barrier sync
	 */
	public int getLocalFinishedSuperstepNo() {
		return localFinishedSuperstepNo;
	}

	/**
	 * Number of current superstep to compute, incremented when master starts next superstep.
	 */
	public int getMasterStartedSuperstep() {
		return masterStartedSuperstepNo;
	}


	/**
	 * Returns if next superstep is ready for notify master, if compute worker barrier sync finished
	 */
	public boolean isNextSuperstepLocallyReady() {
		return finishedComputeSuperstepNo == localFinishedSuperstepNo + 1
				&& (barrierSyncedSuperstepNo == localFinishedSuperstepNo + 1
				|| barrierSyncedSuperstepNo == localFinishedSuperstepNo + 2);
	}


	public String getSuperstepNosLog() {
		return finishedComputeSuperstepNo + " " + barrierSyncedSuperstepNo + " " + localFinishedSuperstepNo + " "
				+ masterStartedSuperstepNo;
	}


	public WorkerQueryExecutionMode getExecutionMode() {
		return executionMode;
	}
}
