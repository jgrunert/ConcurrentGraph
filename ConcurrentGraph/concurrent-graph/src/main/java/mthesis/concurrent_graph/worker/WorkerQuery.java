package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public final int QueryId;
	// Global query, values from master, aggregated from local queries from last frame
	public Q Query;
	// Local query values, are sent to the master after superstep and then aggregated
	public Q QueryLocal;
	//private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Last calculated superstep number
	private volatile int calculatedSuperstepNo = -1;
	// Last superstep when finished barrier sync
	private volatile int barrierFinishedSuperstepNo = -1;
	// Superstep to start, confirmed by master and ready. Is >= CalculatedSuperstepNo
	private volatile int startedSuperstepNo = 0;

	public Set<Integer> ChannelBarrierWaitSet = new HashSet<>();
	// Channgel barriers received for next superstep
	public Set<Integer> ChannelBarrierPremature = new HashSet<>();
	//public Int2ObjectMap<List<M>> InVertexMessages = new Int2ObjectOpenHashMap<>();
	public boolean Master = false;

	// Active vertices for next superstep
	public ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> ActiveVerticesNext = new ConcurrentHashMap<>();
	// Active vertices this superstep
	public ConcurrentMap<Integer, AbstractVertex<V, E, M, Q>> ActiveVerticesThis = new ConcurrentHashMap<>();
	//	public IntSet ActiveVertices = new IntOpenHashSet();


	public WorkerQuery(Q globalQueryValues, BaseQueryGlobalValuesFactory<Q> globalValueFactory,
			Collection<Integer> vertexIds) {
		//this.globalValueFactory = globalValueFactory;
		QueryId = globalQueryValues.QueryId;
		Query = globalQueryValues;
		QueryLocal = globalValueFactory.createClone(globalQueryValues);
		//		for (Integer vertexId : vertexIds) {
		//			InVertexMessages.put(vertexId, new ArrayList<>());
		//		}
	}

	public void calculatedSuperstep() {
		calculatedSuperstepNo++;
	}

	public void finishedBarrierSync() {
		barrierFinishedSuperstepNo++;
	}

	public void startNextSuperstep() {
		startedSuperstepNo++;
	}


	public int getCalculatedSuperstepNo() {
		return calculatedSuperstepNo;
	}

	public int getBarrierFinishedSuperstepNo() {
		return barrierFinishedSuperstepNo;
	}

	public int getMasterSuperstepNo() {
		return startedSuperstepNo;
	}
}
