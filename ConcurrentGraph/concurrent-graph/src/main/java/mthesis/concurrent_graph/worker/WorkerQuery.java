package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	public final int QueryId;
	/** Global query, values from master, aggregated from local queries from last frame */
	public Q Query;
	/** Local query values, are sent to the master after superstep and then aggregated */
	public Q QueryLocal;
	//private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Supersteps: Start with -1, the prepare step
	// Last calculated superstep number
	private volatile int calculatedSuperstepNo = -2;
	// Last superstep when finished barrier sync
	private volatile int barrierFinishedSuperstepNo = -2;
	// Superstep prepared, confirmed by master. Is >= CalculatedSuperstepNo
	private volatile int preparedSuperstepNo = -1;
	// Superstep to start, confirmed by master and ready. Is >= CalculatedSuperstepNo
	private volatile int startedSuperstepNo = -1;

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

	// Set of machines to wait for to move vertices here, for the current superstep.
	public Set<Integer> VertexMovesWaitingFor = new HashSet<>();
	// Machine received vertices from, for the current superstep.
	public Set<Integer> VertexMovesReceived = new HashSet<>();


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
		assert (calculatedSuperstepNo + 1) == startedSuperstepNo;
		calculatedSuperstepNo++;
	}

	public void finishedBarrierSync() {
		assert (barrierFinishedSuperstepNo + 1) == startedSuperstepNo;
		barrierFinishedSuperstepNo++;
	}

	public void preparedSuperstep() {
		assert preparedSuperstepNo == calculatedSuperstepNo && preparedSuperstepNo == barrierFinishedSuperstepNo;
		preparedSuperstepNo++;
	}

	public void startNextSuperstep() {
		assert (startedSuperstepNo + 1) == preparedSuperstepNo;
		startedSuperstepNo++;
	}


	public int getCalculatedSuperstepNo() {
		return calculatedSuperstepNo;
	}

	public int getBarrierFinishedSuperstepNo() {
		return barrierFinishedSuperstepNo;
	}

	public int getPreparedSuperstepNo() {
		return preparedSuperstepNo;
	}

	public int getStartedSuperstepNo() {
		return startedSuperstepNo;
	}
}
