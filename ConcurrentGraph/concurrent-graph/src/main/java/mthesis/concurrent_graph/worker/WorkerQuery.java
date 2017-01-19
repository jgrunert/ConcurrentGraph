package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public final int QueryId;
	// Global query, values from master, aggregated from local queries from last frame
	public Q Query;
	// Local query values, are sent to the master after superstep and then aggregated
	public Q QueryLocal;
	//private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Last calculated superstep number
	private int calculatedSuperstepNo = -1;
	// Last superstep when finished barrier sync
	private int barrierFinishedSuperstepNo = -1;
	// Superstep to start confirmed by master, >= CalculatedSuperstepNo
	private int masterSuperstepNo = 0;

	public Set<Integer> ChannelBarrierWaitSet = new HashSet<>();
	// Channgel barriers received for next superstep
	public Set<Integer> ChannelBarrierPremature = new HashSet<>();
	//public Int2ObjectMap<List<M>> InVertexMessages = new Int2ObjectOpenHashMap<>();
	public boolean Master = false;

	public IntSet ActiveVertices = new IntOpenHashSet();

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

	public void masterConfirmedNextSuperstep() {
		masterSuperstepNo++;
	}


	public int getCalculatedSuperstepNo() {
		return calculatedSuperstepNo;
	}

	public int getBarrierFinishedSuperstepNo() {
		return barrierFinishedSuperstepNo;
	}

	public int getMasterSuperstepNo() {
		return masterSuperstepNo;
	}
}