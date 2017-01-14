package mthesis.concurrent_graph.worker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.writable.BaseWritable;

public class WorkerQuery<M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	public Q Query;
	public Q QueryLocal;
	//private final BaseQueryGlobalValuesFactory<Q> globalValueFactory;

	// Last calculated superstep number
	private int calculatedSuperstepNo = -1;
	// Superstep to start confirmed by master, >= CalculatedSuperstepNo
	private int masterSuperstepNo = 0;

	public Set<Integer> ChannelBarrierWaitSet = new HashSet<>();
	// Channgel barriers received for next superstep
	public Set<Integer> ChannelBarrierPremature = new HashSet<>();
	//public Int2ObjectMap<List<M>> InVertexMessages = new Int2ObjectOpenHashMap<>();
	public boolean Master = false;

	//	public IntSet ActiveVertices = new IntOpenHashSet();


	public WorkerQuery(Q globalQueryValues, BaseQueryGlobalValuesFactory<Q> globalValueFactory,
			Collection<Integer> vertexIds) {
		//this.globalValueFactory = globalValueFactory;
		Query = globalQueryValues;
		QueryLocal = globalValueFactory.createClone(globalQueryValues);
		//		for (Integer vertexId : vertexIds) {
		//			InVertexMessages.put(vertexId, new ArrayList<>());
		//		}
	}

	public void calculatedSuperstep() {
		calculatedSuperstepNo++;
	}

	public void masterConfirmedNextSuperstep() {
		masterSuperstepNo++;
	}


	public int getCalculatedSuperstepNo() {
		return calculatedSuperstepNo;
	}

	public int getMasterSuperstepNo() {
		return masterSuperstepNo;
	}
}
