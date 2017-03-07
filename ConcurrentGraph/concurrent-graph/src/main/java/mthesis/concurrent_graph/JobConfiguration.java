package mthesis.concurrent_graph;

import java.util.LinkedList;

import mthesis.concurrent_graph.BaseQueryGlobalValues.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public abstract class JobConfiguration<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues> {

	private final VertexFactory<V, E, M, Q> vertexFactory;
	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseWritableFactory<E> edgeValueFactory;
	private final BaseWritableFactory<M> messageValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValuesFactory;

	private LinkedList<M> messageValuePool = new LinkedList<>();


	public JobConfiguration(VertexFactory<V, E, M, Q> vertexFactory, BaseWritableFactory<V> vertexValueFactory,
			BaseWritableFactory<E> edgeValueFactory, BaseWritableFactory<M> messageValueFactory,
			BaseQueryGlobalValuesFactory<Q> globalValuesFactory) {
		super();
		this.vertexFactory = vertexFactory;
		this.vertexValueFactory = vertexValueFactory;
		this.edgeValueFactory = edgeValueFactory;
		this.messageValueFactory = messageValueFactory;
		this.globalValuesFactory = globalValuesFactory;
	}


	public VertexFactory<V, E, M, Q> getVertexFactory() {
		return vertexFactory;
	}

	public BaseWritableFactory<V> getVertexValueFactory() {
		return vertexValueFactory;
	}

	public BaseWritableFactory<E> getEdgeValueFactory() {
		return edgeValueFactory;
	}

	public BaseWritableFactory<M> getMessageValueFactory() {
		return messageValueFactory;
	}

	public BaseQueryGlobalValuesFactory<Q> getGlobalValuesFactory() {
		return globalValuesFactory;
	}


	public M getPooledMessageValue() {
		M message;
		synchronized (messageValuePool) {
			message = messageValuePool.poll();
		}

		//		if (message == null)
		//			System.out.println("new");
		//		else
		//			System.out.println("old");

		if (message == null)
			message = messageValueFactory.createDefault();
		return message;
	}

	public void freePooledMessageValue(M message) {
		synchronized (messageValuePool) {
			messageValuePool.add(message);
		}
	}
}
