package mthesis.concurrent_graph;

import mthesis.concurrent_graph.BaseQuery.BaseQueryGlobalValuesFactory;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.writable.BaseWritable;
import mthesis.concurrent_graph.writable.BaseWritable.BaseWritableFactory;

public abstract class JobConfiguration<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery> {

	private final VertexFactory<V, E, M, Q> vertexFactory;
	private final BaseWritableFactory<V> vertexValueFactory;
	private final BaseWritableFactory<E> edgeValueFactory;
	private final BaseWritableFactory<M> messageValueFactory;
	private final BaseQueryGlobalValuesFactory<Q> globalValuesFactory;

	//	private final ArrayList<M> messageValuePool;


	public JobConfiguration(VertexFactory<V, E, M, Q> vertexFactory, BaseWritableFactory<V> vertexValueFactory,
			BaseWritableFactory<E> edgeValueFactory, BaseWritableFactory<M> messageValueFactory,
			BaseQueryGlobalValuesFactory<Q> globalValuesFactory) {
		super();
		this.vertexFactory = vertexFactory;
		this.vertexValueFactory = vertexValueFactory;
		this.edgeValueFactory = edgeValueFactory;
		this.messageValueFactory = messageValueFactory;
		this.globalValuesFactory = globalValuesFactory;
		//				if (Configuration.VERTEX_MESSAGE_POOLING)
		//			messageValuePool = new ArrayList<>(Configuration.VERTEX_MESSAGE_POOL_SIZE);
		//		else
		//			messageValuePool = null;
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



	//	//	int newM = 0;
	//	//	int newO = 0;
	//	public M getPooledMessageValue() {
	//		if (Configuration.VERTEX_MESSAGE_POOLING) {
	//			synchronized (messageValuePool) {
	//				if (!messageValuePool.isEmpty())
	//					return messageValuePool.remove(messageValuePool.size() - 1);
	//				else
	//					return messageValueFactory.createDefault();
	//			}
	//
	//			//		if (message == null) {
	//			//			newM++;
	//			//			System.out.println("new " + (newM));
	//			//		}
	//			//		else {
	//			//			newO++;
	//			//			if ((newO % 1000) == 0)
	//			//				System.out.println("old " + (newO));
	//			//		}
	//		}
	//		else {
	//			return messageValueFactory.createDefault();
	//		}
	//	}
	//
	//	public void freePooledMessageValue(M message) {
	//		if (Configuration.VERTEX_MESSAGE_POOLING) {
	//			synchronized (messageValuePool) {
	//				if (messageValuePool.size() < Configuration.VERTEX_MESSAGE_POOL_SIZE)
	//					messageValuePool.add(message);
	//			}
	//			//		if ((messageValuePool.size() % 1000) == 0)
	//			//			System.out.println("pool " + messageValuePool.size() + " / " + newM);
	//		}
	//	}
}
