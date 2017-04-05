package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;

public class MoveVerticesMessage<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery>
		implements ChannelMessage {

	public final int srcMachine;
	public final int queryId;
	public final Collection<AbstractVertex<V, E, M, Q>> vertices;
	public final boolean lastSegment;

	public MoveVerticesMessage(int srcMachine, int queryId, Collection<AbstractVertex<V, E, M, Q>> vertices,
			boolean lastSegment) {
		super();
		this.srcMachine = srcMachine;
		this.queryId = queryId;
		this.lastSegment = lastSegment;
		this.vertices = vertices;
	}

	public MoveVerticesMessage(ByteBuffer buffer, VertexWorkerInterface<V, E, M, Q> worker,
			JobConfiguration<V, E, M, Q> jobConfig, VertexFactory<V, E, M, Q> vertexFactory) {
		super();
		this.srcMachine = buffer.getInt();
		this.queryId = buffer.getInt();
		this.lastSegment = (buffer.get() == 0);
		int numVertices = buffer.getInt();
		vertices = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			vertices.add(vertexFactory.newInstance(buffer, worker, jobConfig));
		}
	}

	@Override
	public void free(boolean freeMembers) {
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.putInt(srcMachine);
		buffer.putInt(queryId);
		buffer.put(lastSegment ? (byte) 0 : (byte) 1);
		buffer.putInt(vertices.size());
		for (final AbstractVertex<V, E, M, Q> vert : vertices) {
			vert.writeToBuffer(buffer);
		}
	}

	@Override
	public boolean hasContent() {
		return true;
	}

	@Override
	public boolean flushAfter() {
		return lastSegment;
	}

	@Override
	public byte getTypeCode() {
		return 3;
	}
}