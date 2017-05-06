package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;

public class MoveVerticesMessage<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery>
		implements ChannelMessage {

	public final int srcMachine;
	// Queries of this chunk to send
	public final Set<Integer> chunkQueries;
	// Queries of vertices that were really send (less than chunkQueries if quries inactive now)
	public final Set<Integer> vertexQueries;
	public final List<AbstractVertex<V, E, M, Q>> vertices;
	public final boolean lastSegment;

	public MoveVerticesMessage(int srcMachine, Set<Integer> chunkQueries, Set<Integer> vertexQueries,
			List<AbstractVertex<V, E, M, Q>> vertices,
			boolean lastSegment) {
		super();
		this.srcMachine = srcMachine;
		this.lastSegment = lastSegment;
		this.chunkQueries = chunkQueries;
		this.vertexQueries = vertexQueries;
		this.vertices = vertices;
	}

	public MoveVerticesMessage(ByteBuffer buffer, VertexWorkerInterface<V, E, M, Q> worker,
			JobConfiguration<V, E, M, Q> jobConfig, VertexFactory<V, E, M, Q> vertexFactory) {
		super();
		this.srcMachine = buffer.getInt();
		this.lastSegment = (buffer.get() == 0);

		int numChunkQueries = buffer.getInt();
		chunkQueries = new HashSet<>(numChunkQueries);
		for (int j = 0; j < numChunkQueries; j++) {
			chunkQueries.add(buffer.getInt());
		}

		int numVertexQueries = buffer.getInt();
		vertexQueries = new HashSet<>(numVertexQueries);
		for (int j = 0; j < numVertexQueries; j++) {
			vertexQueries.add(buffer.getInt());
		}

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
		buffer.put(lastSegment ? (byte) 0 : (byte) 1);

		buffer.putInt(chunkQueries.size());
		for (final Integer q : chunkQueries) {
			buffer.putInt(q);
		}

		buffer.putInt(vertexQueries.size());
		for (final Integer q : vertexQueries) {
			buffer.putInt(q);
		}


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