package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.writable.BaseWritable;

public class MoveVerticesMessage<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery>
implements ChannelMessage {

	public final int srcMachine;
	// Queries of this chunk to send
	public final Set<Integer> chunkQueries;
	public final List<Pair<AbstractVertex<V, E, M, Q>, Integer[]>> vertices;
	public final boolean lastSegment;

	public MoveVerticesMessage(int srcMachine, Set<Integer> chunkQueries, List<Pair<AbstractVertex<V, E, M, Q>, Integer[]>> vertices, boolean lastSegment) {
		super();
		this.srcMachine = srcMachine;
		this.lastSegment = lastSegment;
		this.chunkQueries = chunkQueries;
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

		int numVertices = buffer.getInt();
		vertices = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			AbstractVertex<V, E, M, Q> vert = vertexFactory.newInstance(buffer, worker, jobConfig);

			int numVertQueries = buffer.getInt();
			Integer[] vertQueries = new Integer[numVertQueries];
			for (int j = 0; j < numVertQueries; j++) {
				vertQueries[j] = buffer.getInt();
			}

			vertices.add(new Pair<>(vert, vertQueries));
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

		buffer.putInt(vertices.size());
		for (final Pair<AbstractVertex<V, E, M, Q>, Integer[]> vert : vertices) {
			vert.first.writeToBuffer(buffer);

			Integer[] vertQueries = vert.second;
			buffer.putInt(vertQueries.length);
			for (final Integer q : vertQueries) {
				buffer.putInt(q);
			}
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