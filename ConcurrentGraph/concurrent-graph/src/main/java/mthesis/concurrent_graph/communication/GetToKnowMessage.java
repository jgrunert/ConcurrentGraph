package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

public class GetToKnowMessage implements ChannelMessage {

	public final int srcMachine;
	public final int queryId;
	public final Collection<Integer> vertices;

	public GetToKnowMessage(int srcMachine, int queryId, Collection<Integer> vertices) {
		super();
		this.srcMachine = srcMachine;
		this.vertices = vertices;
		this.queryId = queryId;
	}

	public GetToKnowMessage(ByteBuffer buffer) {
		super();
		this.srcMachine = buffer.getInt();
		this.queryId = buffer.getInt();
		int numVertices = buffer.getInt();
		vertices = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			vertices.add(buffer.getInt());
		}
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.putInt(srcMachine);
		buffer.putInt(queryId);
		buffer.putInt(vertices.size());
		for (final Integer vert : vertices) {
			buffer.putInt(vert);
		}
	}

	@Override
	public void free(boolean freeMembers) {
	}

	@Override
	public boolean hasContent() {
		return true;
	}

	@Override
	public boolean flushAfter() {
		return false;
	}

	@Override
	public byte getTypeCode() {
		return 2;
	}
}