package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

public class UpdateRegisteredVerticesMessage implements ChannelMessage {

	public final int srcMachine;
	public final int queryId;
	public final int movedTo;
	public final Collection<Integer> vertices;

	public UpdateRegisteredVerticesMessage(int srcMachine, int queryId, int movedTo, Collection<Integer> vertices) {
		super();
		this.srcMachine = srcMachine;
		this.vertices = vertices;
		this.movedTo = movedTo;
		this.queryId = queryId;
	}

	public UpdateRegisteredVerticesMessage(ByteBuffer buffer) {
		super();
		this.srcMachine = buffer.getInt();
		this.queryId = buffer.getInt();
		movedTo = buffer.getInt();
		int numVertices = buffer.getInt();
		vertices = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			vertices.add(buffer.getInt());
		}
	}

	@Override
	public void free() {
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.putInt(srcMachine);
		buffer.putInt(queryId);
		buffer.putInt(movedTo);
		buffer.putInt(vertices.size());
		for (final Integer vert : vertices) {
			buffer.putInt(vert);
		}
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
		return 4;
	}
}