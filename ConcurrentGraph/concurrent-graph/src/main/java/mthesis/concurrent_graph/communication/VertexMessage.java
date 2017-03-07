package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.BaseQueryGlobalValues;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;

public class VertexMessage<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQueryGlobalValues>
		implements ChannelMessage {

	public int superstepNo;
	public int srcMachine;
	public boolean broadcastFlag;
	public int queryId;
	public List<Pair<Integer, M>> vertexMessages;
	private int referenceCounter;

	private final VertexMessagePool<V, E, M, Q> messagePool;

	public VertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages, VertexMessagePool<V, E, M, Q> messagePool,
			int referenceCounter) {
		this.messagePool = messagePool;
		setup(superstepNo, srcMachine, broadcastFlag, queryId, vertexMessages, referenceCounter);
	}

	public void setup(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId,
			List<Pair<Integer, M>> vertexMessages, int referenceCounter) {
		this.srcMachine = srcMachine;
		this.superstepNo = superstepNo;
		this.broadcastFlag = broadcastFlag;
		this.queryId = queryId;
		this.vertexMessages = vertexMessages;
		this.referenceCounter = referenceCounter;
	}

	public VertexMessage(ByteBuffer buffer, BaseWritable.BaseWritableFactory<M> vertexMessageFactory,
			VertexMessagePool<V, E, M, Q> messagePool, int referenceCounter) {
		super();
		this.messagePool = messagePool;
		setup(buffer, vertexMessageFactory, referenceCounter);
	}

	public void setup(ByteBuffer buffer, BaseWritable.BaseWritableFactory<M> vertexMessageFactory, int referenceCounter) {
		this.superstepNo = buffer.getInt();
		this.srcMachine = buffer.getInt();
		this.broadcastFlag = (buffer.get() == 0);
		this.queryId = buffer.getInt();
		int numVertices = buffer.getInt();
		vertexMessages = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			vertexMessages.add(new Pair<Integer, M>(buffer.getInt(), vertexMessageFactory.createFromBytes(buffer)));
		}
		this.referenceCounter = referenceCounter;
	}

	@Override
	public void free() {
		referenceCounter--;
		if (referenceCounter <= 0) {
			if (messagePool != null) {
				messagePool.freeVertexMessage(this);
			}
		}
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.putInt(superstepNo);
		buffer.putInt(srcMachine);
		buffer.put(broadcastFlag ? (byte) 0 : (byte) 1);
		buffer.putInt(queryId);
		buffer.putInt(vertexMessages.size());
		for (final Pair<Integer, M> msg : vertexMessages) {
			buffer.putInt(msg.first);
			msg.second.writeToBuffer(buffer);
		}
	}

	@Override
	public byte getTypeCode() {
		return 0;
	}

	@Override
	public boolean hasContent() {
		return true;
	}

	@Override
	public boolean flushAfter() {
		return false;
	}
}