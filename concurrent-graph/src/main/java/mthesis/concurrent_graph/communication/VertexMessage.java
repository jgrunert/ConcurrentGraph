package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;

public class VertexMessage<V extends BaseWritable, E extends BaseWritable, M extends BaseWritable, Q extends BaseQuery>
implements ChannelMessage {

	public int superstepNo;
	public int srcMachine;
	public boolean broadcastFlag;
	public int queryId;
	/** Indicates if from a query running in localmode */
	public boolean fromLocalMode;
	public List<Pair<Integer, M>> vertexMessages;

	public VertexMessage(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId, boolean fromLocalMode,
			List<Pair<Integer, M>> vertexMessages) {
		setup(superstepNo, srcMachine, broadcastFlag, queryId, fromLocalMode, vertexMessages);
	}

	public void setup(int superstepNo, int srcMachine, boolean broadcastFlag, int queryId, boolean fromLocalMode,
			List<Pair<Integer, M>> vertexMessages) {
		this.srcMachine = srcMachine;
		this.superstepNo = superstepNo;
		this.broadcastFlag = broadcastFlag;
		this.queryId = queryId;
		this.fromLocalMode = fromLocalMode;
		this.vertexMessages = vertexMessages;
	}

	public VertexMessage(ByteBuffer buffer, JobConfiguration<V, E, M, Q> jobConfig) {
		super();
		setup(buffer, jobConfig);
	}

	public void setup(ByteBuffer buffer, JobConfiguration<V, E, M, Q> jobConfig) {
		this.superstepNo = buffer.getInt();
		this.srcMachine = buffer.getInt();
		this.broadcastFlag = (buffer.get() == 0);
		this.queryId = buffer.getInt();
		this.fromLocalMode = (buffer.get() == 0);
		int numVertices = buffer.getInt();
		vertexMessages = new ArrayList<>(numVertices);
		for (int i = 0; i < numVertices; i++) {
			int msgId = buffer.getInt();
			M msg = jobConfig.getMessageValueFactory().createFromBytes(buffer);
			vertexMessages.add(new Pair<Integer, M>(msgId, msg));
		}
	}

	/**
	 * Puts this message instance into the object pool
	 * @param freeMembers If true, members of this instance will be freed too (eg. vertex messages)
	 */
	@Override
	public void free(boolean freeMembers) {
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.putInt(superstepNo);
		buffer.putInt(srcMachine);
		buffer.put(broadcastFlag ? (byte) 0 : (byte) 1);
		buffer.putInt(queryId);
		buffer.put(fromLocalMode ? (byte) 0 : (byte) 1);
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

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "_" + srcMachine + "_" + queryId + ":" + superstepNo + "_"
				+ vertexMessages;
	}
}