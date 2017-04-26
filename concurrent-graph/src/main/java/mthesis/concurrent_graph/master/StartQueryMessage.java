package mthesis.concurrent_graph.master;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQuery;
import mthesis.concurrent_graph.communication.ChannelMessage;

public class StartQueryMessage<Q extends BaseQuery> implements ChannelMessage {

	public static final int ChannelMessageTypeCode = -2;

	public final Q Query;


	public StartQueryMessage(Q query) {
		super();
		this.Query = query;
	}


	@Override
	public boolean hasContent() {
		return false;
	}

	@Override
	public boolean flushAfter() {
		return true;
	}

	@Override
	public byte getTypeCode() {
		return ChannelMessageTypeCode;
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void free(boolean freeMembers) {
		throw new RuntimeException("Not implemented");
	}
}
