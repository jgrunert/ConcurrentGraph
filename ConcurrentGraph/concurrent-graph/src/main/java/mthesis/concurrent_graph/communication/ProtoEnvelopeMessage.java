package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.communication.Messages.MessageEnvelope;

public class ProtoEnvelopeMessage implements ChannelMessage {

	public final MessageEnvelope message;
	private final boolean flushAfter;

	public ProtoEnvelopeMessage(MessageEnvelope message, boolean flushAfter) {
		this.message = message;
		this.flushAfter = flushAfter;
	}

	@Override
	public byte getTypeCode() {
		return 1;
	}

	@Override
	public void writeMessageToBuffer(ByteBuffer buffer) {
		buffer.put(message.toByteArray());
	}


	@Override
	public boolean hasContent() {
		return true;
	}

	@Override
	public boolean flushAfter() {
		return flushAfter;
	}
}