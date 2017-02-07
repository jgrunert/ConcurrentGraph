package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;

interface SendableMessage {

	boolean hasContent();

	boolean flushAfter();

	byte getTypeCode();

	void writeMessageToBuffer(ByteBuffer buffer);
}