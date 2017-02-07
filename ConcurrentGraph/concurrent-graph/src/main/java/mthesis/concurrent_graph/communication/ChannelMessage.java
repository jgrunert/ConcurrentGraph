package mthesis.concurrent_graph.communication;

import java.nio.ByteBuffer;

/**
 * Message for transfer in channels between machines.
 *
 * @author Jonas Grunert
 *
 */
public interface ChannelMessage {

	boolean hasContent();

	boolean flushAfter();

	byte getTypeCode();

	void writeMessageToBuffer(ByteBuffer buffer);
}