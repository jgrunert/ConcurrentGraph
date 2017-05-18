package mthesis.concurrent_graph.master.vertexmove;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryChunkMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryChunkMessage;


/**
 * Represents a decision about vertex moving.
 *
 * @author Jonas Grunert
 *
 */
public class VertexMoveDecision {

	public final int moveMessages;
	public final Map<Integer, List<SendQueryChunkMessage>> WorkerVertSendMsgs;
	public final Map<Integer, List<ReceiveQueryChunkMessage>> WorkerVertRecvMsgs;


	public VertexMoveDecision(Map<Integer, List<SendQueryChunkMessage>> workerVertSendMsgs,
			Map<Integer, List<ReceiveQueryChunkMessage>> workerVertRecvMsgs) {
		super();
		WorkerVertSendMsgs = workerVertSendMsgs;
		WorkerVertRecvMsgs = workerVertRecvMsgs;
		int mvs = 0;
		for (List<SendQueryChunkMessage> msgs : WorkerVertSendMsgs.values()) {
			mvs += msgs.size();
		}
		moveMessages = mvs;
	}

	public void printDecission(PrintStream stream) {
		for (Entry<Integer, List<SendQueryChunkMessage>> sender : WorkerVertSendMsgs.entrySet()) {
			stream.println(sender.getKey());
			for (SendQueryChunkMessage sendMsg : sender.getValue()) {
				stream.println("\t" + sendMsg.getIncludeQueriesList() + " " + sendMsg.getTolreateQueriesList() + " " + sender.getKey()
						+ "->" + sendMsg.getMoveToMachine() + " " + sendMsg.getMaxMoveCount());
			}
		}
	}

	public void printDecission(PrintWriter stream) {
		for (Entry<Integer, List<SendQueryChunkMessage>> sender : WorkerVertSendMsgs.entrySet()) {
			stream.println(sender.getKey());
			for (SendQueryChunkMessage sendMsg : sender.getValue()) {
				stream.println("\t" + sendMsg.getIncludeQueriesList() + " " + sendMsg.getTolreateQueriesList() + " " + sender.getKey()
						+ "->" + sendMsg.getMoveToMachine() + " " + sendMsg.getMaxMoveCount());
			}
		}
	}
}
