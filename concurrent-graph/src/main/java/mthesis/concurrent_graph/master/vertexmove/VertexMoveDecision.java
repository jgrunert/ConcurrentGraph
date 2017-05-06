package mthesis.concurrent_graph.master.vertexmove;

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

	public final Map<Integer, List<SendQueryChunkMessage>> WorkerVertSendMsgs;
	public final Map<Integer, List<ReceiveQueryChunkMessage>> WorkerVertRecvMsgs;


	public VertexMoveDecision(Map<Integer, List<SendQueryChunkMessage>> workerVertSendMsgs,
			Map<Integer, List<ReceiveQueryChunkMessage>> workerVertRecvMsgs) {
		super();
		WorkerVertSendMsgs = workerVertSendMsgs;
		WorkerVertRecvMsgs = workerVertRecvMsgs;
	}

	public void printDecission() {
		for (Entry<Integer, List<ReceiveQueryChunkMessage>> receiver : WorkerVertRecvMsgs.entrySet()) {
			System.out.println(receiver.getKey());
			for (ReceiveQueryChunkMessage recvMsg : receiver.getValue()) {
				System.out.println("   " + recvMsg.getChunkQueriesList() + " from " + recvMsg.getReceiveFromMachine());
			}
		}
	}
}
