package mthesis.concurrent_graph.master.vertexmove;

import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.ReceiveQueryVerticesMessage;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.StartBarrierMessage.SendQueryVerticesMessage;


/**
 * Represents a decision about vertex moving.
 *
 * @author Jonas Grunert
 *
 */
public class VertexMoveDecision {

	public final Map<Integer, List<SendQueryVerticesMessage>> WorkerVertSendMsgs;
	public final Map<Integer, List<ReceiveQueryVerticesMessage>> WorkerVertRecvMsgs;


	public VertexMoveDecision(Map<Integer, List<SendQueryVerticesMessage>> workerVertSendMsgs,
			Map<Integer, List<ReceiveQueryVerticesMessage>> workerVertRecvMsgs) {
		super();
		WorkerVertSendMsgs = workerVertSendMsgs;
		WorkerVertRecvMsgs = workerVertRecvMsgs;
	}
}
