package mthesis.concurrent_graph.communication;

import java.util.LinkedList;
import java.util.List;

import mthesis.concurrent_graph.util.Pair;
import mthesis.concurrent_graph.writable.BaseWritable;

public class VertexMessageBucket<M extends BaseWritable> {
	//	public final Map<Integer, List<M>> messageBuckets = new HashMap<>();
	public final List<Pair<Integer, M>> messages = new LinkedList<>();
	//	public int messageCount;

	public void addMessage(int dstVertex, M messageContent) {
		//		List<M> vertexBucket = messageBuckets.get(dstVertex);
		//		if(vertexBucket == null) {
		//			vertexBucket = new LinkedList<>();
		//			messageBuckets.put(dstVertex, vertexBucket);
		//		}
		//		vertexBucket.add(messageContent);
		//		messageCount++;
		messages.add(new Pair<Integer, M>(dstVertex, messageContent));
	}
}
