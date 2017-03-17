package mthesis.concurrent_graph.apps.sssp;

import java.nio.ByteBuffer;
import java.util.List;

import mthesis.concurrent_graph.JobConfiguration;
import mthesis.concurrent_graph.vertex.AbstractVertex;
import mthesis.concurrent_graph.vertex.Edge;
import mthesis.concurrent_graph.vertex.VertexFactory;
import mthesis.concurrent_graph.worker.VertexWorkerInterface;
import mthesis.concurrent_graph.worker.WorkerQuery;
import mthesis.concurrent_graph.writable.DoubleWritable;

/**
 * Example vertex for single source shortest path
 *
 * @author Jonas Grunert
 *
 */
public class SSSPVertex extends AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

	public SSSPVertex(int id,
			VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> messageSender) {
		super(id, messageSender);
	}

	public SSSPVertex(ByteBuffer bufferToRead,
			VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> worker,
			JobConfiguration<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> jobConfig) {
		super(bufferToRead, worker, jobConfig);
	}

	@Override
	protected void compute(int superstepNo, List<SSSPMessageWritable> messages,
			WorkerQuery<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> query) {
		if (superstepNo == 0) {
			if (ID != query.Query.From) {
				voteVertexHalt(query.QueryId);
				return;
			}
			else {
				System.out.println("GO " + ID + " in " + query.QueryId);
				SSSPVertexWritable mutableValue = new SSSPVertexWritable(-1, 0, false);
				setValue(mutableValue, query.QueryId);
				for (Edge<DoubleWritable> edge : getEdges()) {
					sendMessageToVertex(getPooledMessageValue().setup(ID, edge.Value.Value), edge.TargetVertexId, query);
				}
				voteVertexHalt(query.QueryId);
				return;
			}
		}

		SSSPVertexWritable mutableValue = getValue(query.QueryId);
		if (mutableValue == null) {
			mutableValue = new SSSPVertexWritable(-1, Double.POSITIVE_INFINITY, false);
			setValue(mutableValue, query.QueryId);
		}

		double minDist = mutableValue.Dist;
		int minPre = mutableValue.Pre;
		if (messages != null) {
			for (SSSPMessageWritable msg : messages) {
				if (msg.Dist < minDist) {
					minDist = msg.Dist;
					minPre = msg.SrcVertex;
				}
			}
		}

		if (minDist > query.Query.MaxDist) {
			// Vertex is out of range
			setValue(null, query.QueryId);
			voteVertexHalt(query.QueryId);
			return;
		}

		if (minDist > superstepNo * 40) { // TODO Better factor, dynamic?
			// Come back later
			if (minDist < mutableValue.Dist) {
				mutableValue.Dist = minDist;
				mutableValue.Pre = minPre;
				mutableValue.SendMsgsLater = true; // Send messages to neighbors later
			}
			return;
		}

		//		if (superstepNo == 209) {
		//			voteVertexHalt(query.QueryId);
		//			return;
		//		}

		boolean sendMessages = mutableValue.SendMsgsLater;
		if (minDist < mutableValue.Dist) {
			mutableValue.Dist = minDist;
			mutableValue.Pre = minPre;
			sendMessages = true;
		}
		if (sendMessages) {
			for (Edge<DoubleWritable> edge : getEdges()) {
				sendMessageToVertex(getPooledMessageValue().setup(ID, mutableValue.Dist + edge.Value.Value), edge.TargetVertexId, query);
			}
			mutableValue.SendMsgsLater = false;
		}
		voteVertexHalt(query.QueryId);


		// TODO Better, faster termination
		if (ID == query.Query.To) {
			//			System.out.println("Target dist " + minDist + " max " + query.QueryLocal.MaxDist);
			query.QueryLocal.TargetFound = true;
			query.QueryLocal.MaxDist = minDist;
		}
	}


	public static class Factory extends VertexFactory<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> {

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> newInstance(int id,
				VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> messageSender) {
			return new SSSPVertex(id, messageSender);
		}

		@Override
		public AbstractVertex<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> newInstance(ByteBuffer bufferToRead,
				VertexWorkerInterface<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> worker,
				JobConfiguration<SSSPVertexWritable, DoubleWritable, SSSPMessageWritable, SSSPQueryValues> jobConfig) {
			return new SSSPVertex(bufferToRead, worker, jobConfig);
		}
	}
}
