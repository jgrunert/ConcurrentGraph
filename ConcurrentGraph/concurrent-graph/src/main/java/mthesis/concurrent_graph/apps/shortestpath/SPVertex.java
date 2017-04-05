package mthesis.concurrent_graph.apps.shortestpath;

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
 * Example vertex for shortest path
 *
 * @author Jonas Grunert
 *
 */
public class SPVertex extends AbstractVertex<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> {

	//	private final Map<Integer, Integer> visits = new HashMap<>(4);
	//	private static int firstVisits = 0;
	//	private static int reVisits = 0;
	//	private static int maxVisits = 0;

	public SPVertex(int id,
			VertexWorkerInterface<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> messageSender) {
		super(id, messageSender);
	}

	public SPVertex(ByteBuffer bufferToRead,
			VertexWorkerInterface<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> worker,
			JobConfiguration<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> jobConfig) {
		super(bufferToRead, worker, jobConfig);
	}

	@Override
	protected void compute(int superstepNo, List<SPMessageWritable> messages,
			WorkerQuery<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> query) {
		// Revisit testing code
		//		int vis = MiscUtil.defaultInt(visits.get(query.QueryId));
		//		visits.put(query.QueryId, vis + 1);
		//		if (vis >= 1) {
		//			if (vis > 1) reVisits++;
		//			else firstVisits++;
		//			maxVisits = Math.max(maxVisits, vis);
		//			if (reVisits % 100000 == 1 || firstVisits % 100000 == 1)
		//				System.out.println(reVisits + "/" + firstVisits + " " + vis + " " + maxVisits);
		//		}

		if (superstepNo == 0) {
			if (ID != query.Query.From) {
				voteVertexHalt(query.QueryId);
				return;
			}
			else {
				logger.info(query.QueryId + ":" + superstepNo + " start vertex compute start");
				SPVertexWritable mutableValue = new SPVertexWritable(-1, 0, false, false);
				setValue(mutableValue, query.QueryId);
				for (Edge<DoubleWritable> edge : getEdges()) {
					sendMessageToVertex(getPooledMessageValue().setup(ID, edge.Value.Value), edge.TargetVertexId,
							query);
				}
				voteVertexHalt(query.QueryId);
				return;
			}
		}

		SPVertexWritable mutableValue = getValue(query.QueryId);

		// Reconstruct path over all vertices that have been visitied so far
		if (query.Query.ReconstructionPhaseActive && mutableValue != null) {
			if (!query.Query.InitializedReconstructionPhase) {
				// Start reconstruction at target vertex
				if (ID == query.Query.To) {
					logger.info(query.QueryId + ":" + superstepNo + " target vertex " + ID + " start reconstructing");
					mutableValue.OnShortestPath = true;
					sendMessageToVertex(getPooledMessageValue().setup(ID, 0), mutableValue.Pre, query);
				}
			}
			else {
				// Send message to pre vertex until start vertex reached
				mutableValue.OnShortestPath = true;
				if (ID != query.Query.From) {
					sendMessageToVertex(getPooledMessageValue().setup(ID, 0), mutableValue.Pre, query);
				}
			}
			voteVertexHalt(query.QueryId);
			return;
		}

		if (mutableValue == null) {
			mutableValue = new SPVertexWritable(-1, Double.POSITIVE_INFINITY, false, false);
			setValue(mutableValue, query.QueryId);
		}

		double minDist = mutableValue.Dist;
		int minPre = mutableValue.Pre;
		if (messages != null) {
			for (SPMessageWritable msg : messages) {
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

		if (minDist > superstepNo * 6.0) { // TODO Dynamic limit step?
			// Come back later
			if (minDist < mutableValue.Dist) {
				mutableValue.Dist = minDist;
				mutableValue.Pre = minPre;
				mutableValue.SendMsgsLater = true; // Send messages to neighbors later
			}
			return;
		}

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

		if (ID == query.Query.To) {
			// Target vertex found.  Now start limiting max dist to target dist.
			if (query.QueryLocal.MaxDist == Double.POSITIVE_INFINITY)
				logger.info(query.QueryId + ":" + superstepNo + " target vertex " + ID + " found with dist " + minDist);
			query.QueryLocal.MaxDist = minDist;
		}
		//		else {
		//			// Halt vertex next superstep if not target and no messages
		//			voteVertexHalt(query.QueryId);
		//		}
	}



	@Override
	public SPVertexWritable getOutputValue(int queryId) {
		SPVertexWritable value = super.getOutputValue(queryId);
		return value != null && value.OnShortestPath ? value : null;
	}


	public static class Factory extends VertexFactory<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> {

		@Override
		public AbstractVertex<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> newInstance(int id,
				VertexWorkerInterface<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> messageSender) {
			return new SPVertex(id, messageSender);
		}

		@Override
		public AbstractVertex<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> newInstance(
				ByteBuffer bufferToRead,
				VertexWorkerInterface<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> worker,
				JobConfiguration<SPVertexWritable, DoubleWritable, SPMessageWritable, SPQuery> jobConfig) {
			return new SPVertex(bufferToRead, worker, jobConfig);
		}
	}
}

