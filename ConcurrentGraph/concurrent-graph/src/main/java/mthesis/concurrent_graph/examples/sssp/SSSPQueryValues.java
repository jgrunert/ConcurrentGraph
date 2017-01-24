package mthesis.concurrent_graph.examples.sssp;

import java.nio.ByteBuffer;

import mthesis.concurrent_graph.BaseQueryGlobalValues;

public class SSSPQueryValues extends BaseQueryGlobalValues {

	public int From;
	public int To;
	// Maximum distance. Initially set to infinite, set to target dist as soon as target discovered
	public double MaxDist;
	public boolean TargetFound; // TODO Remove?


	/**
	 * Public query creation constructor
	 */
	public SSSPQueryValues(int queryId, int from, int to) {
		super(queryId);
		From = from;
		To = to;
		MaxDist = Double.POSITIVE_INFINITY;
		TargetFound = false;
	}

	private SSSPQueryValues(int queryId, int activeVertices, int vertexCount, int messagesTransmittedLocal, int messagesSentUnicast,
			int messagesSentBroadcast, int messageBucketsSentUnicast, int messageBucketsSentBroadcast, int messagesReceivedWrongVertex,
			int messagesReceivedCorrectVertex, int discoveredNewVertexMachines,
			int from, int to, double maxDist, boolean targetFound) {
		super(queryId, activeVertices, vertexCount, messagesTransmittedLocal, messagesSentUnicast,
				messagesSentBroadcast, messageBucketsSentUnicast, messageBucketsSentBroadcast, messagesReceivedWrongVertex,
				messagesReceivedCorrectVertex, discoveredNewVertexMachines);
		From = from;
		To = to;
		MaxDist = maxDist;
		TargetFound = targetFound;
	}


	@Override
	public void writeToBuffer(ByteBuffer buffer) {
		super.writeToBuffer(buffer);
		buffer.putInt(From);
		buffer.putInt(To);
		buffer.putDouble(MaxDist);
		buffer.put(TargetFound ? (byte) 0 : (byte) 1);
	}

	@Override
	public String getString() {
		return super.getString() + ":" + From + ":" + To + ":" + MaxDist + ":" + TargetFound;
	}

	@Override
	public int getBytesLength() {
		return super.getBytesLength() + 2 * 4 + 8 + 1;
	}

	@Override
	public void combine(BaseQueryGlobalValues v) {
		SSSPQueryValues other = (SSSPQueryValues) v;
		MaxDist = Math.min(MaxDist, other.MaxDist);
		TargetFound |= (other).TargetFound;
		super.combine(v);
	}



	public static class Factory extends BaseQueryGlobalValuesFactory<SSSPQueryValues> {

		@Override
		public SSSPQueryValues createDefault(int queryId) {
			return new SSSPQueryValues(queryId, 0, 0);
		}

		@Override
		public SSSPQueryValues createFromString(String str) {
			final String[] sSplit = str.split(":");
			int iSplit = 0;
			return new SSSPQueryValues(Integer.parseInt(sSplit[iSplit++]), Integer.parseInt(sSplit[iSplit++]),
					Integer.parseInt(sSplit[iSplit++]),
					Integer.parseInt(sSplit[iSplit++]), Integer.parseInt(sSplit[iSplit++]), Double.parseDouble(sSplit[iSplit++]),
					Boolean.parseBoolean(sSplit[iSplit++]));
		}

		@Override
		public SSSPQueryValues createFromBytes(ByteBuffer bytes) {
			return new SSSPQueryValues(bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(), bytes.getInt(),
					bytes.getDouble(), bytes.get() == 0);
		}
	}
}
