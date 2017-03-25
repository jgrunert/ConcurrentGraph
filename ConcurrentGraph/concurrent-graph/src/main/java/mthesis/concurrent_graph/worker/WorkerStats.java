package mthesis.concurrent_graph.worker;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.ByteString;

import mthesis.concurrent_graph.QueryStats;
import mthesis.concurrent_graph.communication.Messages.ControlMessage.WorkerStatsMessage.WorkerStatSample;

/**
 * Statistics per worker, over all active queries.
 * All statistics are kept in simple variables to keep performance good.
 * WorkerStats represent a sample of statistics in a fixed period of time.
 * There are two types of stats: Worker stats and aggregated query stats.
 * Query stats are aggregated by adding them to the worker step when a superstep is finished.
 *
 * @author Jonas Grunert
 */
public class WorkerStats {

	private static final int WorkerStatsMaxBytes = 2048;
	private final byte[] workerStatsBuffer = new byte[WorkerStatsMaxBytes];

	// Direct variables for quick access of frequently changed variables
	public long ActiveVertices;
	public long IdleTime;
	public long QueryWaitTime;
	public long HandleMessagesTime;
	public long ComputeTime;
	public long BarrierStartWaitTime;
	public long BarrierFinishWaitTime;
	public long BarrierVertexMoveTime;

	private QueryStats aggregatedQueryStats;


	public WorkerStats() {
		aggregatedQueryStats = new QueryStats();
	}

	public WorkerStats(ByteString bytesString) {
		super();
		ByteBuffer bytes = ByteBuffer.allocate(WorkerStatsMaxBytes);
		bytesString.copyTo(bytes);
		bytes.position(0);

		ActiveVertices = bytes.getLong();
		IdleTime = bytes.getLong();
		QueryWaitTime = bytes.getLong();
		HandleMessagesTime = bytes.getLong();
		ComputeTime = bytes.getLong();
		BarrierStartWaitTime = bytes.getLong();
		BarrierFinishWaitTime = bytes.getLong();
		BarrierVertexMoveTime = bytes.getLong();
		aggregatedQueryStats = new QueryStats(bytes);
	}


	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putLong(ActiveVertices);
		buffer.putLong(IdleTime);
		buffer.putLong(QueryWaitTime);
		buffer.putLong(HandleMessagesTime);
		buffer.putLong(ComputeTime);
		buffer.putLong(BarrierStartWaitTime);
		buffer.putLong(BarrierFinishWaitTime);
		buffer.putLong(BarrierVertexMoveTime);
		aggregatedQueryStats.writeToBuffer(buffer);
	}

	public Map<String, Long> getStatsMap() {
		Map<String, Long> statsMap = new TreeMap<>();

		statsMap.put("ActiveVertices", ActiveVertices);
		statsMap.put("IdleTime", IdleTime);
		statsMap.put("QueryWaitTime", QueryWaitTime);
		statsMap.put("HandleMessagesTime", HandleMessagesTime);
		statsMap.put("ComputeTime", ComputeTime);
		statsMap.put("BarrierStartWaitTime", BarrierStartWaitTime);
		statsMap.put("BarrierFinishWaitTime", BarrierFinishWaitTime);
		statsMap.put("BarrierVertexMoveTime", BarrierVertexMoveTime);

		statsMap.putAll(aggregatedQueryStats.getStatsMap());

		return statsMap;
	}


	public void addQueryStatsstepStats(QueryStats stats) {
		aggregatedQueryStats.combine(stats);
	}


	public WorkerStatSample getSample(long sampleTime) {
		ByteBuffer buffer = ByteBuffer.wrap(workerStatsBuffer);
		writeToBuffer(buffer);
		int byteCount = buffer.position();
		buffer.position(0);
		ByteString bs = ByteString.copyFrom(buffer, byteCount);
		return WorkerStatSample.newBuilder().setTime(sampleTime).setStatsBytes(bs).build();
	}
}
