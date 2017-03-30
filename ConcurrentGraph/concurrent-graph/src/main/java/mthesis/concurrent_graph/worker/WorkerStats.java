package mthesis.concurrent_graph.worker;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.ByteString;
import com.sun.management.OperatingSystemMXBean;

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
@SuppressWarnings("restriction")
public class WorkerStats {

	private static final int WorkerStatsMaxBytes = 2048;
	private final byte[] workerStatsBuffer = new byte[WorkerStatsMaxBytes];

	// Direct variables for quick access of frequently changed variables
	public long ActiveVertices;
	public long IdleTime;
	public long QueryWaitTime;
	public long HandleMessagesTime;
	public long BarrierStartWaitTime;
	public long BarrierFinishWaitTime;
	public long BarrierVertexMoveTime;

	public double SystemCpuLoad;
	public double ProcessCpuTime;
	public double ProcessCpuLoad;

	private QueryStats aggregatedQueryStats;

	private static final boolean cpuStatsActive;
	private static final OperatingSystemMXBean operatingSystemMXBean;

	static {
		OperatingSystemMXBean operatingSystemMXBeanTmp = null;
		try {
			operatingSystemMXBeanTmp = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		}
		catch (Throwable e) {
			e.printStackTrace();
		}
		operatingSystemMXBean = operatingSystemMXBeanTmp;
		cpuStatsActive = operatingSystemMXBean != null;
	}

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
		BarrierStartWaitTime = bytes.getLong();
		BarrierFinishWaitTime = bytes.getLong();
		BarrierVertexMoveTime = bytes.getLong();

		SystemCpuLoad = bytes.getDouble();
		ProcessCpuTime = bytes.getDouble();
		ProcessCpuLoad = bytes.getDouble();

		aggregatedQueryStats = new QueryStats(bytes);
	}


	public void writeToBuffer(ByteBuffer buffer) {
		buffer.putLong(ActiveVertices);
		buffer.putLong(IdleTime);
		buffer.putLong(QueryWaitTime);
		buffer.putLong(HandleMessagesTime);
		buffer.putLong(BarrierStartWaitTime);
		buffer.putLong(BarrierFinishWaitTime);
		buffer.putLong(BarrierVertexMoveTime);

		buffer.putDouble(SystemCpuLoad);
		buffer.putDouble(ProcessCpuTime);
		buffer.putDouble(ProcessCpuLoad);

		aggregatedQueryStats.writeToBuffer(buffer);
	}

	public Map<String, Double> getStatsMap() {
		Map<String, Double> statsMap = new TreeMap<>();

		statsMap.put("ActiveVertices", (double) ActiveVertices);
		statsMap.put("IdleTime", (double) IdleTime);
		statsMap.put("QueryWaitTime", (double) QueryWaitTime);
		statsMap.put("HandleMessagesTime", (double) HandleMessagesTime);
		statsMap.put("BarrierStartWaitTime", (double) BarrierStartWaitTime);
		statsMap.put("BarrierFinishWaitTime", (double) BarrierFinishWaitTime);
		statsMap.put("BarrierVertexMoveTime", (double) BarrierVertexMoveTime);

		statsMap.put("SystemCpuLoad", SystemCpuLoad);
		statsMap.put("ProcessCpuTime", ProcessCpuTime);
		statsMap.put("ProcessCpuLoad", ProcessCpuLoad);

		statsMap.putAll(aggregatedQueryStats.getStatsMap());

		return statsMap;
	}


	public void addQueryStatsstepStats(QueryStats stats) {
		aggregatedQueryStats.combine(stats);
	}


	public WorkerStatSample getSample(long sampleTime) {
		if (cpuStatsActive) {
			SystemCpuLoad = operatingSystemMXBean.getSystemCpuLoad() * 100;
			ProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();
			ProcessCpuLoad = operatingSystemMXBean.getProcessCpuLoad() * 100;
		}

		ByteBuffer buffer = ByteBuffer.wrap(workerStatsBuffer);
		writeToBuffer(buffer);
		int byteCount = buffer.position();
		buffer.position(0);
		ByteString bs = ByteString.copyFrom(buffer, byteCount);
		return WorkerStatSample.newBuilder().setTime(sampleTime).setStatsBytes(bs).build();
	}
}
