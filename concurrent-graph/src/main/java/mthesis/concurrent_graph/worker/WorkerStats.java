package mthesis.concurrent_graph.worker;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

	// Direct variables for quick access of frequently changed variables
	public long WorkerVertices;
	public long ActiveVertices;
	public long IdleTime;
	public long QueryWaitTime;
	public long HandleMessagesTime;
	public long BarrierStartWaitTime;
	public long BarrierFinishWaitTime;
	public long BarrierVertexMoveTime;
	public long IntersectCalcTime;
	public long SuperstepsFinished;

	public double SystemCpuLoad;
	public double ProcessCpuTime;
	public double ProcessCpuLoad;

	/** All queries, their active vertices and intersections since last vertex move barrier */
	//public Map<Integer, Map<Integer, Integer>> QueryIntersectsSinceBarrier;

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
		ByteBuffer bytes = ByteBuffer.allocate(bytesString.size());
		bytesString.copyTo(bytes);
		bytes.position(0);

		WorkerVertices = bytes.getLong();
		ActiveVertices = bytes.getLong();
		IdleTime = bytes.getLong();
		QueryWaitTime = bytes.getLong();
		HandleMessagesTime = bytes.getLong();
		BarrierStartWaitTime = bytes.getLong();
		BarrierFinishWaitTime = bytes.getLong();
		BarrierVertexMoveTime = bytes.getLong();
		IntersectCalcTime = bytes.getLong();
		SuperstepsFinished = bytes.getLong();

		SystemCpuLoad = bytes.getDouble();
		ProcessCpuTime = bytes.getDouble();
		ProcessCpuLoad = bytes.getDouble();

		//		int qICount = bytes.getInt();
		//		QueryIntersectsSinceBarrier = new HashMap<>(qICount);
		//		for (int iQi = 0; iQi < qICount; iQi++) {
		//			Integer qiKey = bytes.getInt();
		//			Integer qiCount = bytes.getInt();
		//			Map<Integer, Integer> qiIntMap = new HashMap<>(qiCount);
		//			for (int iQiInt = 0; iQiInt < qiCount; iQiInt++) {
		//				qiIntMap.put(bytes.getInt(), bytes.getInt());
		//			}
		//			QueryIntersectsSinceBarrier.put(qiKey, qiIntMap);
		//		}

		aggregatedQueryStats = new QueryStats(bytes);
	}

	private void writeToStream(DataOutputStream stream) throws IOException {
		stream.writeLong(WorkerVertices);
		stream.writeLong(ActiveVertices);
		stream.writeLong(IdleTime);
		stream.writeLong(QueryWaitTime);
		stream.writeLong(HandleMessagesTime);
		stream.writeLong(BarrierStartWaitTime);
		stream.writeLong(BarrierFinishWaitTime);
		stream.writeLong(BarrierVertexMoveTime);
		stream.writeLong(IntersectCalcTime);
		stream.writeLong(SuperstepsFinished);

		stream.writeDouble(SystemCpuLoad);
		stream.writeDouble(ProcessCpuTime);
		stream.writeDouble(ProcessCpuLoad);

		//		if (QueryIntersectsSinceBarrier != null) {
		//			stream.writeInt(QueryIntersectsSinceBarrier.size());
		//			for (Entry<Integer, Map<Integer, Integer>> qI : QueryIntersectsSinceBarrier.entrySet()) {
		//				stream.writeInt(qI.getKey());
		//				stream.writeInt(qI.getValue().size());
		//				for (Entry<Integer, Integer> inters : qI.getValue().entrySet()) {
		//					stream.writeInt(inters.getKey());
		//					stream.writeInt(inters.getValue());
		//				}
		//			}
		//		}
		//		else stream.writeInt(0);

		aggregatedQueryStats.writeToStream(stream);
	}

	public Map<String, Double> getStatsMap() {
		Map<String, Double> statsMap = new TreeMap<>();

		statsMap.put("WorkerVertices", (double) WorkerVertices);
		statsMap.put("ActiveVertices", (double) ActiveVertices);
		statsMap.put("IdleTime", (double) IdleTime);
		statsMap.put("QueryWaitTime", (double) QueryWaitTime);
		statsMap.put("HandleMessagesTime", (double) HandleMessagesTime);
		statsMap.put("BarrierStartWaitTime", (double) BarrierStartWaitTime);
		statsMap.put("BarrierFinishWaitTime", (double) BarrierFinishWaitTime);
		statsMap.put("BarrierVertexMoveTime", (double) BarrierVertexMoveTime);
		statsMap.put("IntersectCalcTime", (double) IntersectCalcTime);
		statsMap.put("SuperstepsFinished", (double) SuperstepsFinished);

		statsMap.put("SystemCpuLoad", SystemCpuLoad);
		statsMap.put("ProcessCpuTime", ProcessCpuTime);
		statsMap.put("ProcessCpuLoad", ProcessCpuLoad);

		statsMap.putAll(aggregatedQueryStats.getStatsMap());

		return statsMap;
	}


	public void addQueryStatsstepStats(QueryStats stats) {
		aggregatedQueryStats.combine(stats);
	}


	public WorkerStatSample getSample(long sampleTime) throws IOException {
		if (cpuStatsActive) {
			SystemCpuLoad = operatingSystemMXBean.getSystemCpuLoad() * 100;
			ProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();
			ProcessCpuLoad = operatingSystemMXBean.getProcessCpuLoad() * 100;
		}

		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		writeToStream(new DataOutputStream(outStream));
		ByteString bs = ByteString.copyFrom(outStream.toByteArray());
		return WorkerStatSample.newBuilder().setTime(sampleTime).setStatsBytes(bs).build();
	}
}
