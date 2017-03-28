package mthesis.concurrent_graph;


public class MachineConfig {

	public final int MachineId;
	public final String HostName;
	public final int MessagePort;

	public MachineConfig(int machineId, String hostName, int messagePort) {
		super();
		MachineId = machineId;
		HostName = hostName;
		MessagePort = messagePort;
	}
}
