package mthesis.concurrent_graph;


public class MachineConfig {
	public final String HostName;
	public final int MessagePort;

	public MachineConfig(String hostName, int messagePort) {
		super();
		HostName = hostName;
		MessagePort = messagePort;
	}
}
