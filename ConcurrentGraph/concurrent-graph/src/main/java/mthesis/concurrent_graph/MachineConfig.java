package mthesis.concurrent_graph;


public class MachineConfig {

	public final String HostName;
	public final int MessagePort;
	// Indicates if run in a separate VM. By default false.
	public final boolean ExtraVm;

	public MachineConfig(String hostName, int messagePort, boolean extraVm) {
		super();
		HostName = hostName;
		MessagePort = messagePort;
		ExtraVm = extraVm;
	}
}
