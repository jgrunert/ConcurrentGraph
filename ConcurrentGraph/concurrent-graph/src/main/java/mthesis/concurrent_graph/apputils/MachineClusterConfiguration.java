package mthesis.concurrent_graph.apputils;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mthesis.concurrent_graph.MachineConfig;

public class MachineClusterConfiguration {

	public final int masterId;
	public final List<Integer> AllWorkerIds = new ArrayList<>();
	public final Map<Integer, MachineConfig> AllMachineConfigs = new HashMap<>();
	public final Map<Integer, Boolean> StartOnThisMachine = new HashMap<>();

	public MachineClusterConfiguration(String configFilePath) {
		masterId = -1;
		try {
			List<String> lines = Files.readAllLines(Paths.get(configFilePath), Charset.forName("UTF-8"));
			AddMachineConfig(-1, lines.get(1));
			for (int i = 3; i < lines.size(); i++) {
				AllWorkerIds.add(i - 3);
				AddMachineConfig(i - 3, lines.get(i));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void AddMachineConfig(int id, String cfg) {
		String[] sSplit = cfg.split("\t");
		boolean extraVm = (sSplit.length >= 4) ? Boolean.parseBoolean(sSplit[3]) : true;
		AllMachineConfigs.put(id, new MachineConfig(sSplit[0], Integer.parseInt(sSplit[1]), extraVm));
		StartOnThisMachine.put(id, Boolean.parseBoolean(sSplit[2]));
	}
}
