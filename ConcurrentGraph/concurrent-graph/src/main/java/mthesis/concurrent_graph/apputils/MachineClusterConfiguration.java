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

	public MachineClusterConfiguration(String configFilePath) {
		int masterIdTmp = -1;
		try {
			List<String> lines = Files.readAllLines(Paths.get(configFilePath), Charset.forName("UTF-8"));
			MachineConfig masterConfig = AddMachineConfig(lines.get(1));
			masterIdTmp = masterConfig.MachineId;
			for (int i = 3; i < lines.size(); i++) {
				MachineConfig workerConfig = AddMachineConfig(lines.get(i));
				AllWorkerIds.add(workerConfig.MachineId);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		masterId = masterIdTmp;
	}

	private MachineConfig AddMachineConfig(String cfg) {
		String[] sSplit = cfg.split("\t");
		MachineConfig config = new MachineConfig(Integer.parseInt(sSplit[0]), sSplit[1], Integer.parseInt(sSplit[2]));
		AllMachineConfigs.put(config.MachineId, config);
		return config;
	}
}
