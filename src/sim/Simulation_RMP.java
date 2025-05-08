package sim;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Simulation_RMP extends Simulation_ClusterModelTransmission {

	public static void main(String[] args) throws IOException, InterruptedException {
		final String USAGE_INFO = String.format(
				"Usage: java %s PROP_FILE_DIRECTORY " + "<-export_skip_backup> <-printProgress> <-seedMap=SEED_MAP>\n"
						+ "    or java %s -genMap PROP_FILE_DIRECTORY -seedMap=SEED_MAP",
				Simulation_RMP.class.getName());
		if (args.length < 1) {
			System.out.println(USAGE_INFO);
			System.exit(0);
		} else if ("-genMap".equals(args[0])) {
			File baseDir = new File(args[1]);
			
			// Reading of seedFile or seedString
			String seedMap_entry = args[2];
			seedMap_entry = seedMap_entry.substring("-seedMap=".length());
			String[] cMap_seeds;
			if (seedMap_entry.endsWith(".csv")) {
				cMap_seeds = util.Util_7Z_CSV_Entry_Extract_Callable
						.extracted_lines_from_text(new File(baseDir, seedMap_entry));
			} else {
				cMap_seeds = seedMap_entry.split(",");

			}

			// Reading of PROP file
			File propFile = new File(baseDir, SimulationInterface.FILENAME_PROP);
			FileInputStream fIS = new FileInputStream(propFile);
			Properties loadedProperties = new Properties();
			loadedProperties.loadFromXML(fIS);
			fIS.close();
			System.out.println(String.format("Properties file < %s > loaded.", propFile.getAbsolutePath()));							

			int num_core = 1;
			
			if(loadedProperties.containsKey(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL])) {
				num_core = Integer.parseInt(loadedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL]));
			}
			
			
			ExecutorService exec = null;
			int numInExec = 0;
			int numInExec_max = Math.min(Math.min(num_core, cMap_seeds.length),
					Runtime.getRuntime().availableProcessors());
			if (numInExec_max > 1) {
				exec = Executors.newFixedThreadPool(numInExec_max);
			}						

			for (String cMapStr : cMap_seeds) {
				Runnable_ContacMap_Generation_MetaPopulation runnable_genMap = new Runnable_ContacMap_Generation_MetaPopulation(
						Long.parseLong(cMapStr), loadedProperties);

				if (numInExec_max <= 1) {
					System.out.printf("Generating map with seed of %s.\n", cMapStr);
					runnable_genMap.run();
				} else {
					System.out.printf("Parallel: Generating map with seed of %s.\n", cMapStr);
					if (exec == null) {
						exec = Executors.newFixedThreadPool(numInExec_max);
					}
					exec.submit(runnable_genMap);
					numInExec++;

					if (numInExec == numInExec_max) {
						exec.shutdown();
						if (!exec.awaitTermination(2, TimeUnit.DAYS)) {
							System.err.println("Thread time-out!");
						}
						exec = null;
					}
				}
			}

			if (exec != null) {
				if (!exec.awaitTermination(2, TimeUnit.DAYS)) {
					System.err.println("Thread time-out!");
				}

			}

		} else {
			Simulation_ClusterModelTransmission.launch(args);
		}
	}

	@Override
	public Abstract_Runnable_ClusterModel_Transmission generateDefaultRunnable(long cMap_seed, long sim_seed,
			Properties loadProperties) {

		String popType = (String) loadedProperties
				.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_POP_TYPE]);

		//TODO: Test for popType

		return null;
	}

}
