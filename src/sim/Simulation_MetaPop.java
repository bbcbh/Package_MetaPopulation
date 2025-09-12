package sim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import map.Abstract_Map_Location;
import map.Map_Location_IREG;

public class Simulation_MetaPop extends Simulation_ClusterModelTransmission {

	public static final String PROP_BASEDIR = "PROP_BASEDIR";

	public static void main(String[] args) throws IOException, InterruptedException {
		final String USAGE_INFO = String.format(
				"Usage: java %s PROP_FILE_DIRECTORY " + "<-export_skip_backup> <-printProgress> <-seedMap=SEED_MAP>\n"
						+ "    or java %s -genMap PROP_FILE_DIRECTORY -seedMap=SEED_MAP",
				Simulation_MetaPop.class.getName(), Simulation_MetaPop.class.getName());
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

			if (loadedProperties.containsKey(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL])) {
				num_core = Integer.parseInt(loadedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL]));
			}

			loadedProperties.put(PROP_BASEDIR, baseDir);

			ExecutorService exec = null;
			int numInExec = 0;
			int numInExec_max = Math.min(Math.min(num_core, cMap_seeds.length),
					Runtime.getRuntime().availableProcessors());
			if (numInExec_max > 1) {
				exec = Executors.newFixedThreadPool(numInExec_max);
			}

			Abstract_Map_Location shared_loc_map = null;

			// Generate demographic
			if (Runnable_ContactMap_Generation_LocationMap_IREG.POP_TYPE
					.equals(loadedProperties.getProperty(PROP_NAME[PROP_POP_TYPE]))) {
				shared_loc_map = new Map_Location_IREG();
				File file_map = new File(loadedProperties.getProperty(String.format("%s%d",
						Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
						Runnable_Demographic_Generation.RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH)));
				File file_nodeInfo = new File(file_map.getParent(), String.format("%s_NoteInfo.csv",
						file_map.getName().substring(0, file_map.getName().length() - 4)));

				BufferedReader reader_map = new BufferedReader(new FileReader(file_map));
				BufferedReader reader_nodeinfo = new BufferedReader(new FileReader(file_nodeInfo));

				shared_loc_map.importConnectionsFromString(reader_map);
				shared_loc_map.importNodeInfoFromString(reader_nodeinfo);

				reader_map.close();
				reader_nodeinfo.close();
				loadedProperties.put(Simulation_MetaPop.PROP_LOC_MAP, shared_loc_map);

				for (String cMapStr : cMap_seeds) {
					Runnable_Demographic_Generation demo_gen = new Runnable_Demographic_Generation(
							Long.parseLong(cMapStr), loadedProperties);
					if (numInExec_max <= 1) {
						System.out.printf("Generating demographic with seed of %s.\n", cMapStr);
						demo_gen.run();
					} else {
						System.out.printf("Parallel: Generating demographic with seed of %s.\n", cMapStr);
						if (exec == null) {
							exec = Executors.newFixedThreadPool(numInExec_max);
						}
						exec.submit(demo_gen);
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

			}

			for (String cMapStr : cMap_seeds) {
				Runnable_ClusterModel_ContactMap_Generation_MultiMap runnable_genMap;

				if (Runnable_ContactMap_Generation_LocationMap_IREG.POP_TYPE
						.equals(loadedProperties.getProperty(PROP_NAME[PROP_POP_TYPE]))) {
					// TODO: Run multiple population in parallel
					runnable_genMap = new Runnable_ContactMap_Generation_LocationMap_IREG(Long.parseLong(cMapStr),
							loadedProperties);
				} else {
					runnable_genMap = new Runnable_ContactMap_Generation_MetaPopulation(Long.parseLong(cMapStr),
							loadedProperties);
				}
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
			Simulation_ClusterModelTransmission.launch(args, new Simulation_MetaPop());
		}
	}

	public static final String PROP_LOC_MAP = "PROP_LOC_MAP";

	@Override
	public Abstract_Runnable_ClusterModel_Transmission generateDefaultRunnable(long cMap_seed, long sim_seed,
			Properties loadProperties) {

		String popType = (String) loadedProperties
				.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_POP_TYPE]);

		// TODO: Simulation to be implemented

		return null;
	}

}
