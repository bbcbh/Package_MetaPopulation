package sim;

public class Simulation_Gen_MetaPop extends Simulation_ClusterModelTransmission {

	public static final String PROP_BASEDIR = "PROP_BASEDIR";
	public static final String PROP_LOC_MAP = "PROP_LOC_MAP";
	public static final String PROP_PRELOAD_FILES = "PROP_PRELOAD_FILES";
	public static final String PROP_INDIV_STAT = "PROP_INDIV_STAT";
	public static final String PROP_PARNTER_EXTRA_SOUGHT = "PROP_PARNTER_EXTRA_SOUGHT";
	public static final String PROP_CONTACT_MAP_LOC = "PROP_CONTACT_MAP_LOC";	
	
	public static final String DIR_NAME_FORMAT_DEMOGRAPHIC = "Demographic_%d";
	
//	public static void test_main(String[] args) throws IOException, InterruptedException {
//		final String USAGE_INFO = String.format(
//				"Usage: java %s PROP_FILE_DIRECTORY " + "<-export_skip_backup> <-printProgress> <-seedMap=SEED_MAP>\n"
//						+ "    or java %s -genMap PROP_FILE_DIRECTORY -seedMap=SEED_MAP",
//				Simulation_Gen_MetaPop.class.getName(), Simulation_Gen_MetaPop.class.getName());
//		if (args.length < 1) {
//			System.out.println(USAGE_INFO);
//			System.exit(0);
//		} else if ("-genMap".equals(args[0])) {
//			File baseDir = new File(args[1]);
//
//			// Reading of seedFile or seedString
//			String seedMap_entry = args[2];
//			seedMap_entry = seedMap_entry.substring("-seedMap=".length());
//			String[] cMap_seeds;
//			if (seedMap_entry.endsWith(".csv")) {
//				cMap_seeds = util.Util_7Z_CSV_Entry_Extract_Callable
//						.extracted_lines_from_text(new File(baseDir, seedMap_entry));
//			} else {
//				cMap_seeds = seedMap_entry.split(",");
//
//			}
//
//			// Reading of PROP file
//			File propFile = new File(baseDir, SimulationInterface.FILENAME_PROP);
//			FileInputStream fIS = new FileInputStream(propFile);
//			Properties loadedPropertiesProp = new Properties();
//			loadedPropertiesProp.loadFromXML(fIS);
//			fIS.close();
//			System.out.println(String.format("Properties file < %s > loaded.", propFile.getAbsolutePath()));
//			
//			HashMap<String, Object> loadedProperties = new HashMap<>();
//			for (Entry<String, Object> ent : loadedProperties.entrySet()) {
//				loadedProperties.put(ent.getKey(), ent.getValue());
//			}
//			
//			
//
//			int num_core = 1;
//
//			if (loadedProperties.containsKey(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL])) {
//				num_core = Integer.parseInt(loadedProperties
//						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL]));
//			}
//
//			loadedProperties.put(PROP_BASEDIR, baseDir);
//
//			ExecutorService exec = null;
//			int numInExec = 0;
//			int numInExec_max = Math.min(Math.min(num_core, cMap_seeds.length),
//					Runtime.getRuntime().availableProcessors());
//			if (numInExec_max > 1) {
//				exec = Executors.newFixedThreadPool(numInExec_max);
//			}
//
//			HashMap<Long, Map_Location_Mobility> map_shared_loc_map = new HashMap<>();
//
//			for (String cMapStr : cMap_seeds) {
//				Runnable runnable_genMap;
//
//				if (Runnable_ContactMap_Generation_Demographic.POP_TYPE
//						.equals(loadedProperties.getProperty(PROP_NAME[PROP_POP_TYPE]))) {
//
//					long mapSeed = Long.parseLong(cMapStr);
//					Map_Location_Mobility shared_loc_map = map_shared_loc_map.get(mapSeed);
//
//					if (shared_loc_map == null) {
//						// Generate demographic
//						shared_loc_map = new Map_Location_Mobility();
//						File file_map = new File(loadedProperties.getProperty(
//								String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
//										Runnable_Demographic_Generation.RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH)));
//						File file_nodeInfo = new File(file_map.getParent(), String.format("%s_NoteInfo.csv",
//								file_map.getName().substring(0, file_map.getName().length() - 4)));
//
//						File file_awayPercent = new File(file_map.getParent(), String.format("%s_Away.csv",
//								file_map.getName().substring(0, file_map.getName().length() - 4)));
//						BufferedReader reader_map = new BufferedReader(new FileReader(file_map));
//						BufferedReader reader_nodeinfo = new BufferedReader(new FileReader(file_nodeInfo));
//						BufferedReader reader_away = new BufferedReader(new FileReader(file_awayPercent));
//						shared_loc_map.importConnectionsFromString(reader_map);
//						shared_loc_map.importNodeInfoFromString(reader_nodeinfo);
//						shared_loc_map.importAwayInfoFromString(reader_away);
//						reader_map.close();
//						reader_nodeinfo.close();
//						reader_away.close();
//						map_shared_loc_map.put(mapSeed, shared_loc_map);
//						Runnable_Demographic_Generation run_gen_dem = new Runnable_Demographic_Generation(mapSeed,
//								loadedProperties);
//						run_gen_dem.run();
//					}
//					loadedProperties.put(Simulation_Gen_MetaPop.PROP_LOC_MAP, shared_loc_map);
//					loadedProperties.put(Simulation_Gen_MetaPop.PROP_PRELOAD_FILES, true);
//					loadedProperties.put(Simulation_Gen_MetaPop.PROP_INDIV_STAT, new ConcurrentHashMap<Integer, int[]>());
//					loadedProperties.put(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT,
//							Collections.synchronizedList(new ArrayList<int[]>()));
//
//					// Run multiple population in parallel
//					runnable_genMap = new Runnable_ContactMap_Generation_Demographic(mapSeed, loadedProperties);
//				} else {
//					runnable_genMap = new Runnable_ContactMap_Generation_MetaPopulation(Long.parseLong(cMapStr),
//							loadedProperties);
//				}
//				if (numInExec_max <= 1) {
//					System.out.printf("Generating map with seed of %s.\n", cMapStr);
//					runnable_genMap.run();
//				} else {
//					System.out.printf("Parallel: Generating map with seed of %s.\n", cMapStr);
//					if (exec == null) {
//						exec = Executors.newFixedThreadPool(numInExec_max);
//					}
//					exec.submit(runnable_genMap);
//					numInExec++;
//
//					if (numInExec == numInExec_max) {
//						exec.shutdown();
//						if (!exec.awaitTermination(2, TimeUnit.DAYS)) {
//							System.err.println("Thread time-out!");
//						}
//						exec = null;
//					}
//				}
//
//			}
//
//			if (exec != null) {
//				if (!exec.awaitTermination(2, TimeUnit.DAYS)) {
//					System.err.println("Thread time-out!");
//				}
//
//			}
//
//		} else {
//			//Simulation_ClusterModelTransmission.launch(args, new Simulation_MetaPop());
//		}
//	}

	
}
