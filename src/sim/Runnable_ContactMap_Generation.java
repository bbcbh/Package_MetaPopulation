package sim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import map.Map_Location_Mobility;
import person.AbstractIndividualInterface;

public class Runnable_ContactMap_Generation implements Runnable {

	private HashMap<String, Object> loadedProperties;
	private Map_Location_Mobility loc_map;
	private long mapSeed;
	public static final String FILENAME_FORMAT_CMAP_BY_POP = "ContactMap_Pop_%d_%d.csv"; // POP_ID , SEED
	public static final String POP_TYPE = "MetaPop_By_Location_Mobility";

	private static final String FILE_HEADER_EXTRA_PARTNER_SOUGHT = "TIME_FROM,PID,EXTRA_PARTNER_SOUGHT";
	public static final String FILENAME_FORMAT_EXTRA_PARTNER_SOUGHT = "Extra_partner_sought_%d.csv"; // SEED
	public static final String FILENAME_FORMAT_POP_INDEX_MAP = "PopIndex_Mapping_%d.csv"; // SEED

	public Runnable_ContactMap_Generation(long mapSeed, HashMap<String, Object> loadedProperties) {
		this.mapSeed = mapSeed;
		this.loadedProperties = loadedProperties;
		try {
			this.loc_map = (Map_Location_Mobility) loadedProperties.get(Simulation_Gen_MetaPop.PROP_LOC_MAP);
		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}

		// Preload thread-safe HashMap and ArrayList
//		this.loadedProperties.put(Simulation_Gen_MetaPop.PROP_INDIV_STAT, new ConcurrentHashMap<Integer, int[]>());
//		this.loadedProperties.put(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT,
//				Collections.synchronizedList(new ArrayList<int[]>()));

		this.loadedProperties.put(Simulation_Gen_MetaPop.PROP_INDIV_STAT, new HashMap<Integer, int[]>());
		this.loadedProperties.put(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT, new ArrayList<int[]>());

		// Move demographic file to associated folder (for backward compatibility)

		File baseDir = new File((String) loadedProperties.get(Simulation_Gen_MetaPop.PROP_BASEDIR));
		File demogrpahic_dir = new File(baseDir,
				String.format(Simulation_Gen_MetaPop.DIR_NAME_FORMAT_DEMOGRAPHIC, mapSeed));
		if (!demogrpahic_dir.exists()) {
			demogrpahic_dir.mkdirs();
		}
		File[] demographic_files = baseDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().endsWith(String.format("%d.csv", mapSeed));
			}
		});

		if (demographic_files.length != 0) {
			int counter = 0;
			for (File mv_file : demographic_files) {
				try {
					Files.move(mv_file.toPath(), new File(demogrpahic_dir, mv_file.getName()).toPath(),
							StandardCopyOption.ATOMIC_MOVE);
					counter++;
				} catch (IOException e) {
					e.printStackTrace(System.err);
				}
			}
			System.out.printf("%d out of %d demographic files for cMap_seed=%d moved successfully.\n", counter,
					demographic_files.length, mapSeed);
		}

	}

	protected HashMap<String, Object> getLoadedProperties() {
		return loadedProperties;
	}

	protected long getMapSeed() {
		return mapSeed;
	}

	protected Map_Location_Mobility getLoc_map() {
		return loc_map;
	}

	@Override
	public void run() {
		Integer[] pop_ids = loc_map.getNode_info().keySet().toArray(new Integer[0]);
		Arrays.sort(pop_ids);

		long tic = System.currentTimeMillis();

		StepWiseOperation_ContactMap_Generation_Demographic[] steps = new StepWiseOperation_ContactMap_Generation_Demographic[pop_ids.length];
		for (int i = 0; i < pop_ids.length; i++) {
			int pop_id = pop_ids[i];
			steps[i] = new StepWiseOperation_ContactMap_Generation_Demographic(mapSeed, pop_id, loadedProperties);
		}

		int currentTime = 0;
		int max_time = Integer.parseInt(
				(String) loadedProperties.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SNAP]))
				* Integer.parseInt((String) loadedProperties
						.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));

		// Extra year round up
		max_time = (max_time / AbstractIndividualInterface.ONE_YEAR_INT + 1)  * AbstractIndividualInterface.ONE_YEAR_INT + 1;
		
		while (currentTime < max_time) {
			for (int i = 0; i < pop_ids.length; i++) {
				steps[i].updateDemogrpahic();
			}
			for (int i = 0; i < pop_ids.length; i++) {
				steps[i].updateMovement();
			}
			for (int i = 0; i < pop_ids.length; i++) {
				steps[i].advanceTimeStep();
			}

			if (currentTime % AbstractIndividualInterface.ONE_YEAR_INT == 0) {
				for (int i = 0; i < pop_ids.length; i++) {
					steps[i].printContactMap(currentTime);
				}
				System.out.printf("Contact map generation for cMap=%d up to t=%d. Time elapsed = %.3fs\n", mapSeed,
						currentTime, (System.currentTimeMillis() - tic) / 1000.0);
			}
			currentTime++;

		}
		for (int i = 0; i < pop_ids.length; i++) {
			steps[i].finalise(currentTime);
		}

		@SuppressWarnings("unchecked")
		List<int[]> extraPartner_record = (List<int[]>) loadedProperties
				.get(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT);
		if (extraPartner_record != null) {
			extraPartner_record.sort(new Comparator<int[]>() {
				@Override
				public int compare(int[] o1, int[] o2) {
					int res = 0;
					for (int i = 0; i < o1.length && res == 0; i++) {
						res = Integer.compare(o1[i], o2[i]);
					}
					return res;
				}
			});
			File baseDir = new File((String) loadedProperties.get(Simulation_Gen_MetaPop.PROP_BASEDIR));
			File demogrpahic_dir = new File(baseDir,
					String.format(Simulation_Gen_MetaPop.DIR_NAME_FORMAT_DEMOGRAPHIC, mapSeed));
			demogrpahic_dir.mkdirs();

			try {
				PrintWriter pWri = new PrintWriter(
						new File(demogrpahic_dir, String.format(FILENAME_FORMAT_EXTRA_PARTNER_SOUGHT, mapSeed)));
				pWri.println(FILE_HEADER_EXTRA_PARTNER_SOUGHT);
				for (int[] extra : extraPartner_record) {
					pWri.printf("%d,%d,%d\n", extra[0], extra[1], extra[2]);
				}
				pWri.close();

				covertDemographicFiles(demogrpahic_dir, demogrpahic_dir, mapSeed);
			} catch (IOException ex) {
				ex.printStackTrace(System.err);
			}

		}

		System.out.printf("Contact map generation for cMap=%d completed. Time required = %.3fs\n", mapSeed,
				(System.currentTimeMillis() - tic) / 1000.0);

	}

	public static void covertDemographicFiles(File baseDir, File tarDir, long seed) throws IOException {
		boolean renameOnly = baseDir.equals(tarDir);
		if (!tarDir.exists()) {
			tarDir.mkdirs();
		}
		Pattern pattern_demographic = Pattern.compile(Runnable_Demographic_Generation.FILENAME_FORMAT_DEMOGRAPHIC
				.replaceFirst("%d", "(\\\\d+)").replaceFirst("%d", Long.toString(seed)));
		Pattern pattern_cMap_by_pop = Pattern.compile(Runnable_ContactMap_Generation.FILENAME_FORMAT_CMAP_BY_POP
				.replaceFirst("%d", "(\\\\d+)").replaceFirst("%d", Long.toString(seed)));

		HashMap<String, Integer> popIdToIndex = new HashMap<>();
		int nextIndex = 0;

		File[] allDemographicFile = baseDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pattern_demographic.matcher(pathname.getName()).matches();
			}
		});

		Arrays.sort(allDemographicFile, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Matcher m1 = pattern_demographic.matcher(o1.getName());
				Matcher m2 = pattern_demographic.matcher(o2.getName());
				m1.find();
				m2.find();
				return Integer.compare(Integer.parseInt(m1.group(1)), Integer.parseInt(m2.group(1)));
			}
		});

		File popStat = new File(tarDir,
				String.format(Runnable_ClusterModel_ContactMap_Generation_MultiMap.POPSTAT_FORMAT, seed));
		PrintWriter pWri = new PrintWriter(popStat);

		pWri.println("ID,GRP,ENTER_POP_AGE,ENTER_POP_AT,EXIT_POP_AT,HOME_LOC");

		for (File f : allDemographicFile) {
			Matcher m = pattern_demographic.matcher(f.getName());
			m.find();
			if (!popIdToIndex.containsKey(m.group(1))) {
				popIdToIndex.put(m.group(1), nextIndex);
				nextIndex++;
			}
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				// ENT: 0:PID, 1:ENTER_AT, 2:EXIT_AT, 3:ENTER_AGE, 4:ENTER_GRP
				String[] ent = line.split(",");
				pWri.printf("%s,%s,%s,%s,%s,%s\n", //
						ent[0], // ID
						ent[4], // GRP
						ent[3], // ENTER_POP_AGE
						ent[1], // ENTER_POP_AT
						ent[2], // EXIT_POP_AT
						m.group(1) // HOME_LOC
				);
			}

			reader.close();
		}
		pWri.close();

		File popIndexFile = new File(tarDir, String.format(FILENAME_FORMAT_POP_INDEX_MAP, seed));
		pWri = new PrintWriter(popIndexFile);
		pWri.println("POP_INDEX,POP_ID");
		for (Entry<String, Integer> ent : popIdToIndex.entrySet()) {
			pWri.printf("%d,%s\n", ent.getValue(), ent.getKey());
		}
		pWri.close();

		File[] allValidFiles = baseDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return !pathname.getName().endsWith("csv")
						|| (pathname.getName().endsWith(String.format("%d.csv", seed))
								&& !pattern_demographic.matcher(pathname.getName()).matches());
			}
		});
		for (File f : allValidFiles) {
			Matcher m;
			File newFile;
			// CMAP
			m = pattern_cMap_by_pop.matcher(f.getName());
			if (m.matches()) {
				newFile = new File(tarDir,
						String.format(Runnable_ClusterModel_ContactMap_Generation_MultiMap.MAPFILE_FORMAT,
								popIdToIndex.get(m.group(1)), seed));
				if (renameOnly) {
					f.renameTo(newFile);
				} else {
					Files.copy(f.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			} else {
				if (!renameOnly) {
					newFile = new File(tarDir, f.getName());
					Files.copy(f.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}

	}

}
