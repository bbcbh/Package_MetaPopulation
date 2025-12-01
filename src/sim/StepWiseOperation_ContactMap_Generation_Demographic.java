package sim;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;

import person.AbstractIndividualInterface;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;
import util.LineCollectionEntry;

/**
 * For generation of demographic and mobility
 */

public class StepWiseOperation_ContactMap_Generation_Demographic {

	// 2: RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH
	public static final int RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH = 2;
	// 3: RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST
	public static final int RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST = RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH
			+ 1;
	// 4: RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_BY_SNAP
	public static final int RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_BY_SNAP = RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST
			+ 1;
	// 5: RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING
	public static final int RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING = RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_BY_SNAP
			+ 1;

	protected boolean preloadLines = false;
	protected LineCollectionEntry lines_demographic;
	protected long mapSeed;
	protected int popId;
	protected HashMap<String, Object> loadedProperties;

	protected ArrayList<LineCollectionEntry> lines_outflow = new ArrayList<>();
	protected ArrayList<LineCollectionEntry> lines_inflow = new ArrayList<>();
	protected int currentTime;

	// Key = PID, V =
	// ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_AGP,ACTIVIY_GRP,NUM_PARTNER_SEEK,
	// PARTNER_RECORD
	protected AbstractMap<Integer, int[]> map_indiv;

	private static final int INDEX_MAP_INDIV_ENTER_AT = 0;
	private static final int INDEX_MAP_INDIV_EXIT_AT = INDEX_MAP_INDIV_ENTER_AT + 1;
	private static final int INDEX_MAP_INDIV_ENTER_AGE = INDEX_MAP_INDIV_EXIT_AT + 1;
	private static final int INDEX_MAP_INDIV_ENTER_GRP = INDEX_MAP_INDIV_ENTER_AGE + 1;
	private static final int INDEX_MAP_INDIV_ENTER_LOC = INDEX_MAP_INDIV_ENTER_GRP + 1;
	private static final int INDEX_MAP_INDIV_CURRENT_GRP = INDEX_MAP_INDIV_ENTER_LOC + 1;
	private static final int INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT = INDEX_MAP_INDIV_CURRENT_GRP + 1;
	private static final int INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL = INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT + 1;
	private static final int INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK = INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL + 1;
	private static final int INDEX_MAP_INDIV_WINDOW_REFRESH_AT = INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK + 1;
	private static final int LENGTH_INDEX_MAP_INDIV = INDEX_MAP_INDIV_WINDOW_REFRESH_AT + 1;

	private List<int[]> extraPartner_record;
	private File baseDir;
	private RandomGenerator RNG;
	private int[][] mat_age_range;
	private int max_age_range = 0;

	private double[][] partnership_by_snap;
	private static final int PARTNERSHIP_REG_DUR_MEAN = 0;
	private static final int PARTNERSHIP_REG_DUR_SD = PARTNERSHIP_REG_DUR_MEAN + 1;
	private static final int PARTNERSHIP_PROB_START = PARTNERSHIP_REG_DUR_SD + 1;

	private double[][] mat_mixing;
	private static final int MIXING_GRP_NUM = 0;
	private static final int MIXING_PROB_REG = MIXING_GRP_NUM + 1;
	private static final int MIXING_PROB_START = MIXING_PROB_REG + 1; // Use group_incl format if < 0

	private AbstractRealDistribution[] rel_duration_dist;

	private ArrayList<int[]> pairing = new ArrayList<>();

	// Key = Grp
	private HashMap<Integer, int[]> lookup_group_age_range = new HashMap<>();
	// Key = Grp, ArrayList<> pids)
	private HashMap<Integer, ArrayList<Integer>> map_in_population_by_grp = new HashMap<>();
	private HashMap<Integer, ArrayList<Integer>> map_available_by_grp = new HashMap<>();

	private Pattern pattern_filename_movement = Pattern.compile(Runnable_Demographic_Generation.FILENAME_FORMAT_MOVEMENT
			.replaceAll("%s", "(\\d+)_(\\d+)").replaceAll("%d", "(-?\\d+)"));

	private Comparator<LineCollectionEntry> movementLineCollectionCmp = new Comparator<LineCollectionEntry>() {
		@Override
		public int compare(LineCollectionEntry o1, LineCollectionEntry o2) {
			Matcher m1 = pattern_filename_movement.matcher(o1.getCsv().getName());
			Matcher m2 = pattern_filename_movement.matcher(o2.getCsv().getName());
			int res = 0;
			if (m1.matches() && m2.matches()) {
				int grp = 1;
				while (res == 0 && grp <= Math.min(m1.groupCount(), m2.groupCount())) {
					res = Long.compare(Long.parseLong(m1.group(grp)), Long.parseLong(m2.group(grp)));
					grp++;
				}
			}
			return res;
		}
	};

	@SuppressWarnings("unchecked")
	public StepWiseOperation_ContactMap_Generation_Demographic(long mapSeed, int popId,
			HashMap<String, Object> loadedProperties) {
		this.mapSeed = mapSeed;
		this.popId = popId;
		this.loadedProperties = loadedProperties;
		this.RNG = new MersenneTwisterRandomGenerator(mapSeed);
		try {
			this.baseDir = new File((String) loadedProperties.get(Simulation_Gen_MetaPop.PROP_BASEDIR));
			this.map_indiv = (AbstractMap<Integer, int[]>) loadedProperties.get(Simulation_Gen_MetaPop.PROP_INDIV_STAT);
			this.extraPartner_record = (List<int[]>) loadedProperties
					.get(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT);

		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
		if (loadedProperties.containsKey(Simulation_Gen_MetaPop.PROP_PRELOAD_FILES)) {
			preloadLines = (Boolean) loadedProperties.get(Simulation_Gen_MetaPop.PROP_PRELOAD_FILES);
		}
		pattern_filename_movement = Pattern.compile(
				String.format(Runnable_Demographic_Generation.FILENAME_FORMAT_MOVEMENT, "(\\d+)_(\\d+)", mapSeed));

		File demogrpahic_dir = new File(baseDir,
				String.format(Simulation_Gen_MetaPop.DIR_NAME_FORMAT_DEMOGRAPHIC, mapSeed));

		// Load line collections
		lines_demographic = null;
		lines_outflow = new ArrayList<>();
		lines_inflow = new ArrayList<>();

		File file_res = new File(demogrpahic_dir,
				String.format(Runnable_Demographic_Generation.FILENAME_FORMAT_DEMOGRAPHIC, popId, mapSeed));

		File[] flow_files = demogrpahic_dir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				Matcher m = pattern_filename_movement.matcher(pathname.getName());
				if (m.find()) {
					return Integer.parseInt(m.group(1)) == popId || Integer.parseInt(m.group(2)) == popId;
				}
				return false;
			}
		});

		try {
			lines_demographic = new LineCollectionEntry(file_res, preloadLines);
			lines_demographic.loadNextLine(); // Header
			lines_demographic.loadNextLine();

			for (File flow_file : flow_files) {
				LineCollectionEntry ent = new LineCollectionEntry(flow_file, preloadLines);
				ent.loadNextLine(); // Header
				ent.loadNextLine();
				Matcher m = pattern_filename_movement.matcher(flow_file.getName());
				m.find();
				if (Integer.parseInt(m.group(1)) == popId) {
					lines_outflow.add(ent);
				} else {
					lines_inflow.add(ent);
				}
			}
			lines_outflow.sort(movementLineCollectionCmp);
			lines_inflow.sort(movementLineCollectionCmp);

		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(-1);
		}

		// POP_PROP_INIT_PREFIX_3
		mat_age_range = (int[][]) util.PropValUtils.propStrToObject((String) loadedProperties
				.get(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
						RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST)),
				int[][].class);
		for (int[] age_range : mat_age_range) {
			max_age_range = Math.max(max_age_range, age_range[age_range.length - 1]);
		}

		// POP_PROP_INIT_PREFIX_4
		partnership_by_snap = (double[][]) util.PropValUtils
				.propStrToObject(
						(String) loadedProperties
								.get(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
										RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_BY_SNAP)),
						double[][].class);

		rel_duration_dist = new AbstractRealDistribution[partnership_by_snap.length];
		for (int g = 0; g < rel_duration_dist.length; g++) {
			double[] row_sel = partnership_by_snap[g];
			// duration_mean, duration_sd, Probability_0.., Min_Partner_0...,
			// Max_Partner_0,...}
			// scale = var / mean
			double scale = row_sel[PARTNERSHIP_REG_DUR_SD] * row_sel[PARTNERSHIP_REG_DUR_SD]
					/ row_sel[PARTNERSHIP_REG_DUR_MEAN];
			// shape = mean / scale i.e. mean / (var / mean)
			double shape = row_sel[PARTNERSHIP_REG_DUR_MEAN] / scale;
			rel_duration_dist[g] = new GammaDistribution(RNG, shape, scale);
		}

		// POP_PROP_INIT_PREFIX_5
		mat_mixing = (double[][]) util.PropValUtils
				.propStrToObject(
						(String) loadedProperties
								.get(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
										RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING)),
						double[][].class);

		currentTime = 0;
		loadCurrentDemographic();
	}

	public void updateDemogrpahic() {
		// Add new person from demographic
		loadCurrentDemographic();
	}

	public void updateMovement() {
		// Load movement
		loadMovement(lines_inflow);
		loadMovement(lines_outflow);
	}

	public void advanceTimeStep() {
		map_available_by_grp.clear();

		Integer[] grpIds = map_in_population_by_grp.keySet().toArray(new Integer[0]);
		for (Integer grpId : grpIds) {
			// Update group (if need)
			ArrayList<Integer> grpPids = map_in_population_by_grp.get(grpId);
			for (Integer pid : grpPids.toArray(new Integer[0])) {
				int[] indiv_ent = map_indiv.get(pid);
				if (currentTime >= indiv_ent[INDEX_MAP_INDIV_EXIT_AT] && indiv_ent[INDEX_MAP_INDIV_EXIT_AT] >= 0) {
					grpPids.remove(Collections.binarySearch(grpPids, pid));
				} else {
					int[] age_range = lookup_group_age_range.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
					if (age_range == null) {
						for (int[] test_range : mat_age_range) {
							if (test_range.length == 3 && test_range[0] == indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]) {
								age_range = Arrays.copyOfRange(test_range, 1, 3);
								break;
							} else if (indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] % (-test_range[0]) == test_range[1]) {
								age_range = Arrays.copyOfRange(test_range, 2, 4);
								break;
							}
						}
						lookup_group_age_range.put(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP], age_range);
					}
					double age = indiv_ent[INDEX_MAP_INDIV_ENTER_AGE] + currentTime
							- indiv_ent[INDEX_MAP_INDIV_ENTER_AT];
					if (age >= age_range[1]) {
						grpPids.remove(Collections.binarySearch(grpPids, pid));
						if (age < max_age_range) {
							indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] += 1; // Next age group
							indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT] = 0;// Force reset
							ArrayList<Integer> grpPids_add = map_in_population_by_grp
									.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
							updatePartnerSeekingActivity(pid, indiv_ent);
							grpPids_add.add(~Collections.binarySearch(grpPids_add, pid), pid);
						} else {
							System.err.printf("Warning! Age of for loc=%d:pid=%d excced MAX_AGE=%d at t=%d\n", 
									popId, pid,	max_age_range, currentTime);

						}

					}
				}
			}
			for (Integer pid : map_in_population_by_grp.get(grpId)) {
				int[] indiv_ent = map_indiv.get(pid);
				if (indiv_ent[INDEX_MAP_INDIV_WINDOW_REFRESH_AT] == currentTime) {
					updatePartnerSeekingActivity(pid, indiv_ent);
				}
				// Add to map_available_by_grp
				if (indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK] > 0) {
					int window_length = indiv_ent[INDEX_MAP_INDIV_WINDOW_REFRESH_AT] - currentTime;
					int p_seekPartnerToday = RNG.nextInt(Math.max(1, window_length));
					if (p_seekPartnerToday < indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK]) {
						ArrayList<Integer> availPids = map_available_by_grp.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
						if (availPids == null) {
							availPids = new ArrayList<>();
							map_available_by_grp.put(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP], availPids);
						}
						availPids.add(~Collections.binarySearch(availPids, pid), pid);
					}
				}
			}
		}

		// Form partnership based on map_available_by_grp
		for (double[] mixing : mat_mixing) {
			int grpNum = (int) mixing[MIXING_GRP_NUM];
			Integer[] seekerPids = new Integer[0];

			if (map_available_by_grp.get(grpNum) != null) {
				seekerPids = map_available_by_grp.get(grpNum).toArray(seekerPids);
				util.ArrayUtilsRandomGenerator.shuffleArray(seekerPids, RNG);
			}

			for (int seekerPid : seekerPids) {

				int grp_sought;
				if (mixing[MIXING_PROB_START] < 0) {
					// Format for mixing:
					// [age_grp_num_seek, prob_reg, -num_grp_incl, age_grp_incl_sought_0, ...
					// age_grp_incl_sought_prob_cumul_0...]

					int num_grp_incl = -(int) mixing[MIXING_PROB_START];
					int mix_prob_start_grp_incl = MIXING_PROB_START + num_grp_incl + 1;

					double p = RNG.nextDouble();
					int pt;
					pt = Arrays.binarySearch(mixing, mix_prob_start_grp_incl, mixing.length, p);
					if (pt < 0) {
						pt = ~pt;
					}

					long grp_inc = (long) mixing[pt - num_grp_incl];
					if (grp_inc >= Long.MAX_VALUE) {
						System.err.printf("Warning! grp_inc = %d is greater than maximum value Long can handle."
								+ "Suggest direct reference to grp instead. \n", mixing[pt - num_grp_incl]);
						System.exit(1);
					}
					ArrayList<Integer> grp_inc_list = new ArrayList<>();
					ArrayList<Integer> grp_inc_count = new ArrayList<>();
					int cumul_count = 0;
					for (int g = 0; g < map_available_by_grp.size(); g++) {
						if ((grp_inc & (1 << g)) != 0) {
							if (map_available_by_grp.get(g) != null) {
								grp_inc_list.add(g);
								cumul_count += map_available_by_grp.get(g).size();
								grp_inc_count.add(cumul_count);
							}
						}
					}

					if (cumul_count > 0) {
						pt = Collections.binarySearch(grp_inc_count, RNG.nextInt(cumul_count));
						if (pt < 0) {
							pt = ~pt;
						}
						grp_sought = grp_inc_list.get(pt);
					} else {
						// No suitable group
						grp_sought = -1;

					}

				} else {
					// Format for mixing:
					// [age_grp_num_seek, prob_reg, grpNum_sought_prob_cumul_0,...]

					double p = RNG.nextDouble();
					grp_sought = Arrays.binarySearch(mixing, MIXING_PROB_START, mixing.length, p);
					if (grp_sought < 0) {
						grp_sought = ~grp_sought;
					}
					grp_sought -= MIXING_PROB_START;
				}
				if (map_available_by_grp.get(grp_sought) != null && map_available_by_grp.get(grp_sought).size() > 0) {
					int soughtPid = map_available_by_grp.get(grp_sought)
							.remove(RNG.nextInt(map_available_by_grp.get(grp_sought).size()));
					map_available_by_grp.get(grpNum)
							.remove(Collections.binarySearch(map_available_by_grp.get(grpNum), seekerPid));

					int dur = 1;
					int[] seek_stat = map_indiv.get(seekerPid);
					int[] sought_stat = map_indiv.get(soughtPid);

					seek_stat[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK]--;
					sought_stat[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK]--;

					// Set to regular partnership if both located at the same location
					if (seek_stat[INDEX_MAP_INDIV_ENTER_LOC] == sought_stat[INDEX_MAP_INDIV_ENTER_LOC]
							&& currentTime >= seek_stat[INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL]
							&& currentTime >= sought_stat[INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL]) {
						if (RNG.nextDouble() < mixing[MIXING_PROB_REG]) {
							dur = Math.max(dur, (int) Math.round(rel_duration_dist[grp_sought].sample()));
							seek_stat[INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL] = currentTime + dur;
							sought_stat[INDEX_MAP_INDIV_IN_REG_PARTNERSHIP_UTIL] = currentTime + dur;
						}
					}
					pairing.add(new int[] { seekerPid, soughtPid, currentTime, dur });
				}

			}
		}
		// Store pairing

		currentTime++;

	}

	protected void loadMovement(ArrayList<LineCollectionEntry> flow_arr) {
		for (LineCollectionEntry ent : flow_arr) {
			String line_flow = ent.getCurrentLine();
			while (line_flow != null) {
				// TIME,PID
				String[] str_ent = line_flow.split(",");
				if (Integer.parseInt(str_ent[0]) > currentTime) {
					break;
				} else {
					int pid = Integer.parseInt(str_ent[1]);
					int[] indiv_ent = map_indiv.get(pid);
					ArrayList<Integer> grpPids = map_in_population_by_grp.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);

					int pt = Collections.binarySearch(grpPids, pid);
					if (flow_arr == lines_outflow) {
						if (pt >= 0) {
							grpPids.remove(pt);
						} else {
							System.err.printf("Warning! Outflow for loc=%d:pid=%d not found at t=%d\n", popId, pid,
									currentTime);
						}
					} else {
						if (pt < 0) {
							grpPids.add(~pt, pid);
						} else {
							System.err.printf("Warning! Inflow for loc=%d:pid=%d not found at t=%d\n", popId, pid,
									currentTime);
						}
					}
				}
				ent.loadNextLine();
				line_flow = ent.getCurrentLine();
			}
		}
	}

	protected void loadCurrentDemographic() {
		// Load demographic (i.e. new person entered)
		String line_demo = lines_demographic.getCurrentLine();
		while (line_demo != null) {
			// PID,ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_GRP
			String[] str_ent = line_demo.split(",");
			if (Integer.parseInt(str_ent[INDEX_MAP_INDIV_ENTER_AT + 1]) > currentTime) {
				break;
			} else {
				int pid = Integer.parseInt(str_ent[0]);	
				int[] indiv_ent = map_indiv.get(pid);
				if (indiv_ent == null) {
					indiv_ent = new int[LENGTH_INDEX_MAP_INDIV];
					indiv_ent[INDEX_MAP_INDIV_ENTER_AT] = Integer.parseInt(str_ent[INDEX_MAP_INDIV_ENTER_AT + 1]);
					indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = Integer.parseInt(str_ent[INDEX_MAP_INDIV_EXIT_AT + 1]);
					indiv_ent[INDEX_MAP_INDIV_ENTER_AGE] = Integer.parseInt(str_ent[INDEX_MAP_INDIV_ENTER_AGE + 1]);
					indiv_ent[INDEX_MAP_INDIV_ENTER_GRP] = Integer.parseInt(str_ent[INDEX_MAP_INDIV_ENTER_GRP + 1]);
					indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] = popId;
					indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = indiv_ent[INDEX_MAP_INDIV_ENTER_GRP];
					updatePartnerSeekingActivity(pid, indiv_ent);
					map_indiv.put(pid, indiv_ent);

					// Update map_in_population_by_grp
					ArrayList<Integer> grpPids = map_in_population_by_grp.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
					if (grpPids == null) {
						grpPids = new ArrayList<>();
						map_in_population_by_grp.put(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP], grpPids);
					}
					grpPids.add(~Collections.binarySearch(grpPids, pid), pid);
				}
			}
			lines_demographic.loadNextLine();
			line_demo = lines_demographic.getCurrentLine();
		}
	}

	private void updatePartnerSeekingActivity(int pid, int[] indiv_ent) {
		if (indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK] > 0) {
			// "TIME_FROM,PID,EXTRA_PARTNER_SOUGHT"
			extraPartner_record.add(new int[] { Math.max(currentTime - AbstractIndividualInterface.ONE_YEAR_INT, 0),
					pid, indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK] });
		}

		double[] partnership_setting = partnership_by_snap[indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]];
		// {duration_mean, duration_sd, Cumul_Probability_0.., Min_Partner_0...,
		// Max_Partner_0, ...}
		int numOptions = (partnership_setting.length - PARTNERSHIP_PROB_START) / 3;
		if (indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT] == 0) {
			double pGrp = RNG.nextDouble();
			int pt = ~Arrays.binarySearch(partnership_setting, PARTNERSHIP_PROB_START,
					PARTNERSHIP_PROB_START + numOptions, pGrp);
			indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT] = pt;
		}

		indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK] = (int) partnership_setting[numOptions
				+ indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT]];

		if (partnership_setting[2 * numOptions + indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT]]
				- partnership_setting[numOptions + indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT]] > 0) {
			indiv_ent[INDEX_MAP_INDIV_NUM_PARTNERS_TO_SEEK] += RNG.nextInt(
					(int) (partnership_setting[2 * numOptions + indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT]]
							- partnership_setting[numOptions + indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT]]));
		}

		indiv_ent[INDEX_MAP_INDIV_WINDOW_REFRESH_AT] = currentTime + AbstractIndividualInterface.ONE_YEAR_INT;

	}

	public void finalise(int currentTime) {
		// Close file reader (if needed)
		if (lines_demographic != null) {
			try {
				lines_demographic.closeReader();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
		for (LineCollectionEntry ent : lines_inflow) {
			try {
				ent.closeReader();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
		for (LineCollectionEntry ent : lines_outflow) {
			try {
				ent.closeReader();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
		printContactMap(currentTime);
	}

	public void printContactMap(int currentTime) {
		// Write contact map

		File demogrpahic_dir = new File(baseDir,
				String.format(Simulation_Gen_MetaPop.DIR_NAME_FORMAT_DEMOGRAPHIC, mapSeed));

		File cMapFile = new File(demogrpahic_dir,
				String.format(Runnable_ContactMap_Generation.FILENAME_FORMAT_CMAP_BY_POP, popId, mapSeed));
		try {
			File[] backup_to_be_remove = new File[0];
			if (cMapFile.isFile()) {
				File resultBackup = new File(baseDir, "RESULT_BACKUP_CONTACT_MAP");
				resultBackup.mkdirs();
				backup_to_be_remove = resultBackup.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pathname.getName().endsWith(cMapFile.getName());
					}
				});

				Files.copy(cMapFile.toPath(),
						new File(resultBackup, String.format("P%d_%s", currentTime, cMapFile.getName())).toPath());

			}

			PrintWriter pWri = new PrintWriter(cMapFile);
			for (int[] partnership : pairing) {
				pWri.printf("%d,%d,%d,%d\n", partnership[0], partnership[1], partnership[2], partnership[3]);
			}
			pWri.close();

			for (File rembackup : backup_to_be_remove) {
				if (rembackup.exists()) {
					try {
						Files.delete(rembackup.toPath());
					} catch (IOException e) {
						e.printStackTrace(System.err);
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

}
