package sim;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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
	protected Properties loadedProperties;

	protected ArrayList<LineCollectionEntry> lines_outflow = new ArrayList<>();
	protected ArrayList<LineCollectionEntry> lines_inflow = new ArrayList<>();
	protected int currentTime;

	// Key = PID, V =
	// ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_AGP,ACTIVIY_GRP,NUM_PARTNER_SEEK,
	// PARTNER_RECORD
	protected ConcurrentHashMap<Integer, int[]> map_indiv;

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

	// Format:
	// double[grpNum][] = {duration_mean, duration_sd, Cumul_Probability_0..,
	// Min_Partner_0..., Max_Partner_0, ...}
	private double[][] partnership_by_snap;
	private static final int PARTNERSHIP_REG_DUR_MEAN = 0;
	private static final int PARTNERSHIP_REG_DUR_SD = PARTNERSHIP_REG_DUR_MEAN + 1;
	private static final int PARTNERSHIP_PROB_START = PARTNERSHIP_REG_DUR_SD + 1;

	// Format:
	// double[grpNum_seek, prob_reg, grpNum_sought_0,...] as cumulative prob
	private double[][] mat_mixing;
	private static final int MIXING_GRP_NUM = 0;
	private static final int MIXING_PROB_REG = MIXING_GRP_NUM + 1;
	private static final int MIXING_PROB_START = MIXING_PROB_REG + 1;

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
	public StepWiseOperation_ContactMap_Generation_Demographic(long mapSeed, int popId, Properties loadedProperties) {
		this.mapSeed = mapSeed;
		this.popId = popId;
		this.loadedProperties = loadedProperties;
		this.RNG = new MersenneTwisterRandomGenerator(mapSeed);
		try {
			this.baseDir = (File) loadedProperties.get(Simulation_Gen_MetaPop.PROP_BASEDIR);
			this.map_indiv = (ConcurrentHashMap<Integer, int[]>) loadedProperties
					.get(Simulation_Gen_MetaPop.PROP_INDIV_STAT);
			this.extraPartner_record = (List<int[]>) loadedProperties.get(Simulation_Gen_MetaPop.PROP_PARNTER_EXTRA_SOUGHT);

		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
		if (loadedProperties.containsKey(Simulation_Gen_MetaPop.PROP_PRELOAD_FILES)) {
			preloadLines = (Boolean) loadedProperties.get(Simulation_Gen_MetaPop.PROP_PRELOAD_FILES);
		}
		pattern_filename_movement = Pattern.compile(
				String.format(Runnable_Demographic_Generation.FILENAME_FORMAT_MOVEMENT, "(\\d+)_(\\d+)", mapSeed));

		// Load line collections
		lines_demographic = null;
		lines_outflow = new ArrayList<>();
		lines_inflow = new ArrayList<>();

		File file_res = new File(baseDir,
				String.format(Runnable_Demographic_Generation.FILENAME_FORMAT_DEMOGRAPHIC, popId, mapSeed));

		File[] flow_files = baseDir.listFiles(new FileFilter() {
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
		mat_age_range = (int[][]) util.PropValUtils.propStrToObject(loadedProperties
				.getProperty(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
						RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST)),
				int[][].class);

		// POP_PROP_INIT_PREFIX_4
		partnership_by_snap = (double[][]) util.PropValUtils
				.propStrToObject(
						loadedProperties.getProperty(
								String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
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
						loadedProperties.getProperty(
								String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
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
			// Update activity group (if need)
			ArrayList<Integer> grpPids = map_in_population_by_grp.get(grpId);
			for (Integer pid : grpPids.toArray(new Integer[0])) {
				int[] indiv_ent = map_indiv.get(pid);
				if (currentTime >= indiv_ent[INDEX_MAP_INDIV_EXIT_AT] && indiv_ent[INDEX_MAP_INDIV_EXIT_AT] > 0) {
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
						indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] += 1; // Next age group
						indiv_ent[INDEX_MAP_INDIV_CURRENT_ACTIVITY_POINT] = 0;// Force reset
						ArrayList<Integer> grpPids_add = map_in_population_by_grp
								.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
						updatePartnerSeekingActivity(pid, indiv_ent);
						grpPids_add.add(~Collections.binarySearch(grpPids_add, pid), pid);

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
				double p = RNG.nextDouble();
				int pt = Arrays.binarySearch(mixing, MIXING_PROB_START, mixing.length, p);
				if (pt < 0) {
					pt = ~pt;
				}
				pt -= MIXING_PROB_START;
				if (map_available_by_grp.get(pt) != null && map_available_by_grp.get(pt).size() > 0) {
					int soughtPid = map_available_by_grp.get(pt)
							.remove(RNG.nextInt(map_available_by_grp.get(pt).size()));
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
							dur = Math.max(dur, (int) Math.round(rel_duration_dist[pt].sample()));
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
					if (flow_arr == lines_outflow) {
						grpPids.remove(Collections.binarySearch(grpPids, pid));
					} else {
						grpPids.add(~Collections.binarySearch(grpPids, pid), pid);
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
		// Individual want to seek new partner by cannot do so

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

	public void finalise() {
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
		// Write contact map
		File cMapFile = new File(baseDir,
				String.format(Runnable_Demographic_Generation.FILENAME_FORMAT_CMAP_BY_POP, popId, mapSeed));
		try {
			PrintWriter pWri = new PrintWriter(cMapFile);
			for (int[] partnership : pairing) {
				pWri.printf("%d,%d,%d,%d\n", partnership[0], partnership[1], partnership[2], partnership[3]);
			}
			pWri.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

}
