package sim;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import person.AbstractIndividualInterface;
import relationship.ContactMap;
import util.LineCollectionEntry;

public class Runnable_MetaPopulation_MultiTransmission extends Runnable_ClusterModel_MultiTransmission {

	// Individual Mapping
	protected HashMap<Integer, int[]> indiv_map = new HashMap<>();
	public static final int INDIV_MAP_CURRENT_GRP = 0;
	public static final int INDIV_MAP_ENTER_POP_AGE = INDIV_MAP_CURRENT_GRP + 1;
	public static final int INDIV_MAP_ENTER_POP_AT = INDIV_MAP_ENTER_POP_AGE + 1;
	public static final int INDIV_MAP_EXIT_POP_AT = INDIV_MAP_ENTER_POP_AT + 1;
	public static final int INDIV_MAP_HOME_LOC = INDIV_MAP_EXIT_POP_AT + 1;
	public static final int INDIV_MAP_ENTER_GRP = INDIV_MAP_HOME_LOC + 1;
	public static final int INDIV_MAP_CURRENT_LOC = INDIV_MAP_ENTER_GRP + 1;
	public static final int LENGTH_INDIV_MAP = INDIV_MAP_CURRENT_LOC + 1;

	protected int lastIndivdualUpdateTime = 0;

	// Individual groupings
	protected int[][] grp_age_range = null;
	protected HashMap<Integer, ArrayList<Integer>> current_pids_by_grp = new HashMap<>();
	protected HashMap<Integer, ArrayList<int[]>> schdule_grp_change = new HashMap<>();
	
	
	protected static final int SCH_GRP_PID = 0;
	protected static final int SCH_GRP_FROM = SCH_GRP_PID + 1;
	protected static final int SCH_GRP_TO = SCH_GRP_FROM + 1;
	

	// Infection history
	protected HashMap<Integer, ArrayList<ArrayList<Integer>>> infection_history = new HashMap<>();
	public static final int INFECTION_HIST_CLEAR_NATURAL_RECOVERY = -1;
	public static final int INFECTION_HIST_CLEAR_TREATMENT = -2;
	public static final int INFECTION_HIST_OVERTREATMENT = -3;

	
	// Individual movement
	protected String[] location_name; // Pop_ID, Pop_Index
	// Key = LocationFrom_LocationTo
	protected HashMap<String, LineCollectionEntry> movementCollections = new HashMap<>();
	protected HashMap<Integer, ArrayList<Integer>> visitor_pids_by_loc = new HashMap<>();

	protected int lastMovement_update = -1;

	// Others
	protected static final String key_pop_size = "EXPORT_POP_SIZE";
	protected static final String FILENAME_EXPORT_POP_SIZE = "Pop_size_%d_%d.csv";

	protected final File dir_demographic;
	
	
	// Private fields
	private boolean preloadMovementFile = false;
	private final Comparator<int[]> comparator_grp_change = new Comparator<int[]>() {
		@Override
		public int compare(int[] o1, int[] o2) {
			int res = 0;
			for (int i = 0; i < Math.min(o1.length, o2.length) && res == 0; i++) {
				res = Integer.compare(o1[i], o2[i]);
			}
			return res;
		}
	};
	

	public Runnable_MetaPopulation_MultiTransmission(long cMap_seed, long sim_seed, Properties prop, int NUM_INF,
			int NUM_SITE, int NUM_ACT) {
		super(cMap_seed, sim_seed, null, prop, NUM_INF, NUM_SITE, NUM_ACT);
		this.setBaseDir((File) prop.get(Simulation_MetaPopulation.PROP_BASEDIR));
		this.setBaseProp(prop);

		this.dir_demographic = new File(
				new File(prop.getProperty(Simulation_ClusterModelTransmission.PROP_CONTACT_MAP_LOC)),
				String.format(Simulation_MetaPopulation.DIR_NAME_FORMAT_DEMOGRAPHIC, cMap_seed));

		try {
			String[] location_lines = util.Util_7Z_CSV_Entry_Extract_Callable
					.extracted_lines_from_text(new File(dir_demographic,
							String.format(Simulation_MetaPopulation.FILENAME_FORMAT_POP_INDEX_MAP, cMap_seed)));
			location_name = new String[location_lines.length];
			for (int i = 1; i < location_lines.length; i++) {
				String[] ent = location_lines[i].split(",");
				location_name[Integer.parseInt(ent[0])] = ent[1];
			}
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(-1);
		}

		Pattern pattern_movement_csv = Pattern.compile(Simulation_MetaPopulation.FILENAME_FORMAT_MOVEMENT
				.replaceAll("%d", Long.toString(cMap_seed)).replaceAll("%s", "(\\\\d+_\\\\d+)"));

		File[] movement_csvs = dir_demographic.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pattern_movement_csv.matcher(pathname.getName()).matches();
			}
		});
		for (File mv : movement_csvs) {
			try {
				Matcher m = pattern_movement_csv.matcher(mv.getName());
				m.matches();
				LineCollectionEntry ent = new LineCollectionEntry(mv, preloadMovementFile);
				ent.loadNextLine();
				ent.loadNextLine(); // Skip Header
				movementCollections.put(m.group(1), ent);
			} catch (IOException e) {
				e.printStackTrace(System.err);
				System.exit(-1);
			}
		}
	}

	@Override
	public void initialse() {
		super.initialse();
		// Set up age range
		init_age_range();
		// Set up individual mapping (i.e. population)
		init_indiv_map();

	}
	
	
	@Override
	protected int initaliseCMap(ContactMap cMap, Integer[][] edges_array, int edges_array_pt, int startTime,
			HashMap<Integer, ArrayList<Integer[]>> removeEdges) {
		int res = super.initaliseCMap(cMap, edges_array, edges_array_pt, startTime, removeEdges);		
		loadMovement(startTime);
		return res;
	}	
	

	@Override
	public void setPop_stat(HashMap<Integer, String[]> pop_stat_src) {
		// Custom pop_stat method - create a new pop_stat
		HashMap<Integer, String[]> pop_stat = new HashMap<>();
		for (Entry<Integer, String[]> ent : pop_stat_src.entrySet()) {
			pop_stat.put(ent.getKey(), Arrays.copyOf(ent.getValue(), ent.getValue().length));
		}
		super.setPop_stat(pop_stat);
	}

	@Override
	public Integer[] getCurrentPopulationPId(int time) {
		updateIndivMap(time);
		ArrayList<Integer> pids = new ArrayList<>();
		for (ArrayList<Integer> pid_by_grp : current_pids_by_grp.values()) {
			pids.addAll(pid_by_grp);
		}
		return pids.toArray(new Integer[0]);
	}

	@Override
	public int getPersonGrp(Integer personId) {
		int[] indiv_stat = indiv_map.get(personId);
		int grp = indiv_stat[INDIV_MAP_CURRENT_GRP];
		if (grp < 0) { // Expired person
			grp = ((indiv_stat[INDIV_MAP_ENTER_GRP] / (NUM_GRP / 2)) + 1) * (NUM_GRP / 2) - 1;
		}
		return grp;
	}

	protected void init_age_range() {
		grp_age_range = new int[NUM_GRP][];
		int[][] age_dist = (int[][]) util.PropValUtils.propStrToObject(baseProp.getProperty(String.format("%s%d",
				Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
				Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST)),
				int[][].class);
		for (int g = 0; g < grp_age_range.length; g++) {
			int[] age_range = null;
			for (int[] test_range : age_dist) {
				if (test_range.length == 3 && test_range[0] == g) {
					age_range = Arrays.copyOfRange(test_range, 1, 3);
					break;
				} else if (g % (-test_range[0]) == test_range[1]) {
					age_range = Arrays.copyOfRange(test_range, 2, 4);
					break;
				}
			}
			if (age_range == null) {
				age_range = new int[] { 15 * AbstractIndividualInterface.ONE_YEAR_INT,
						40 * AbstractIndividualInterface.ONE_YEAR_INT };
				System.err.printf("Warning! Age range for Grp #%d not defined." + " Default range %s is used.\n", g,
						Arrays.toString(age_range));
			}
			grp_age_range[g] = age_range;
		}
	}

	protected void init_indiv_map() {
		for (Entry<Integer, String[]> ent : getPop_stat().entrySet()) {
			int[] indiv_stat = new int[LENGTH_INDIV_MAP];
			// Load default entry from pop_stat
			for (int i = 0; i < ent.getValue().length; i++) {
				indiv_stat[i] = Integer.parseInt(ent.getValue()[i]);
			}
			indiv_stat[INDIV_MAP_ENTER_GRP] = indiv_stat[INDIV_MAP_CURRENT_GRP];
			indiv_stat[INDIV_MAP_CURRENT_LOC] = indiv_stat[INDIV_MAP_HOME_LOC];

			indiv_map.put(ent.getKey(), indiv_stat);
			Integer pid = ent.getKey();

			// ENTER_POP_AT : Fill current_pids_by_gps and schdule_grp_change for
			if (indiv_stat[INDIV_MAP_ENTER_POP_AT] == lastIndivdualUpdateTime) {
				ArrayList<Integer> grpPids = current_pids_by_grp.get(indiv_stat[INDIV_MAP_CURRENT_GRP]);
				if (grpPids == null) {
					grpPids = new ArrayList<>();
					current_pids_by_grp.put(indiv_stat[INDIV_MAP_CURRENT_GRP], grpPids);
				}
				grpPids.add(~Collections.binarySearch(grpPids, pid), pid);

			} else {
				addEntryToSchduleGrpChange(indiv_stat[INDIV_MAP_ENTER_POP_AT],
						new int[] { pid, -1, indiv_stat[INDIV_MAP_CURRENT_GRP] });
			}

			// Update grp change
			int checkGrp = indiv_stat[INDIV_MAP_CURRENT_GRP];
			boolean reach_max_age_grp = false;

			while (!reach_max_age_grp) {
				int changeAge = grp_age_range[checkGrp][1];
				int changeTime = changeAge - indiv_stat[INDIV_MAP_ENTER_POP_AGE] + indiv_stat[INDIV_MAP_ENTER_POP_AT];

				int nextGrp = checkGrp + 1;

				if (indiv_stat[INDIV_MAP_EXIT_POP_AT] > 0 && indiv_stat[INDIV_MAP_EXIT_POP_AT] < changeTime) {
					changeTime = indiv_stat[INDIV_MAP_EXIT_POP_AT];
					nextGrp = -1;
					reach_max_age_grp = true;
				}

				if ((checkGrp % (NUM_GRP / 2)) + 1 < (NUM_GRP / 2)) {
					addEntryToSchduleGrpChange(changeTime, new int[] { pid, checkGrp, nextGrp });
				} else {
					addEntryToSchduleGrpChange(changeTime, new int[] { pid, checkGrp, -1 });
					reach_max_age_grp = true;
				}
				checkGrp++;
			}

			// Update riskGrp (not used)
			risk_cat_map.put(pid, 0);

		}
	}

	private void addEntryToSchduleGrpChange(int schTime, int[] schEntry) {
		ArrayList<int[]> grpChange;
		grpChange = schdule_grp_change.get(schTime);
		if (grpChange == null) {
			grpChange = new ArrayList<>();
			schdule_grp_change.put(schTime, grpChange);
		}
		grpChange.add(~Collections.binarySearch(grpChange, schEntry, comparator_grp_change), schEntry);
	}

	protected void updateIndivMap(int time) {
		while (lastIndivdualUpdateTime <= time) {
			ArrayList<int[]> grpChange = schdule_grp_change.get(lastIndivdualUpdateTime);
			if (grpChange != null) {
				for (int[] indiv_grp_change : grpChange) {
					int pid = indiv_grp_change[SCH_GRP_PID];
					ArrayList<Integer> grpPids;
					int[] indiv_stat = indiv_map.get(pid);
					indiv_stat[INDIV_MAP_CURRENT_GRP] = indiv_grp_change[SCH_GRP_TO];
					pop_stat.get(pid)[POP_INDEX_GRP] = Integer.toString(indiv_stat[INDIV_MAP_CURRENT_GRP]);
					if (indiv_grp_change[SCH_GRP_FROM] != -1) {
						grpPids = current_pids_by_grp.get(indiv_grp_change[SCH_GRP_FROM]);
						grpPids.remove(Collections.binarySearch(grpPids, pid));
					}
					if (indiv_grp_change[SCH_GRP_TO] != -1) {
						grpPids = current_pids_by_grp.get(indiv_grp_change[SCH_GRP_TO]);
						grpPids.add(~Collections.binarySearch(grpPids, pid), pid);
					}

				}
			}
			lastIndivdualUpdateTime++;
		}
	}

	protected void loadMovement(int movementUpToTime) {
		while (lastMovement_update <= movementUpToTime) {
			for (Entry<String, LineCollectionEntry> mvE : movementCollections.entrySet()) {
				String[] direction = mvE.getKey().split("_");
				while ((mvE.getValue().getCurrentLine()) != null) {
					String[] ent = mvE.getValue().getCurrentLine().split(",");
					if (Integer.parseInt(ent[0]) == lastMovement_update) {
						int pid = Integer.parseInt(ent[1]);
						int[] indiv_stat = indiv_map.get(pid);
						int src_loc = Integer.parseInt(direction[0]);
						int tar_loc = Integer.parseInt(direction[1]);
						indiv_stat[INDIV_MAP_CURRENT_LOC] = tar_loc;
						if (tar_loc != indiv_stat[INDIV_MAP_HOME_LOC]) {
							// Moving away from home
							ArrayList<Integer> loc_arr = visitor_pids_by_loc.get(tar_loc);
							if (loc_arr == null) {
								loc_arr = new ArrayList<>();
								visitor_pids_by_loc.put(tar_loc, loc_arr);
							}
							loc_arr.add(~Collections.binarySearch(loc_arr, pid), pid);
						} else {
							// Returning home
							ArrayList<Integer> loc_arr = visitor_pids_by_loc.get(src_loc);
							loc_arr.remove(Collections.binarySearch(loc_arr, pid));
						}
						mvE.getValue().loadNextLine();
					} else {
						break;
					}
				}
			}
			lastMovement_update++;
		}
	}

	@Override
	protected double getTransmissionProb(int currentTime, int inf_id, int pid_inf_src, int pid_inf_tar,
			int partnershiptDur, int actType, int src_site, int tar_site) {
		if (indiv_map.get(pid_inf_src)[INDIV_MAP_CURRENT_LOC] != indiv_map.get(pid_inf_tar)[INDIV_MAP_CURRENT_LOC]) {
			// Only possible at same location
			return 0;
		} else {
			return super.getTransmissionProb(currentTime, inf_id, pid_inf_src, pid_inf_tar, partnershiptDur, actType,
					src_site, tar_site);
		}
	}

}
