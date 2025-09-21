package sim;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import person.AbstractIndividualInterface;

public class Runnable_MetaPopulation_Transmission_Single_Site_Four_Inf<E>
		extends Runnable_ClusterModel_MultiTransmission {

	public static final int SITE_VAGINA = 0;
	public static final int SITE_PENIS = SITE_VAGINA + 1;
	public static final int SITE_ANY = SITE_PENIS + 1;
	// Initialise during initialse
	protected final File dir_demographic;
	protected int lastIndivdualUpdateTime = 0;

	// Pop stat will be the stat prior to simulation
	protected HashMap<Integer, int[]> indiv_map = new HashMap<>();
	protected static final int INDIV_MAP_CURRENT_GRP = 0;	
	protected static final int INDIV_MAP_ENTER_POP_AGE = INDIV_MAP_CURRENT_GRP + 1;
	protected static final int INDIV_MAP_ENTER_POP_AT = INDIV_MAP_ENTER_POP_AGE + 1;
	protected static final int INDIV_MAP_EXIT_POP_AT = INDIV_MAP_ENTER_POP_AT + 1;
	protected static final int INDIV_MAP_ENTER_GRP = INDIV_MAP_EXIT_POP_AT+1;
	protected static final int LENGTH_INDIV_MAP = INDIV_MAP_ENTER_GRP + 1;

	// Key = Grp
	protected HashMap<Integer, ArrayList<Integer>> current_pids_by_gps = new HashMap<>();
	protected int[][] grp_age_range = null;

	// Key = Time, V = ArrayList<int[]{pid,grp_from,grp_to}>
	protected HashMap<Integer, ArrayList<int[]>> schdule_grp_change = new HashMap<>();
	private static final Comparator<int[]> comparator_grp_change = new Comparator<int[]>() {
		@Override
		public int compare(int[] o1, int[] o2) {
			int res = 0;
			for (int i = 0; i < Math.min(o1.length, o2.length) && res == 0; i++) {
				res = Integer.compare(o1[i], o2[i]);
			}
			return res;
		}
	};
	protected static final int SCH_GRP_PID = 0;
	protected static final int SCH_GRP_FROM = SCH_GRP_PID + 1;
	protected static final int SCH_GRP_TO = SCH_GRP_FROM + 1;

	// Key = Time
	protected HashMap<Integer, int[][]> seed_infection = new HashMap<>();		
	
	// Default parameter
	private static final int NUM_INF = 4;
	private static final int NUM_SITE = 3;
	private static final int NUM_ACT = 1;
	private static final int NUM_GRP_PER_GENDER = 3;
	

	public Runnable_MetaPopulation_Transmission_Single_Site_Four_Inf(long cMap_seed, long sim_seed, Properties prop) {
		super(cMap_seed, sim_seed, null, prop, NUM_INF, NUM_SITE, NUM_ACT);
		this.setBaseDir((File) prop.get(Simulation_MetaPop.PROP_BASEDIR));
		this.setBaseProp(prop);
		this.dir_demographic = new File(baseProp.getProperty(Simulation_MetaPop.PROP_CONTACT_MAP_LOC));
	}

	@Override
	public void initialse() {
		super.initialse();
		// Set up age range
		init_age_range();
		// Set up individual map (i.e. population)
		init_indiv_map();
	}

	@Override
	public Integer[] getCurrentPopulationPId(int time) {
		updateIndivMap(time);
		ArrayList<Integer> pids = new ArrayList<>();
		for (ArrayList<Integer> pid_by_grp : current_pids_by_gps.values()) {
			pids.addAll(pid_by_grp);
		}
		return pids.toArray(new Integer[0]);
	}

	
	@Override
	public int getPersonGrp(Integer personId) {
		int[] indiv_stat = indiv_map.get(personId);
		int grp = indiv_stat[INDIV_MAP_CURRENT_GRP];		
		if(grp < 0) { // Expired person			
			grp = ((indiv_stat[INDIV_MAP_ENTER_GRP] / NUM_GRP_PER_GENDER)+ 1) * NUM_GRP_PER_GENDER -1;
		}	
		return grp;
	}
	@Override
	protected void handleRemovePerson(Integer pid) {
		// Do nothing		
	}		
	
	
	@Override
	protected void postSimulation() {
		// TODO Auto-generated method stub
		super.postSimulation();
	}

	

	private void updateIndivMap(int time) {
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
						grpPids = current_pids_by_gps.get(indiv_grp_change[SCH_GRP_FROM]);
						grpPids.remove(Collections.binarySearch(grpPids, pid));
					}
					if (indiv_grp_change[SCH_GRP_TO] != -1) {
						grpPids = current_pids_by_gps.get(indiv_grp_change[SCH_GRP_TO]);
						grpPids.add(~Collections.binarySearch(grpPids, pid), pid);
					}

				}
			}
			lastIndivdualUpdateTime++;
		}
	}

	/*
	 * Setup indiv_map, current_pids_by_gps and current_pids_by_gps
	 */
	private void init_indiv_map() {
		for (Entry<Integer, String[]> ent : getPop_stat().entrySet()) {
			int[] indiv_stat = new int[LENGTH_INDIV_MAP];
			// Load default entry from pop_stat
			for (int i = 0; i < ent.getValue().length; i++) {
				indiv_stat[i] = Integer.parseInt(ent.getValue()[i]);
			}			
			indiv_stat[INDIV_MAP_ENTER_GRP] = indiv_stat[INDIV_MAP_CURRENT_GRP];
			indiv_map.put(ent.getKey(), indiv_stat);
			Integer pid = ent.getKey();

			// ENTER_POP_AT : Fill current_pids_by_gps and schdule_grp_change for
			if (indiv_stat[INDIV_MAP_ENTER_POP_AT] == lastIndivdualUpdateTime) {
				ArrayList<Integer> grpPids = current_pids_by_gps.get(indiv_stat[INDIV_MAP_CURRENT_GRP]);
				if (grpPids == null) {
					grpPids = new ArrayList<>();
					current_pids_by_gps.put(indiv_stat[INDIV_MAP_CURRENT_GRP], grpPids);
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

				if (indiv_stat[INDIV_MAP_EXIT_POP_AT] > 0 && indiv_stat[INDIV_MAP_EXIT_POP_AT] < changeTime) {
					changeTime = indiv_stat[INDIV_MAP_EXIT_POP_AT];
					reach_max_age_grp = true;
				}

				if ((checkGrp % NUM_GRP_PER_GENDER) + 1 < NUM_GRP_PER_GENDER) {
					addEntryToSchduleGrpChange(changeTime, new int[] { pid, checkGrp, checkGrp + 1 });
				} else {
					addEntryToSchduleGrpChange(changeTime, new int[] { pid, checkGrp, -1 });
					reach_max_age_grp = true;
				}
				checkGrp++;
			}
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

	private void init_age_range() {
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

}
