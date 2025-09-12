package sim;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import map.Map_Location_IREG;
import person.AbstractIndividualInterface;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;

public class Runnable_Demographic_Generation implements Runnable {

	private Properties loadedProperties;
	private Map_Location_IREG loc_map;
	private File baseDir;
	private RandomGenerator RNG;
	private long mapSeed;

	public static final String FILENAME_FORMAT_DEMOGRAPHIC = "Demographic_%d_%d.csv"; // POP_ID, SEED
	public static final String FILENAME_FORMAT_MOVEMENT = "Movement_%d_%d.csv"; // POP_ID, SEED

	public static final String FILE_HEADER_DEMOGRAPHIC = "PID,ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_GRP";
	public static final String FILE_HEADER_MOVEMENT = "TIME,PID,LOC_FROM,LOC_TO";

	// K = PID, V = int[]{enter_pop_at, exit_pop_at, enter_pop_age, enter_grp,
	// home_location, current_grp}
	public static final int INDEX_MAP_INDIV_ENTER_AT = 0;
	public static final int INDEX_MAP_INDIV_EXIT_AT = INDEX_MAP_INDIV_ENTER_AT + 1;
	public static final int INDEX_MAP_INDIV_ENTER_AGE = INDEX_MAP_INDIV_EXIT_AT + 1;
	public static final int INDEX_MAP_INDIV_ENTER_GRP = INDEX_MAP_INDIV_EXIT_AT + 1;
	public static final int INDEX_MAP_INDIV_ENTER_LOC = INDEX_MAP_INDIV_ENTER_GRP + 1;
	public static final int INDEX_MAP_INDIV_CURRENT_GRP = INDEX_MAP_INDIV_ENTER_AGE + 1;
	public static final int LENGTH_MAP_INDIV = INDEX_MAP_INDIV_CURRENT_GRP + 1;

	// 1: RUNNABLE_FIELD_CONTACT_MAP_LOCATION_TOTAL_POP_SIZE
	// Total pop size
	public static final int RUNNABLE_FIELD_CONTACT_MAP_LOCATION_TOTAL_POP_SIZE = Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_INT_SETTING;

	// 2: RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH
	// Format: File path. Noted that it assumes node info will be
	// filename_NoteInfo.csv
	public static final int RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH = Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_NUMBER_OF_GRP;

	public Runnable_Demographic_Generation(long mapSeed, Properties loadedProperties) {
		this.loadedProperties = loadedProperties;
		this.mapSeed = mapSeed;
		this.RNG = new MersenneTwisterRandomGenerator(mapSeed);
		try {
			this.loc_map = (Map_Location_IREG) loadedProperties.get(Simulation_MetaPop.PROP_LOC_MAP);
			this.baseDir = (File) loadedProperties.get(Simulation_MetaPop.PROP_BASEDIR);
		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}

	}

	@Override
	public void run() {
		int nextId = 1;
		int currentTime = 0;

		int max_time = Integer.parseInt(
				loadedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SNAP]))
				* Integer.parseInt(loadedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));

		int[][] age_dist = (int[][]) util.PropValUtils.propStrToObject(loadedProperties.getProperty(String.format(
				"%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
				Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST)),
				int[][].class);

		// Key = POP_ID
		HashMap<Integer, PrintWriter> map_demographic_priWri = new HashMap<>();
		HashMap<Integer, PrintWriter> map_movement_priWri = new HashMap<>();
		HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> map_group_member_by_pop = new HashMap<>();
		// Key = PID
		HashMap<Integer, int[]> map_indiv = new HashMap<>();
		// Key = Group
		HashMap<Integer, int[]> map_group_age_range = new HashMap<>();

		HashMap<Integer, HashMap<String, Object>> node_info = loc_map.getNode_info();
		Integer[] pop_id_arr = node_info.keySet().toArray(new Integer[0]);
		Arrays.sort(pop_id_arr);

		int model_pop_size = Integer.parseInt(loadedProperties
				.getProperty(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
						RUNNABLE_FIELD_CONTACT_MAP_LOCATION_TOTAL_POP_SIZE)));
		float total_pop_size = 0;
		for (Integer pop_id : pop_id_arr) {
			int[] pop_size = (int[]) node_info.get(pop_id).get(Map_Location_IREG.NODE_INFO_POP_SIZE);
			for (int p : pop_size) {
				total_pop_size += p;
			}
		}

		// Initialise population

		int maxAge = 0;
		ArrayList<Integer> grpPids;
		for (Integer pop_id : pop_id_arr) {
			int[] pop_size = (int[]) node_info.get(pop_id).get(Map_Location_IREG.NODE_INFO_POP_SIZE);
			for (int g = 0; g < pop_size.length; g++) {
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

				map_group_age_range.put(g, age_range);
				maxAge = Math.max(maxAge, age_range[1]);

				int numPersonInGrp = 0;
				while (numPersonInGrp < Math.round(model_pop_size * pop_size[g] / total_pop_size)) {
					int[] indiv_ent = new int[LENGTH_MAP_INDIV];
					indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] = pop_id;
					indiv_ent[INDEX_MAP_INDIV_ENTER_AT] = currentTime;
					indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = -1;
					indiv_ent[INDEX_MAP_INDIV_ENTER_AGE] = age_range[0] + RNG.nextInt(age_range[1] - age_range[0]);
					indiv_ent[INDEX_MAP_INDIV_ENTER_GRP] = g;
					indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = g;
					map_indiv.put(nextId, indiv_ent);

					HashMap<Integer, ArrayList<Integer>> map_group_member = map_group_member_by_pop.get(pop_id);
					if (map_group_member == null) {
						map_group_member = new HashMap<>();
						map_group_member_by_pop.put(pop_id, map_group_member);
					}

					grpPids = map_group_member.get(g);
					if (grpPids == null) {
						grpPids = new ArrayList<>();
						map_group_member.put(g, grpPids);
					}
					grpPids.add(nextId);

					nextId++;
					numPersonInGrp++;
				}
			}
		}
		// Time step
		while (currentTime < max_time) {
			int pid = 1;
			while (pid < nextId) {
				int[] indiv_ent = map_indiv.get(pid);
				// Aging
				int currentAge = (currentTime - indiv_ent[INDEX_MAP_INDIV_ENTER_AT])
						+ indiv_ent[INDEX_MAP_INDIV_ENTER_AGE];
				if (currentAge >= maxAge) {
					indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = currentTime;
					grpPids = map_group_member_by_pop.get(indiv_ent[INDEX_MAP_INDIV_ENTER_LOC])
							.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
					grpPids.remove(Collections.binarySearch(grpPids, pid));
					indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = -1;
				} else {
					// Age to next group
					if (currentAge >= map_group_age_range.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP])[1]) {
						grpPids = map_group_member_by_pop.get(indiv_ent[INDEX_MAP_INDIV_ENTER_LOC])
								.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
						grpPids.remove(Collections.binarySearch(grpPids, pid));
						indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] += 1;
						grpPids = map_group_member_by_pop.get(indiv_ent[INDEX_MAP_INDIV_ENTER_LOC])
								.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
						grpPids.add(~Collections.binarySearch(grpPids, pid), pid);
					}
				}
				pid++;
			}

			// Add new individuals
			for (Integer pop_id : pop_id_arr) {
				int[] pop_size = (int[]) node_info.get(pop_id).get(Map_Location_IREG.NODE_INFO_POP_SIZE);
				Integer[] grpArray = map_group_member_by_pop.get(pop_id).keySet().toArray(new Integer[0]);
				Arrays.sort(grpArray);
				for (int g = 0; g < pop_size.length; g++) {
					int gSize = Math.round(model_pop_size * pop_size[g] / total_pop_size);
					grpPids = map_group_member_by_pop.get(pop_id).get(g);					
					while(grpPids.size() != gSize) {						
						if (grpPids.size() > gSize) {
							// Remove individuals from group							
							int remove_pid = grpPids.remove(RNG.nextInt(grpPids.size()));
							int[] indiv_ent = map_indiv.get(remove_pid);
							indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = currentTime;
							indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = -1;							
						} else if (grpPids.size() < gSize) {
							// Add individuals from group																				
							grpPids.add(~Collections.binarySearch(grpPids, nextId), nextId);							
							int[] indiv_ent = new int[LENGTH_MAP_INDIV];							
							indiv_ent[INDEX_MAP_INDIV_ENTER_AT] = currentTime;
							indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = -1;
							indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] = pop_id;
							indiv_ent[INDEX_MAP_INDIV_ENTER_AGE] = map_group_age_range.get(g)[0];
							indiv_ent[INDEX_MAP_INDIV_ENTER_GRP] = g;
							indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = g;
							map_indiv.put(nextId, indiv_ent);																																			
							nextId++;							
						}											
					}									
				}
			}

			// TODO: Movement

			currentTime++;
		}

		for (Entry<Integer, int[]> ent : map_indiv.entrySet()) {
			int homeLoc = ent.getValue()[INDEX_MAP_INDIV_ENTER_LOC];
			try {
				PrintWriter pWri = map_demographic_priWri.get(homeLoc);
				if (pWri == null) {
					pWri = new PrintWriter(new File(String.format(FILENAME_FORMAT_DEMOGRAPHIC, homeLoc, mapSeed)));
					map_demographic_priWri.put(homeLoc, pWri);
					pWri.println(FILE_HEADER_DEMOGRAPHIC);
				}
				pWri.printf("%d,%d,%d,%d,%d\n", ent.getKey(), ent.getValue()[INDEX_MAP_INDIV_ENTER_AT],
						ent.getValue()[INDEX_MAP_INDIV_EXIT_AT], ent.getValue()[INDEX_MAP_INDIV_ENTER_AGE],
						ent.getValue()[INDEX_MAP_INDIV_ENTER_GRP]);

			} catch (IOException e) {
				e.printStackTrace(System.err);
			}

		}

		// Close PrintWriters
		for (PrintWriter pWri : map_demographic_priWri.values()) {
			pWri.close();
		}
		for (PrintWriter pWri : map_movement_priWri.values()) {
			pWri.close();
		}

	}

}
