package sim;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.jgrapht.graph.DefaultWeightedEdge;

import map.Map_Location_Mobility;
import person.AbstractIndividualInterface;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;

public class Runnable_Demographic_Generation implements Runnable {

	private Properties loadedProperties;
	private Map_Location_Mobility loc_map;
	private File baseDir;
	private RandomGenerator RNG;
	private long mapSeed;

	public static final String FILENAME_FORMAT_DEMOGRAPHIC = "Demographic_%d_%d.csv"; // POP_ID, SEED
	public static final String FILENAME_FORMAT_MOVEMENT = "Movement_%s_%d.csv"; // POPSRC_POPTAR , SEED

	public static final String FILE_HEADER_DEMOGRAPHIC = "PID,ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_GRP";
	public static final String FILE_HEADER_MOVEMENT = "TIME,PID";

	// K = PID, V = int[]{enter_pop_at, exit_pop_at, enter_pop_age, enter_grp,
	// home_location, current_grp, current_loc}
	private static final int INDEX_MAP_INDIV_ENTER_AT = 0;
	private static final int INDEX_MAP_INDIV_EXIT_AT = INDEX_MAP_INDIV_ENTER_AT + 1;
	private static final int INDEX_MAP_INDIV_ENTER_AGE = INDEX_MAP_INDIV_EXIT_AT + 1;
	private static final int INDEX_MAP_INDIV_ENTER_GRP = INDEX_MAP_INDIV_ENTER_AGE + 1;
	private static final int INDEX_MAP_INDIV_ENTER_LOC = INDEX_MAP_INDIV_ENTER_GRP + 1;
	private static final int INDEX_MAP_INDIV_CURRENT_GRP = INDEX_MAP_INDIV_ENTER_LOC + 1;
	private static final int INDEX_MAP_INDIV_CURRENT_LOC = INDEX_MAP_INDIV_CURRENT_GRP + 1;
	private static final int LENGTH_MAP_INDIV = INDEX_MAP_INDIV_CURRENT_LOC + 1;

	// 1: RUNNABLE_FIELD_INT_SETTING
	public static final int RUNNABLE_FIELD_INT_SETTING = Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_INT_SETTING;
	public static final int INT_SETTING_TOTAL_POP_SIZE = 0;
	public static final int INT_SETTING_AWAY_DAYS_MIN = INT_SETTING_TOTAL_POP_SIZE + 1;
	public static final int INT_SETTING_AWAY_DAYS_RANGE = INT_SETTING_AWAY_DAYS_MIN + 1;

	// 2: RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH
	// Format: File path. Noted that it assumes node info will be
	// filename_NoteInfo.csv
	public static final int RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH = Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_NUMBER_OF_GRP;

	public Runnable_Demographic_Generation(long mapSeed, Properties loadedProperties) {
		this.loadedProperties = loadedProperties;
		this.mapSeed = mapSeed;
		this.RNG = new MersenneTwisterRandomGenerator(mapSeed);
		try {
			this.loc_map = (Map_Location_Mobility) loadedProperties.get(Simulation_MetaPop.PROP_LOC_MAP);
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

		// Properties
		int max_time = Integer.parseInt(
				loadedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SNAP]))
				* Integer.parseInt(loadedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));

		int[][] age_dist = (int[][]) util.PropValUtils.propStrToObject(loadedProperties.getProperty(String.format(
				"%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
				Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST)),
				int[][].class);

		int[] intSetting = (int[]) util.PropValUtils.propStrToObject(
				loadedProperties.getProperty(String.format("%s%d",
						Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX, RUNNABLE_FIELD_INT_SETTING)),
				int[].class);

		// Key = POP_ID
		HashMap<Integer, HashMap<String, Object>> node_info = loc_map.getNode_info();
		// Key = POP_ID, Value = Map(Grp, ArrayList<> pids)
		HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> map_group_member_by_pop = new HashMap<>();
		// Key = POPSRC_POPTAR
		HashMap<String, StringBuilder> map_movement_strBuilder = new HashMap<>();

		// Key = Time, Value = ArrayList(pids)
		HashMap<Integer, ArrayList<Integer>> map_indivdual_return = new HashMap<>();
		// Key = PID
		HashMap<Integer, int[]> map_indiv = new HashMap<>();
		// Key = Group
		HashMap<Integer, int[]> map_group_age_range = new HashMap<>();

		// Lookup all have Key = POP_ID
		HashMap<Integer, int[]> lookup_edge_target_array = new HashMap<>();
		HashMap<Integer, double[]> lookup_edge_weight_array = new HashMap<>();
		HashMap<Integer, Integer> lookup_pop_size_all_grps = new HashMap<>();

		// Population id
		Integer[] pop_id_arr = node_info.keySet().toArray(new Integer[0]);
		Arrays.sort(pop_id_arr);

		int model_pop_size = intSetting[INT_SETTING_TOTAL_POP_SIZE];
		float total_pop_size = 0;

		long tic = System.currentTimeMillis();

		// Pop size
		for (Integer pop_id : pop_id_arr) {
			int[] pop_size_by_grps = (int[]) node_info.get(pop_id).get(Map_Location_Mobility.NODE_INFO_POP_SIZE);
			int pop_all_grps = 0;
			for (int g = 0; g < pop_size_by_grps.length; g++) {
				pop_all_grps += pop_size_by_grps[g];
			}
			lookup_pop_size_all_grps.put(pop_id, pop_all_grps);
			total_pop_size += pop_all_grps;
		}

		// Set edge weight
		for (Integer pop_id : pop_id_arr) {
			Set<DefaultWeightedEdge> connections = loc_map.outgoingEdgesOf(pop_id);
			int[] target_pop = new int[connections.size()];
			double[] edge_weight = new double[connections.size()];
			double weight_offset = 0;
			int weight_pt = 0;
			for (DefaultWeightedEdge connc : connections) {
				target_pop[weight_pt] = loc_map.getEdgeTarget(connc);
				// Assume to be proportional to target population size if > 0, or direct weight
				// if < 0
				weight_offset += loc_map.getEdgeWeight(connc) >= 0
						? loc_map.getEdgeWeight(connc) * lookup_pop_size_all_grps.get(target_pop[weight_pt])
						: -loc_map.getEdgeWeight(connc);
				edge_weight[weight_pt] = weight_offset;
				weight_pt++;
			}
			lookup_edge_target_array.put(pop_id, target_pop);
			lookup_edge_weight_array.put(pop_id, edge_weight);
		}

		// Initialise population

		int maxAge = 0;
		ArrayList<Integer> grpPids;
		for (Integer pop_id : pop_id_arr) {
			int[] pop_size_by_grps = (int[]) node_info.get(pop_id).get(Map_Location_Mobility.NODE_INFO_POP_SIZE);
			for (int g = 0; g < pop_size_by_grps.length; g++) {
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
				while (numPersonInGrp < Math.round(model_pop_size * pop_size_by_grps[g] / total_pop_size)) {
					int[] indiv_ent = new int[LENGTH_MAP_INDIV];
					indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] = pop_id;
					indiv_ent[INDEX_MAP_INDIV_ENTER_AT] = currentTime;
					indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = -1;
					indiv_ent[INDEX_MAP_INDIV_ENTER_AGE] = age_range[0] + RNG.nextInt(age_range[1] - age_range[0]);
					indiv_ent[INDEX_MAP_INDIV_ENTER_GRP] = g;
					indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = g;
					indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] = pop_id;
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

			// Aging
			for (Integer pop_id : pop_id_arr) {
				for (int g = 0; g < ((int[]) node_info.get(pop_id)
						.get(Map_Location_Mobility.NODE_INFO_POP_SIZE)).length; g++) {
					Integer[] grpPidsArr = map_group_member_by_pop.get(pop_id).get(g).toArray(new Integer[0]);
					for (int pid : grpPidsArr) {
						int[] indiv_ent = map_indiv.get(pid);
						if (indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] != -1) {
							// Aging
							int currentAge = (currentTime - indiv_ent[INDEX_MAP_INDIV_ENTER_AT])
									+ indiv_ent[INDEX_MAP_INDIV_ENTER_AGE];
							if (currentAge >= maxAge) {
								// Return home first
								if (indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] != indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC]) {
									setMovementAt(currentTime, pid, indiv_ent, indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC],
											indiv_ent[INDEX_MAP_INDIV_ENTER_LOC], map_movement_strBuilder);
								}

								indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = currentTime;
								grpPids = map_group_member_by_pop.get(indiv_ent[INDEX_MAP_INDIV_ENTER_LOC])
										.get(indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP]);
								grpPids.remove(Collections.binarySearch(grpPids, pid));
								indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = -1;
								indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] = -1;

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
						}
					}
				}
			}

			// Add or remove new individuals
			for (Integer pop_id : pop_id_arr) {
				int[] pop_size_by_grps = (int[]) node_info.get(pop_id).get(Map_Location_Mobility.NODE_INFO_POP_SIZE);
				Integer[] grpArray = map_group_member_by_pop.get(pop_id).keySet().toArray(new Integer[0]);
				Arrays.sort(grpArray);
				for (int g = 0; g < pop_size_by_grps.length; g++) {
					int gSize = Math.round(model_pop_size * pop_size_by_grps[g] / total_pop_size);
					grpPids = map_group_member_by_pop.get(pop_id).get(g);
					while (grpPids.size() != gSize) {
						if (grpPids.size() > gSize) {
							// Remove individuals from group
							int remove_pid = grpPids.remove(RNG.nextInt(grpPids.size()));
							int[] indiv_ent = map_indiv.get(remove_pid);
							if (indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] != indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC]) {
								setMovementAt(currentTime, remove_pid, indiv_ent,
										indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC], indiv_ent[INDEX_MAP_INDIV_ENTER_LOC],
										map_movement_strBuilder);
							}
							indiv_ent[INDEX_MAP_INDIV_EXIT_AT] = currentTime;
							indiv_ent[INDEX_MAP_INDIV_CURRENT_GRP] = -1;
							indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] = -1;
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
							indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] = pop_id;
							map_indiv.put(nextId, indiv_ent);
							nextId++;
						}
					}
				}
			}
			// Movement - Return home
			ArrayList<Integer> return_home_pids = map_indivdual_return.remove(currentTime);				
			if (return_home_pids != null) {
				Collections.sort(return_home_pids);
				for (int pid : return_home_pids) {
					int[] indiv_ent = map_indiv.get(pid);
					if (indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] != -1) {
						int move_src = indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC];
						int move_target = indiv_ent[INDEX_MAP_INDIV_ENTER_LOC];
						setMovementAt(currentTime, pid, indiv_ent, move_src, move_target, map_movement_strBuilder);
					}
				}
			}
			
			// Movement - Away from home
			for (Integer pop_id : pop_id_arr) {
				int[] pop_size_by_grps = (int[]) node_info.get(pop_id).get(Map_Location_Mobility.NODE_INFO_POP_SIZE);
				Integer[] grpArray = map_group_member_by_pop.get(pop_id).keySet().toArray(new Integer[0]);
				Arrays.sort(grpArray);
				
				float[] num_away_by_grp = (float[]) node_info.get(pop_id).get(Map_Location_Mobility.NODE_INFO_AWAY);
				for (int g = 0; g < pop_size_by_grps.length; g++) {
					ArrayList<Integer> pids_at_home = new ArrayList<>();
					for (Integer pid : map_group_member_by_pop.get(pop_id).get(g)) {
						int[] indiv_ent = map_indiv.get(pid);
						if (indiv_ent[INDEX_MAP_INDIV_ENTER_LOC] == indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC]) {
							if (return_home_pids == null || Collections.binarySearch(return_home_pids, pid) < 0) {
								// Not allow to move away when just return home
								pids_at_home.add(pid);
							}
						}
					}
					int num_home_expected = Math.round(pop_size_by_grps[g] * (1 - num_away_by_grp[g] / 100f));

					while (pids_at_home.size() > num_home_expected) {
						int pid = pids_at_home.remove(RNG.nextInt(pids_at_home.size()));
						// Movement
						int[] pop_target = lookup_edge_target_array.get(pop_id);
						double[] target_weight = lookup_edge_weight_array.get(pop_id);

						double pTar = RNG.nextDouble() * target_weight[target_weight.length - 1];
						int tar_pt = Arrays.binarySearch(target_weight, pTar);
						if (tar_pt < 0) {
							tar_pt = ~tar_pt;
						}

						int move_target = pop_target[tar_pt];
						setMovementAt(currentTime, pid, map_indiv.get(pid), pop_id, move_target,
								map_movement_strBuilder);

						// Set return
						int return_day = currentTime + intSetting[INT_SETTING_AWAY_DAYS_MIN]
								+ RNG.nextInt(intSetting[INT_SETTING_AWAY_DAYS_RANGE]);
						ArrayList<Integer> sch_return = map_indivdual_return.get(return_day);
						if (sch_return == null) {
							sch_return = new ArrayList<>();
							map_indivdual_return.put(return_day, sch_return);
						}
						sch_return.add(pid);

					}

				}

			}

			currentTime++;
		}

		// Print demographic CSV

		HashMap<Integer, PrintWriter> demo_priWri = new HashMap<>();

		for (Entry<Integer, int[]> ent : map_indiv.entrySet()) {
			int homeLoc = ent.getValue()[INDEX_MAP_INDIV_ENTER_LOC];
			try {
				PrintWriter pWri = demo_priWri.get(homeLoc);
				if (pWri == null) {
					pWri = new PrintWriter(
							new File(baseDir, String.format(FILENAME_FORMAT_DEMOGRAPHIC, homeLoc, mapSeed)));
					pWri.println(FILE_HEADER_DEMOGRAPHIC);
					demo_priWri.put(homeLoc, pWri);
				}
				pWri.printf("%d,%d,%d,%d,%d\n", ent.getKey(), ent.getValue()[INDEX_MAP_INDIV_ENTER_AT],
						ent.getValue()[INDEX_MAP_INDIV_EXIT_AT], ent.getValue()[INDEX_MAP_INDIV_ENTER_AGE],
						ent.getValue()[INDEX_MAP_INDIV_ENTER_GRP]);
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
		for (PrintWriter pWri : demo_priWri.values()) {
			pWri.close();
		}

		// Print movement CSV
		for (Entry<String, StringBuilder> ent : map_movement_strBuilder.entrySet()) {
			try {
				PrintWriter pWri = new PrintWriter(
						new File(baseDir, String.format(FILENAME_FORMAT_MOVEMENT, ent.getKey(), mapSeed)));
				pWri.println(FILE_HEADER_MOVEMENT);
				pWri.print(ent.getValue().toString());
				pWri.close();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}

		System.out.printf("Demographic / Mobility generation for mapSeed=%d completed. Time required = %.3fs\n",
				mapSeed, (System.currentTimeMillis() - tic) / 1000.0);

	}

	private void setMovementAt(int move_time, Integer pid, int[] indiv_ent, int move_src, int move_target,
			HashMap<String, StringBuilder> map_movement_strBuilder) {
		indiv_ent[INDEX_MAP_INDIV_CURRENT_LOC] = move_target;
		String move_key = String.format("%d_%d", move_src, move_target);
		StringBuilder move_str = map_movement_strBuilder.get(move_key);

		if (move_str == null) {
			move_str = new StringBuilder();
			map_movement_strBuilder.put(move_key, move_str);
		}
		move_str.append(String.format("%d,%d\n", move_time, pid));
	}

}
