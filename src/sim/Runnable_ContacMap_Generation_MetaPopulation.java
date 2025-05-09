package sim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Runnable_ContacMap_Generation_MetaPopulation extends Runnable_ClusterModel_ContactMap_Generation_MultiMap {

	public static final int RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING = Runnable_ClusterModel_ContactMap_Generation_MultiMap.LENGTH_RUNNABLE_MAP_GEN_FIELD;
	public static final int RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_LOC_CONNECTIONS = RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING
			+ 1;
	public static final int LENGTH_RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN = RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_LOC_CONNECTIONS
			+ 1;

	public static final String POP_STAT_INITAL_FORMAT = "POP_STAT_INITAL_%d.csv"; // Seed
	public static final String POP_STAT_FINAL_FORMAT = "POP_STAT_FINAL_%d.csv"; // Seed
	public static final String POP_STAT_MOVEGRP_FORMAT = "POP_STAT_MOVEGRP_%d.csv"; // Seed

	// Population Index from Abstract_Runnable_ClusterModel
	// POP_INDEX_GRP = 0;
	// POP_INDEX_ENTER_POP_AGE = POP_INDEX_GRP + 1;
	// POP_INDEX_ENTER_POP_AT = POP_INDEX_ENTER_POP_AGE + 1;
	// POP_INDEX_EXIT_POP_AT = POP_INDEX_ENTER_POP_AT + 1;
	// POP_INDEX_HAS_REG_PARTNER_UNTIL = POP_INDEX_EXIT_POP_AT + 1;
	private static int POP_INDEX_META_HOME_LOCATION_HIST = Abstract_Runnable_ClusterModel.LENGTH_POP_ENTRIES;
	private static int LENGTH_POP_INDEX_META = POP_INDEX_META_HOME_LOCATION_HIST + 1;

	protected final int NUM_GENDER;
	protected final int NUM_LOC;
	protected final int NUM_AGE_GRP;
	protected Properties loadedProperties;

	private Pattern pattern_propName = Pattern.compile("RMP_MultMap_(\\d+)_(\\d+)_(\\d+)");

	private HashMap<Integer, int[]> helper_ageRange = null; // K = grp_indec, V = {age_min, age_max}
	private HashMap<Integer, int[][]> helper_loc_connection = null; // K = location index, V = {connection_1, ...}
																	// {cumul_weight_1,...}

	public Runnable_ContacMap_Generation_MetaPopulation(long mapSeed, Properties loadedProperties) {
		super(mapSeed);
		this.runnable_fields = Arrays.copyOf(this.runnable_fields, LENGTH_RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN);
		// Load properties
		this.loadedProperties = loadedProperties;
		loadRunnableFieldsFromProperties(this.loadedProperties);

		if (loadedProperties.containsKey(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ])) {
			snap_dur = Integer.parseInt(
					loadedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));
		}

		// Load NUM_GENDER, NUM_LOC and NUM_AGE_GRP
		Matcher m = pattern_propName.matcher(
				(CharSequence) loadedProperties.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_POP_TYPE]));
		if (m.find()) {
			NUM_GENDER = Integer.parseInt(m.group(1));
			NUM_LOC = Integer.parseInt(m.group(2));
			NUM_AGE_GRP = Integer.parseInt(m.group(3));
		} else {
			System.err.printf("Error: Ill-formed popType %s. Simulation will not run.\n ",
					loadedProperties.get(SimulationInterface.PROP_NAME[SimulationInterface.PROP_POP_TYPE]));
			NUM_GENDER = 0;
			NUM_LOC = 0;
			NUM_AGE_GRP = 0;
		}

	}

	@Override
	public void run() {
		if (NUM_GENDER + NUM_LOC + NUM_AGE_GRP > 0) {
			HashMap<Integer, Object[]> population = new HashMap<>();
			int[] contactMapValidRange = (int[]) runnable_fields[RUNNABLE_FIELD_CONTACT_MAP_GEN_VALID_RANGE];
			final long exportFreq = (long) runnable_fields[RUNNABLE_FILED_EXPORT_FREQ];
			int[] numInGrp = (int[]) runnable_fields[RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_NUMBER_OF_GRP];

			int popTime = contactMapValidRange[0];
			int nextId = 1;

			ArrayList<Integer> pop_stat_init_ids = new ArrayList<>();
			ArrayList<Integer> pop_stat_final_ids = new ArrayList<>();

			HashMap<Integer, ArrayList<Integer>> schedule_move_grp = new HashMap<>(); // K = time, V = list of pids e.g. for aging
			HashMap<Integer, ArrayList<Integer>> active_in_pop = new HashMap<>(); // K = grp, V = list of pids (sorted)

			ArrayList<int[]> pop_stat_moveGrp = new ArrayList<>(); // int[]{time, pid, from_grp, to_grp}

			HashMap<String, Object> updateObjs = new HashMap<>();
			updateObjs.put("active_in_pop", active_in_pop);
			updateObjs.put("pop_stat_init_ids", pop_stat_init_ids);
			updateObjs.put("pop_stat_final_ids", pop_stat_final_ids);
			updateObjs.put("pop_stat_moveGrp", pop_stat_moveGrp);
			updateObjs.put("schedule_move_grp", schedule_move_grp);

			if (population.isEmpty()) {
				// Initialise pop
				for (int g = 0; g < numInGrp.length; g++) {
					int[] ageRange = getAgeRange(g);

					for (int i = 0; i < numInGrp[g]; i++) {
						Object[] newPerson = new Object[LENGTH_POP_INDEX_META];
						newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_GRP] = g;
						newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT] = 0;
						newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE] = (int) ageRange[0]
								+ RNG.nextInt(ageRange[1] - ageRange[0]);

						// Note: Exit at in the case is exit the new group
						newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT] = ageRange[1]
								- (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE];

						nextId = addNewPerson(population, nextId, newPerson, updateObjs);
					}
				}
			}

			int numSnapsSim = Math.max(numSnaps, Math.round(contactMapValidRange[1] / snap_dur));

			for (int snapC = 0; snapC < numSnapsSim; snapC++) {

				for (int t = 0; t < snap_dur; t++) {

					if (schedule_move_grp.containsKey(popTime)) {
						ArrayList<Integer> move_grp_pids = schedule_move_grp.remove(popTime);

						for (int move_grp_pid : move_grp_pids) {
							int[] gla_index = getGrpIndex(
									(int) population.get(move_grp_pid)[Abstract_Runnable_ClusterModel.POP_INDEX_GRP]);
							if ((gla_index[2] + 1) < NUM_AGE_GRP) {
								// Aging
								int newGrp = (int) population
										.get(move_grp_pid)[Abstract_Runnable_ClusterModel.POP_INDEX_GRP] + 1;
								movePersonToNewGrp(popTime, population, move_grp_pid, newGrp, updateObjs);
							} else {
								// Age out
								removePerson(population, move_grp_pid, updateObjs);
							}

						}
					}

					int[] grpDiff = new int[numInGrp.length];
					ArrayList<Integer> grp_need_remove = new ArrayList<>();
					ArrayList<Integer> active_by_grp_sorted;

					for (int g = 0; g < numInGrp.length; g++) {
						active_by_grp_sorted = active_in_pop.get(g);
						// Check how many person in grp
						grpDiff[g] = active_by_grp_sorted.size() - numInGrp[g];
						if (grpDiff[g] > 0) {
							grp_need_remove.add(g);
						}
					}

					while (!grp_need_remove.isEmpty()) {
						int src_g = grp_need_remove.remove(RNG.nextInt(grp_need_remove.size()));
						int[] gla_index_src = getGrpIndex(src_g);
						active_by_grp_sorted = active_in_pop.get(src_g);
						int numToRemove = numInGrp[src_g] - active_by_grp_sorted.size();

						int[] dest = getDestinations(gla_index_src[1])[0];
						float offset = 0;
						float[] cumul_num_req_by_grp = new float[dest.length];

						// Check if there space at other locations
						for (int dI = 0; dI < dest.length; dI++) {
							int g = getGrpIndex(new int[] { gla_index_src[0], dest[dI], gla_index_src[2] });
							cumul_num_req_by_grp[dI] = offset + Math.max(0, numInGrp[g] - active_in_pop.get(g).size());
							offset += cumul_num_req_by_grp[dI];
						}

						while (numToRemove > 0 && cumul_num_req_by_grp[cumul_num_req_by_grp.length - 1] > 0) {

							// Attempt to find a destination
							int dest_location_index = Arrays.binarySearch(cumul_num_req_by_grp,
									RNG.nextFloat() * cumul_num_req_by_grp[cumul_num_req_by_grp.length - 1]);

							if (dest_location_index >= 0) {
								dest_location_index = ~dest_location_index;
							} else {
								dest_location_index = Math.min(dest_location_index + 1,
										cumul_num_req_by_grp.length - 1);
							}
							int dest_grp = getGrpIndex(
									new int[] { gla_index_src[0], dest[dest_location_index], gla_index_src[2] });

							// Choose a random person to move away
							int move_pid = active_in_pop.get(src_g).get(RNG.nextInt(active_in_pop.get(src_g).size()));
							movePersonToNewGrp(popTime, population, move_pid, dest_grp, updateObjs);

							// Update cumul_num_req_by_grp
							for (int dI = 0; dI < dest.length; dI++) {
								int g = getGrpIndex(new int[] { gla_index_src[0], dest[dI], gla_index_src[2] });
								cumul_num_req_by_grp[dI] = offset
										+ Math.max(0, numInGrp[g] - active_in_pop.get(g).size());
								offset += cumul_num_req_by_grp[dI];
							}

							numToRemove--;

						}

						// Remove the rest
						while (numToRemove > 0) {
							int removed_pid = active_by_grp_sorted.remove(RNG.nextInt(active_by_grp_sorted.size()));
							removePerson(population, removed_pid, updateObjs);
							numToRemove = numInGrp[src_g] - active_by_grp_sorted.size();
						}

					}

					// TODO: Form partnership

					popTime++;

				} // End of for (int t = 0; t < snap_dur; t++) loop

				// Print intermittent and final pop stat
				try {
					PrintWriter pWri;
					File tarFile;
					if (!pop_stat_init_ids.isEmpty()) {
						tarFile = getTargetFile(String.format(POP_STAT_INITAL_FORMAT, mapSeed));
						boolean append = tarFile.exists();
						pWri = new PrintWriter(new FileWriter(tarFile, append));

						if (!append) {
							pWri.println("ID,ENTER_POP_AGE,ENTER_POP_AT_TIME,INIT_GRP");
						}
						for (Integer id : pop_stat_init_ids) {
							Object[] person_stat = population.get(id);
							pWri.printf("%d,%s,%s,%s\n", id,
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP].toString());

						}
						pop_stat_init_ids.clear();

					}
					if (!pop_stat_final_ids.isEmpty()) {
						tarFile = getTargetFile(String.format(POP_STAT_FINAL_FORMAT, mapSeed));
						boolean append = tarFile.exists();
						pWri = new PrintWriter(new FileWriter(tarFile, append));
						if (!append) {
							pWri.println("ID,ENTER_POP_AGE,ENTER_POP_AT_TIME,EXIT_POP_AT_TIME");
						}
						for (Integer id : pop_stat_final_ids) {
							Object[] person_stat = population.get(id);
							pWri.printf("%d,%s,%s,%s\n", id,
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT].toString());

						}
						pop_stat_final_ids.clear();
					}
					if (!pop_stat_moveGrp.isEmpty()) {
						tarFile = getTargetFile(String.format(POP_STAT_MOVEGRP_FORMAT, mapSeed));
						boolean append = tarFile.exists();
						pWri = new PrintWriter(new FileWriter(tarFile, append));
						if (!append) {
							pWri.println("TIME,ID,GRP_SRC,GRP_DEST");
						}
						for (int[] movement : pop_stat_moveGrp) {
							String line_raw = Arrays.toString(movement);
							pWri.println(line_raw.substring(1, line_raw.length() - 1)); // Exclude ending []
						}
						pop_stat_moveGrp.clear();
					}

				} catch (IOException e) {

					e.printStackTrace(System.err);
				}

			}

		}

	}

	@SuppressWarnings("unchecked")
	protected void removePerson(HashMap<Integer, Object[]> population, int remove_pid,
			HashMap<String, Object> updateObjs) {
		HashMap<Integer, ArrayList<Integer>> active_in_pop = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get("active_in_pop");
		ArrayList<Integer> pop_stat_final_ids = (ArrayList<Integer>) updateObjs.get("pop_stat_final_ids");

		Object[] person_stat = population.get(remove_pid);
		int orgGrp = (int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP];

		if (active_in_pop != null) {
			ArrayList<Integer> active_by_grp_sorted = active_in_pop.get(orgGrp);
			int aI = Collections.binarySearch(active_by_grp_sorted, remove_pid);
			if (aI >= 0) {
				active_by_grp_sorted.remove(aI);
			}
		} else {
			System.err.println("Warning! active_in_pop not found.");
		}

		if (pop_stat_final_ids != null) {
			pop_stat_final_ids.add(remove_pid);
		} else {
			System.err.println("Warning! pop_stat_final_ids not found.");
		}
	}

	@SuppressWarnings("unchecked")
	protected void movePersonToNewGrp(int moveTime, HashMap<Integer, Object[]> population, int move_pid, int newGrp,
			HashMap<String, Object> updateObjs) {
		HashMap<Integer, ArrayList<Integer>> active_in_pop = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get("active_in_pop");
		ArrayList<int[]> pop_stat_moveGrp = (ArrayList<int[]>) updateObjs.get("pop_stat_moveGrp");

		HashMap<Integer, ArrayList<Integer>> schedule_move_grp = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get("schedule_move_grp");

		int aI;
		Object[] person_stat = population.get(move_pid);
		int orgGrp = (int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP];
		person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP] = newGrp;

		if (active_in_pop != null) {
			ArrayList<Integer> active_by_grp_sorted = active_in_pop.get(orgGrp);
			aI = Collections.binarySearch(active_by_grp_sorted, move_pid);
			if (aI >= 0) {
				active_by_grp_sorted.remove(aI);
			}
			ArrayList<Integer> target_grp_sorted = active_in_pop.get(newGrp);
			aI = Collections.binarySearch(target_grp_sorted, move_pid);
			if (aI < 0) {
				target_grp_sorted.add(~aI, move_pid);
			}
		} else {
			System.err.println("Warning! active_in_pop not found.");

		}

		if (schedule_move_grp != null) {
			ArrayList<Integer> move_grp_pids = schedule_move_grp.get(moveTime);
			if (move_grp_pids == null) {
				move_grp_pids = new ArrayList<>();
				schedule_move_grp.put(moveTime, move_grp_pids);
			}
			move_grp_pids.add(move_pid);
		} else {
			System.err.println("Warning! schedule_move_grp not found.");
		}

		if (pop_stat_moveGrp != null) {
			pop_stat_moveGrp.add(new int[] { moveTime, move_pid, orgGrp, newGrp });
		} else {
			System.err.println("Warning! pop_stat_moveGrp not found.");
		}

	}

	@SuppressWarnings("unchecked")
	protected int addNewPerson(HashMap<Integer, Object[]> population, int nextId, Object[] newPerson,
			HashMap<String, Object> updateObjs) {

		population.put(nextId, newPerson);

		HashMap<Integer, ArrayList<Integer>> schedule_move_grp = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get("schedule_move_grp");
		HashMap<Integer, ArrayList<Integer>> active_in_pop = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get("active_in_pop");
		ArrayList<Integer> pop_stat_init_ids = (ArrayList<Integer>) updateObjs.get("pop_stat_init_ids");

		// Schedule exit group
		if (schedule_move_grp != null) {

			int exit_grp_time = (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT];
			ArrayList<Integer> move_grp_pids = schedule_move_grp.get(exit_grp_time);
			if (move_grp_pids == null) {
				move_grp_pids = new ArrayList<>();
				schedule_move_grp.put(exit_grp_time, move_grp_pids);
			}
			move_grp_pids.add(nextId);
		} else {
			System.err.println("Warning! schedule_move_grp not found.");
		}

		// Active by grp
		if (active_in_pop != null) {
			int grp_num = (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_GRP];
			ArrayList<Integer> active_by_grp_sorted = active_in_pop.get(grp_num);
			if (active_by_grp_sorted == null) {
				active_by_grp_sorted = new ArrayList<>();
				active_in_pop.put(grp_num, active_by_grp_sorted);
			}
			int aI = Collections.binarySearch(active_by_grp_sorted, nextId);
			if (aI < 0) {
				active_by_grp_sorted.add(~aI, nextId);
			}
		} else {
			System.err.println("Warning! active_in_pop not found.");
		}

		if (pop_stat_init_ids != null) {
			pop_stat_init_ids.add(nextId);
		} else {
			System.err.println("Warning! pop_stat_init_ids not found.");
		}

		nextId++;
		return nextId;
	}

	protected int getGrpIndex(int[] gender_loc_age) {
		return gender_loc_age[2] + gender_loc_age[1] * NUM_AGE_GRP + gender_loc_age[0] * (NUM_AGE_GRP * NUM_LOC);
	}

	protected int[] getGrpIndex(int grpNum) {
		return new int[] { (grpNum / (NUM_AGE_GRP * NUM_LOC)) % NUM_GENDER, (grpNum / NUM_AGE_GRP) % NUM_LOC,
				grpNum % NUM_AGE_GRP, };
	}

	/**
	 * 
	 * @param location
	 * @return int[][], with first array as destination and second as weight
	 */
	private int[][] getDestinations(int location) {
		int[][] res;
		if (helper_loc_connection == null) {
			helper_loc_connection = new HashMap<>();
			HashMap<Integer, ArrayList<Integer>> connc_raw = new HashMap<>();
			int[][] conncs = (int[][]) getRunnable_fields()[RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_LOC_CONNECTIONS];
			for (int[] connc : conncs) {
				int src = connc[0];
				ArrayList<Integer> arr = connc_raw.get(src);
				if (arr == null) {
					arr = new ArrayList<>();
					connc_raw.put(src, arr);
				}
				for (int i = 1; i < connc.length; i++) {
					arr.add(connc[i]);
				}
			}
			for (Entry<Integer, ArrayList<Integer>> ent : connc_raw.entrySet()) {
				res = new int[2][ent.getValue().size() / 2];
				int cumul_weight = 0;
				int loc_pt = 0;

				for (int i = 0; i < ent.getValue().size(); i += 2) {
					res[0][loc_pt] = ent.getValue().get(i);
					res[1][loc_pt] = cumul_weight + ent.getValue().get(i + 1);

					loc_pt++;
					cumul_weight += res[1][loc_pt];
				}
				helper_loc_connection.put(ent.getKey(), res);
			}
		}

		res = helper_loc_connection.get(location);
		return res;
	}

	private int[] getAgeRange(int grpNum) {
		if (helper_ageRange == null) {
			helper_ageRange = new HashMap<>();
			double[][] ageRanges = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST];
			for (double[] ageRange : ageRanges) {
				helper_ageRange.put((int) ageRange[0], new int[] { (int) ageRange[1], (int) ageRange[2] });
			}
		}
		int[] res = helper_ageRange.get(grpNum);
		if (res == null) {
			res = helper_ageRange.get(~(grpNum % NUM_AGE_GRP));
			helper_ageRange.put(grpNum, res);
		}
		return res;
	}

	protected int getAge(int pid, int time, HashMap<Integer, Object[]> population) {
		Object[] person = population.get(pid);
		return (int) person[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE]
				+ (time - (int) person[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT]);

	}

}
