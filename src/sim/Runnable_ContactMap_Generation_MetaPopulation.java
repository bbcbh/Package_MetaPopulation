package sim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.distribution.AbstractRealDistribution;

/**
 * Generate Meta_Population with network defined directly in population file. 
 * 
 * For more complex network, use Runnable_ContactMap_Generation_LocationMap instead.
 * 
 * @see Runnable_ContactMap_Generation_LocationMap_IREG
 */

public class Runnable_ContactMap_Generation_MetaPopulation extends Runnable_ClusterModel_ContactMap_Generation_MultiMap {

	public static final Pattern pattern_propName = Pattern.compile("RMP_MultMap_(\\d+)_(\\d+)_(\\d+)");

	public static final int RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING = Runnable_ClusterModel_ContactMap_Generation_MultiMap.LENGTH_RUNNABLE_MAP_GEN_FIELD;
	public static final int RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_LOC_CONNECTIONS = RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING
			+ 1;
	public static final int LENGTH_RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN = RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_LOC_CONNECTIONS
			+ 1;

	public static final String POP_STAT_INITAL_FORMAT = "POP_STAT_INITAL_%d.csv"; // Seed
	public static final String POP_STAT_FINAL_FORMAT = "POP_STAT_FINAL_%d.csv"; // Seed
	public static final String POP_STAT_MOVE_LOC_FORMAT = "POP_STAT_MOVE_LOC_%d.csv"; // Seed

	// Population Index from Abstract_Runnable_ClusterModel
	// POP_INDEX_GRP = 0; // Group when enter population
	// POP_INDEX_ENTER_POP_AGE = POP_INDEX_GRP + 1;
	// POP_INDEX_ENTER_POP_AT = POP_INDEX_ENTER_POP_AGE + 1;
	// POP_INDEX_EXIT_POP_AT = POP_INDEX_ENTER_POP_AT + 1;
	// POP_INDEX_HAS_REG_PARTNER_UNTIL = POP_INDEX_EXIT_POP_AT + 1;
	private static final int POP_INDEX_META_LOC_TARGET = Abstract_Runnable_ClusterModel.LENGTH_POP_ENTRIES;
	private static final int POP_INDEX_META_LOC_MOVE_AT = POP_INDEX_META_LOC_TARGET + 1;
	private static final int POP_INDEX_META_PARTNER_HIST = POP_INDEX_META_LOC_MOVE_AT + 1; // See PARTNER_HIST_INDEX_
																							// ... below
	private static final int LENGTH_POP_INDEX_META = POP_INDEX_META_PARTNER_HIST + 1;

	private static final int PARTNER_HIST_INDEX_VALID_UNTIL = 0;
	private static final int PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK = PARTNER_HIST_INDEX_VALID_UNTIL + 1;
	private static final int PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL = PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK + 1;
	private static final int PARTNER_HIST_INDEX_HISTORY_START = PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL + 1;

	private final String UPDATEOBJ_ACTIVE_IN_POP = "active_in_pop";
	private final String UPDATEOBJ_SCHEDULE_MOVE_GRP = "schedule_move_loc";
	private final String UPDATEOBJ_POP_STAT_INIT_IDS = "pop_stat_init_ids";
	private final String UPDATEOBJ_POP_STAT_FINAL_IDS = "pop_stat_final_ids";
	private final String UPDATEOBJ_POP_STAT_MOVE_IDS = "pop_stat_move_ids";

	// Fields

	protected final int NUM_GENDER;
	protected final int NUM_LOC;
	protected final int NUM_AGE_GRP;
	protected Properties loadedProperties;

	private transient int[] helper_ageRange_all = null;
	private transient HashMap<Integer, int[]> helper_ageRange_by_grp = null; // K = grp_index, V = {age_min, age_max}
	private transient HashMap<Integer, int[][]> helper_loc_connection = null; // K = location index, V = {connection_1,
																				// ...}
	// {cumul_weight_1,...}
	private transient HashMap<Integer, double[]> helper_partnership_setting = null; // K = grp_index, V = {time_range,
	// number_partner_range, probability}
	private transient HashMap<Integer, double[]> helper_partner_age_mix = null; // K = grp_index, V =
																				// {age_index_prob_0,...}

	private static final int HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE = 0;
	private static final int HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_MEAN = HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE
			+ 1;
	private static final int HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_SD = HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_MEAN
			+ 1;
	private static final int HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START = HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_SD
			+ 1;

	public Runnable_ContactMap_Generation_MetaPopulation(long mapSeed, Properties loadedProperties) {
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
			int[] numInGrp = (int[]) runnable_fields[RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_NUMBER_OF_GRP];

			int popTime = contactMapValidRange[0];
			int nextId = 1;

			ArrayList<Integer> pop_stat_init_ids = new ArrayList<>();
			ArrayList<Integer> pop_stat_final_ids = new ArrayList<>();
			ArrayList<int[]> pop_stat_move_ids = new ArrayList<>();

			HashMap<Integer, ArrayList<Integer>> active_in_pop = new HashMap<>(); // K = grp, V = list of pids (sorted)
			HashMap<Integer, ArrayList<int[]>> schedule_move_grp = new HashMap<>(); // K = time, V = {pid,
																					// from_grp,to_grp}

			HashMap<String, Object> updateObjs = new HashMap<>();
			updateObjs.put(UPDATEOBJ_ACTIVE_IN_POP, active_in_pop);
			updateObjs.put(UPDATEOBJ_SCHEDULE_MOVE_GRP, schedule_move_grp);
			updateObjs.put(UPDATEOBJ_POP_STAT_INIT_IDS, pop_stat_init_ids);
			updateObjs.put(UPDATEOBJ_POP_STAT_FINAL_IDS, pop_stat_final_ids);
			updateObjs.put(UPDATEOBJ_POP_STAT_MOVE_IDS, pop_stat_move_ids);

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
						// Time for moving to next age group
						newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT] = ageRange[1]
								- (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE];

						newPerson[POP_INDEX_META_LOC_TARGET] = new ArrayList<Integer>();
						newPerson[POP_INDEX_META_LOC_MOVE_AT] = new ArrayList<Integer>();

						nextId = addNewPersonToPopulation(population, nextId, newPerson, updateObjs);

					}
				}
			}

			int numSnapsSim = Math.max(numSnaps, Math.round(contactMapValidRange[1] / snap_dur));

			for (int snapC = 0; snapC < numSnapsSim; snapC++) {
				int snap_start = popTime;

				// Schedule group movement with snap duration based on group size
				int[] grpDiff = new int[numInGrp.length];
				ArrayList<Integer> grp_need_remove = new ArrayList<>();
				ArrayList<Integer> active_by_grp_sorted;
				HashMap<Integer, ArrayList<int[]>> partnership_added = new HashMap<>();

				for (int g = 0; g < numInGrp.length; g++) {
					active_by_grp_sorted = active_in_pop.get(g);
					// Check how many person in grp
					grpDiff[g] = active_by_grp_sorted.size() - numInGrp[g];
					if (grpDiff[g] > 0) {
						grp_need_remove.add(g);
					}
				}

				// Move person if there are any suitable
				while (!grp_need_remove.isEmpty()) {
					int src_g = grp_need_remove.remove(RNG.nextInt(grp_need_remove.size()));
					int[] gla_index_src = getGrpIndex(src_g);
					active_by_grp_sorted = active_in_pop.get(src_g);
					int numToRemove = active_by_grp_sorted.size() - numInGrp[src_g];

					int[] dest = getDestinations(gla_index_src[1])[0];
					ArrayList<Integer> valid_dest = new ArrayList<>();
					ArrayList<Float> valid_space = new ArrayList<>();

					// Check if there space at other locations
					checkValidMoveDestination(gla_index_src, dest, grpDiff, valid_dest, valid_space);

					while (numToRemove > 0 && valid_dest.size() > 0) {
						// Attempt to find a destination
						int dest_location_index = Collections.binarySearch(valid_space,
								RNG.nextFloat() * valid_space.get(valid_space.size() - 1));

						if (dest_location_index >= 0) {
							dest_location_index = ~dest_location_index;
						} else {
							dest_location_index = Math.min(dest_location_index + 1, valid_space.size() - 1);
						}
						int dest_grp = getGrpIndex(
								new int[] { gla_index_src[0], valid_dest.get(dest_location_index), gla_index_src[2] });

						// Choose a random person to move away
						int move_pid = active_in_pop.get(src_g).get(RNG.nextInt(active_in_pop.get(src_g).size()));

						schedule_movePerson(snap_start, move_pid, population.get(move_pid), dest_grp, updateObjs);

						grpDiff[src_g]--;
						grpDiff[dest_grp]++;

						checkValidMoveDestination(gla_index_src, dest, grpDiff, valid_dest, valid_space);
						numToRemove--;
					}
				}

				// Add or remove person
				for (int g = 0; g < numInGrp.length; g++) {
					// Add new person
					if (grpDiff[g] < 0) {
						int[] ageRange = getAgeRange(g);
						int numToAdd = -grpDiff[g];

						while (numToAdd > 0) {
							int add_time = snap_start + RNG.nextInt(snap_dur);
							ArrayList<int[]> move_schedule_at = schedule_move_grp.get(add_time);

							if (move_schedule_at == null) {
								move_schedule_at = new ArrayList<>();
								schedule_move_grp.put(add_time, move_schedule_at);
							}
							move_schedule_at.add(new int[] { -nextId });

							Object[] newPerson = new Object[LENGTH_POP_INDEX_META];
							newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_GRP] = g;
							newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT] = add_time;
							newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE] = (int) ageRange[0]
									+ RNG.nextInt(ageRange[1] - ageRange[0]);
							// Time for moving to next age group
							newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT] = popTime + ageRange[1]
									- (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE];
							newPerson[POP_INDEX_META_LOC_TARGET] = new ArrayList<Integer>();
							newPerson[POP_INDEX_META_LOC_MOVE_AT] = new ArrayList<Integer>();
							nextId = addNewPersonToPopulation(population, nextId, newPerson, updateObjs);
							numToAdd--;
						}
					} else if (grpDiff[g] > 0) {
						int numToRemove = grpDiff[g];
						while (numToRemove > 0) {
							active_by_grp_sorted = active_in_pop.get(g);
							int removed_pid = active_by_grp_sorted.get(RNG.nextInt(active_by_grp_sorted.size()));
							Object[] remPerson = population.get(removed_pid);
							int remove_time = snap_start + RNG.nextInt(snap_dur);
							remPerson[POP_INDEX_EXIT_POP_AT] = remove_time;
							ArrayList<int[]> move_schedule_at = schedule_move_grp.get(remove_time);
							if (move_schedule_at == null) {
								move_schedule_at = new ArrayList<>();
								schedule_move_grp.put(remove_time, move_schedule_at);
							}
							move_schedule_at.add(new int[] { removed_pid });
							numToRemove--;
						}

					}

				}
				while (popTime < snap_start + snap_dur) {
					// Aging, movement and form partnership
					ArrayList<int[]> schedule_move_grp_today = schedule_move_grp.remove(popTime);
					if (schedule_move_grp_today != null) {
						for (int[] movement : schedule_move_grp_today) {
							int aI;
							int pid = movement[0];
							Object[] person_stat = population.get(pid);

							if (pid < 0) {
								// Add person
								pid = Math.abs(pid);
								int init_grp = (int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP];
								ArrayList<Integer> active_in_pop_by_grp = active_in_pop.get(init_grp);
								if (active_in_pop_by_grp == null) {
									active_in_pop_by_grp = new ArrayList<>();
									active_in_pop.put(init_grp, active_in_pop_by_grp);
								}
								active_in_pop_by_grp.add(pid);

							} else {
								int currentGrp = getGrpAtTime(person_stat, popTime);
								int[] gla = getGrpIndex(currentGrp);

								aI = Collections.binarySearch(active_in_pop.get(currentGrp), pid);
								if (aI >= 0) {
									active_in_pop.get(currentGrp).remove(aI);
								}
								if (movement.length == 1) {
									// Aging
									if (gla[2] + 1 < NUM_AGE_GRP) {
										gla[2]++;
										int newGrp = getGrpIndex(gla);
										int[] ageRange = getAgeRange(newGrp);
										int exitTime = (ageRange[1] - ageRange[0]) + popTime;
										person_stat[POP_INDEX_EXIT_POP_AT] = exitTime;

										ArrayList<int[]> move_sch_exit = schedule_move_grp.get(exitTime);
										if (move_sch_exit == null) {
											move_sch_exit = new ArrayList<>();
											schedule_move_grp.put(exitTime, move_sch_exit);
										}
										move_sch_exit.add(new int[] { nextId });

										aI = Collections.binarySearch(active_in_pop.get(newGrp), pid);
										if (aI < 0) {
											active_in_pop.get(newGrp).add(~aI, pid);
										}
									} else {
										person_stat[POP_INDEX_EXIT_POP_AT] = popTime;
										removePerson(pid, person_stat, updateObjs);
									}

								} else {
									// Movement
									aI = Collections.binarySearch(active_in_pop.get(movement[1]), pid);
									if (aI < 0) {
										active_in_pop.get(movement[1]).add(~aI, pid);
									}
									pop_stat_move_ids.add(new int[] { popTime, pid, movement[1] });
								}
							}
						}
					}

					// Form partnership
					HashMap<Integer, ArrayList<Integer>> candidate_by_age = new HashMap<>();

					for (int loc = 0; loc < NUM_LOC; loc++) {
						partnership_added.put(loc, new ArrayList<>());

						// Candidate
						int[] candidate_hist;
						candidate_by_age.clear();

						for (int age = 0; age < NUM_AGE_GRP; age++) {
							int m_index = getGrpIndex(new int[] { 1, loc, age });
							double[] partnership_setting_candidate = getNumberParnterSetting(m_index);
							candidate_by_age.put(age, new ArrayList<>(active_in_pop.get(m_index)));

							Iterator<Integer> candidate_iter = candidate_by_age.get(age).iterator();
							while (candidate_iter.hasNext()) {
								int pid_m = candidate_iter.next();
								Object[] active_m = population.get(pid_m);

								candidate_hist = (int[]) active_m[POP_INDEX_META_PARTNER_HIST];
								if (candidate_hist == null) {
									candidate_hist = new int[PARTNER_HIST_INDEX_HISTORY_START
											+ (int) partnership_setting_candidate[HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE]];
									active_m[POP_INDEX_META_PARTNER_HIST] = candidate_hist;
								}
								if (candidate_hist[PARTNER_HIST_INDEX_VALID_UNTIL] <= snap_start + snap_dur) {
									updatePersonPartnershipStatus(snap_start, candidate_hist, partnership_setting_candidate);
								}
								// Check availability
								if (candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] <= 0) {
									candidate_iter.remove();
								}
							}

						}

						for (int age = 0; age < NUM_AGE_GRP; age++) {
							int f_index = getGrpIndex(new int[] { 0, loc, age });

							double[] partnership_setting_seeker = getNumberParnterSetting(f_index);
							double[] age_mix = getAgeMix(f_index);
							int time_range = (int) partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE];
							AbstractRealDistribution partner_dur_default = generateGammaDistribution(RNG,
									new double[] {
											partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_MEAN],
											partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_SD] }

							);

							ArrayList<Integer> active_female = active_in_pop.get(f_index);

							for (int pid : active_female) {
								Object[] active_f = population.get(pid);

								int[] seeker_hist = (int[]) active_f[POP_INDEX_META_PARTNER_HIST];
								if (seeker_hist == null) {
									// int[]{valid_until, number_partner_to_seek, partner_history};
									seeker_hist = new int[PARTNER_HIST_INDEX_HISTORY_START
											+ (int) partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE]];

									active_f[POP_INDEX_META_PARTNER_HIST] = seeker_hist;
								}
								if (seeker_hist[PARTNER_HIST_INDEX_VALID_UNTIL] <= snap_start + snap_dur) {
									updatePersonPartnershipStatus(snap_start, seeker_hist, partnership_setting_seeker);
								}
								if (seeker_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] > 0) {
									int numDaysLeft = seeker_hist[PARTNER_HIST_INDEX_VALID_UNTIL] - popTime;
									if (RNG.nextInt(
											numDaysLeft) < seeker_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK]) {
										
										int targetAgeGrp = Arrays.binarySearch(age_mix, RNG.nextFloat());
										if (targetAgeGrp < 0) {
											targetAgeGrp = ~targetAgeGrp;
										}

										if (candidate_by_age.get(targetAgeGrp).size() > 0) {
											// Choose a random candidate
											int candidate_index = RNG
													.nextInt(candidate_by_age.get(targetAgeGrp).size());
											int candidate_id = candidate_by_age.get(targetAgeGrp)
													.get(candidate_index);

											candidate_hist = (int[]) population
													.get(candidate_id)[POP_INDEX_META_PARTNER_HIST];

											// Generate duration of partnership
											int duration;
											if (seeker_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL] > popTime
													|| candidate_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL] > popTime) {
												// Still in partnership - casual partnership
												duration = 1;
											} else {
												// Possible long term partnerships
												AbstractRealDistribution partner_dur = partner_dur_default;
												
												int max_partners = Math.max(seeker_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK],
														candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK]);

												if (max_partners > 1) {
													double mean_dur = numDaysLeft
															/ (max_partners - 1);
													double sd_dur = partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_SD]
															* mean_dur
															/ partnership_setting_seeker[HELPER_NUM_PARTNERSHIP_SETTING_DEFAULT_DURATION_MEAN];

													partner_dur = generateGammaDistribution(RNG,
															new double[] { mean_dur, sd_dur });

												}

												duration = (int) Math.round(partner_dur.sample());
											}

											// Update candidate (male)

											candidate_hist[PARTNER_HIST_INDEX_HISTORY_START
													+ (popTime % time_range)] = pid;
											candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK]--;
											candidate_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL] = Math.max(
													(int) candidate_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL],
													popTime + duration);

											if (candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] <= 0) {
												candidate_by_age.get(targetAgeGrp).remove(candidate_index);
											}

											// Update seeker (female)
											seeker_hist[PARTNER_HIST_INDEX_HISTORY_START
													+ (popTime % time_range)] = candidate_id;
											seeker_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK]--;
											seeker_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL] = Math.max(
													(int) seeker_hist[PARTNER_HIST_INDEX_IN_PARTNERSHIP_UNTIL],
													popTime + duration);

											partnership_added.get(loc)
													.add(new int[] { pid, candidate_id, popTime, duration });

										}

									}

								}
							} // End of for (int pid : active_female) {

						}
					}
					popTime++;
				} // End of while (popTime < snap_start + snap_dur) {

				try {
					PrintWriter pWri;
					File tarFile;
					// Print pop stat
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
							pWri.println("ID,ENTER_POP_AGE,ENTER_POP_AT_TIME,INIT_GRP,EXIT_POP_AT_TIME");
						}
						for (Integer id : pop_stat_final_ids) {
							Object[] person_stat = population.get(id);
							pWri.printf("%d,%s,%s,%s,%s\n", id,
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP].toString(),
									person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT].toString());

						}
						pop_stat_final_ids.clear();
					}
					if (!pop_stat_move_ids.isEmpty()) {
						tarFile = getTargetFile(String.format(POP_STAT_MOVE_LOC_FORMAT, mapSeed));
						boolean append = tarFile.exists();
						pWri = new PrintWriter(new FileWriter(tarFile, append));
						if (!append) {
							pWri.println("TIME,ID,DEST_LOC");
						}
						for (int[] movement : pop_stat_move_ids) {
							pWri.printf("%d,%d,%d\n", movement[0], movement[1], movement[2]);
						}
						schedule_move_grp.clear();
					}

					// Print edge array
					if (partnership_added.isEmpty()) {
						for (Entry<Integer, ArrayList<int[]>> ent : partnership_added.entrySet()) {
							tarFile = getTargetFile(String.format(MAPFILE_FORMAT, ent.getKey(), mapSeed));
							boolean append = tarFile.exists();
							pWri = new PrintWriter(new FileWriter(tarFile, append));
							if (!append) {
								pWri.println("P1,P2,START_TIME,DURATION");
							}
							for (int[] partnership : ent.getValue()) {
								String strRaw = Arrays.toString(partnership);
								pWri.println(strRaw.substring(1, strRaw.length() - 1)); // Exclude [ ] at the end
							}

						}

						partnership_added.clear();
					}

				} catch (IOException e) {

					e.printStackTrace(System.err);
				}

			}

		}

	}

	private void updatePersonPartnershipStatus(int snap_start, int[] candidate_hist, double[] partnership_setting) {
		candidate_hist[PARTNER_HIST_INDEX_VALID_UNTIL] = snap_start
				+ (int) partnership_setting[HELPER_NUM_PARTNERSHIP_SETTING_TIME_RANGE];

		int numOpt = (partnership_setting.length - HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START) / 3;
		int nPI = Arrays.binarySearch(partnership_setting, HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START,
				HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START + numOpt, RNG.nextFloat());

		if (nPI < 0) {
			nPI = ~nPI;
		}

		int min_p = (int) partnership_setting[numOpt + nPI];
		int range_p = (int) partnership_setting[2 * numOpt + nPI] - min_p;
		candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] = min_p;
		if (range_p > 0) {
			candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] += RNG.nextInt(range_p);
		}

		for (int i = PARTNER_HIST_INDEX_HISTORY_START; i < candidate_hist.length
				&& candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK] > 0; i++) {
			if (candidate_hist[i] != 0) {
				candidate_hist[PARTNER_HIST_INDEX_NUM_PARTNER_TO_SEEK]--;
			}
		}
	}

	private double[] getAgeMix(int grp) {
		double[] age_mix;
		if (helper_partner_age_mix == null) {
			helper_partner_age_mix = new HashMap<>();
			double[][] age_mix_ent = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_GRP_MIXING];

			for (double[] numPartnerEnt : age_mix_ent) {
				int grpNum = (int) numPartnerEnt[0];
				double[] ent = Arrays.copyOfRange(numPartnerEnt, 1, numPartnerEnt.length);
				for (int i = 1; i < ent.length; i++) {
					ent[i] = ent[i - 1] + ent[i];
				}
				helper_partner_age_mix.put(grpNum, ent);
			}

		}

		age_mix = helper_partner_age_mix.get(grp);
		if (age_mix == null) {
			age_mix = helper_partner_age_mix.get(~(grp % NUM_AGE_GRP)); // By age
			helper_partner_age_mix.put(grp, age_mix);
		}
		return age_mix;
	}

	private void checkValidMoveDestination(int[] src_gla_index, int[] dest, int[] grpDiff,
			ArrayList<Integer> valid_dest, ArrayList<Float> valid_space) {
		float offset;
		valid_dest.clear();
		valid_space.clear();
		offset = 0;
		for (int dI = 0; dI < dest.length; dI++) {
			int g = getGrpIndex(new int[] { src_gla_index[0], dest[dI], src_gla_index[2] });
			int spaceInGrp = -grpDiff[g];
			if (spaceInGrp > 0) {
				valid_dest.add(dest[dI]);
				valid_space.add(offset + spaceInGrp);
				offset += spaceInGrp;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void removePerson(int remove_pid, Object[] remove_person_stat, HashMap<String, Object> updateObjs) {
		ArrayList<Integer> pop_stat_final_ids = (ArrayList<Integer>) updateObjs.get(UPDATEOBJ_POP_STAT_FINAL_IDS);

		if (pop_stat_final_ids != null) {
			pop_stat_final_ids.add(remove_pid);
		} else {
			System.err.println("Warning! pop_stat_final_ids not found.");
		}
	}

	@SuppressWarnings("unchecked")
	private void schedule_movePerson(int moveTime_from, int move_pid, Object[] person_stat, int newGrp,
			HashMap<String, Object> updateObjs) {

		HashMap<Integer, ArrayList<int[]>> schedule_move_loc = (HashMap<Integer, ArrayList<int[]>>) updateObjs
				.get(UPDATEOBJ_SCHEDULE_MOVE_GRP);

		int moveTime = moveTime_from + RNG.nextInt(snap_dur);

		int orgGrp = getGrpAtTime(person_stat, moveTime_from);
		int srcLoc = getGrpIndex(orgGrp)[1];
		int newLoc = getGrpIndex(newGrp)[1];

		if (srcLoc != newLoc) {
			((ArrayList<Integer>) person_stat[POP_INDEX_META_LOC_MOVE_AT]).add(moveTime);
			((ArrayList<Integer>) person_stat[POP_INDEX_META_LOC_TARGET]).add(newLoc);
			if (schedule_move_loc != null) {
				ArrayList<int[]> move_schedule_at = schedule_move_loc.get(moveTime);
				if (move_schedule_at == null) {
					move_schedule_at = new ArrayList<>();
					schedule_move_loc.put(moveTime, move_schedule_at);
				}
				move_schedule_at.add(new int[] { move_pid, newLoc });
			} else {
				System.err.println("Warning! schedule_move_loc not found.");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private int getGrpAtTime(Object[] person_stat, int time) {
		int time_diff = time - (int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT];
		int age = time_diff + (int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE];
		int[] gla = getGrpIndex((int) person_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP]);

		// Location
		ArrayList<Integer> loc_schedule = (ArrayList<Integer>) person_stat[POP_INDEX_META_LOC_TARGET];
		ArrayList<Integer> loc_moveAt = (ArrayList<Integer>) person_stat[POP_INDEX_META_LOC_MOVE_AT];
		if (!loc_moveAt.isEmpty()) {
			int loc_index = Collections.binarySearch(loc_moveAt, time);
			if (loc_index < 0) {
				loc_index = ~loc_index;
			}
			gla[1] = loc_schedule.get(loc_index);
		}
		// AGE
		int age_index = Arrays.binarySearch(helper_ageRange_all, age);
		if (age_index < 0) {
			age_index = ~age_index;
		}
		gla[2] = age_index;

		return getGrpIndex(gla);

	}

	@SuppressWarnings("unchecked")
	protected int addNewPersonToPopulation(HashMap<Integer, Object[]> population, int nextId, Object[] newPerson,
			HashMap<String, Object> updateObjs) {
		HashMap<Integer, ArrayList<int[]>> schedule_move_grp = (HashMap<Integer, ArrayList<int[]>>) updateObjs
				.get(UPDATEOBJ_SCHEDULE_MOVE_GRP);
		HashMap<Integer, ArrayList<Integer>> active_in_pop = (HashMap<Integer, ArrayList<Integer>>) updateObjs
				.get(UPDATEOBJ_ACTIVE_IN_POP);
		ArrayList<Integer> pop_stat_init_ids = (ArrayList<Integer>) updateObjs.get(UPDATEOBJ_POP_STAT_INIT_IDS);

		population.put(nextId, newPerson);

		// Add and Aging
		if (schedule_move_grp != null) {
			ArrayList<int[]> move_sch;
			// Enter pop time
			int enterTime = (int) newPerson[POP_INDEX_ENTER_POP_AT];
			move_sch = schedule_move_grp.get(enterTime);
			if (enterTime != 0) {
				// Enter on schedule
				if (move_sch == null) {
					move_sch = new ArrayList<>();
					schedule_move_grp.put(enterTime, move_sch);
				}
				move_sch.add(new int[] { -nextId });
			} else {
				// Enter at init time
				int init_grp = (int) newPerson[Abstract_Runnable_ClusterModel.POP_INDEX_GRP];

				ArrayList<Integer> active_in_pop_by_grp = active_in_pop.get(init_grp);
				if (active_in_pop_by_grp == null) {
					active_in_pop_by_grp = new ArrayList<>();
					active_in_pop.put(init_grp, active_in_pop_by_grp);
				}
				active_in_pop_by_grp.add(nextId);
			}
			// Exit Grp time
			int exitTime = (int) newPerson[POP_INDEX_EXIT_POP_AT];
			move_sch = schedule_move_grp.get(exitTime);
			if (move_sch == null) {
				move_sch = new ArrayList<>();
				schedule_move_grp.put(exitTime, move_sch);
			}
			move_sch.add(new int[] { nextId });
		}

		if (pop_stat_init_ids != null) {
			pop_stat_init_ids.add(nextId);
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
			int[][] conncs = (int[][]) getRunnable_fields()[RUNNABLE_FIELD_RMP_CONTACT_MAP_GEN_MULTIMAP_LOC_CONNECTIONS];
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
					cumul_weight += res[1][loc_pt];
					loc_pt++;
				}
				helper_loc_connection.put(ent.getKey(), res);
			}
		}

		res = helper_loc_connection.get(location);
		return res;
	}

	private int[] getAgeRange(int grpNum) {
		if (helper_ageRange_by_grp == null) {
			helper_ageRange_by_grp = new HashMap<>();
			ArrayList<Integer> age_range_all = new ArrayList<>();
			double[][] ageRanges = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_AGE_DIST];
			for (double[] ageRange : ageRanges) {
				helper_ageRange_by_grp.put((int) ageRange[0], new int[] { (int) ageRange[1], (int) ageRange[2] });
				for (int i : new int[] { 1, 2 }) {
					int aI = Collections.binarySearch(age_range_all, (int) ageRange[i]);
					if (aI < 0) {
						age_range_all.add(~aI, (int) ageRange[i]);
					}
				}
			}
			helper_ageRange_all = new int[age_range_all.size()];
			for (int i = 0; i < helper_ageRange_all.length; i++) {
				helper_ageRange_all[i] = age_range_all.get(i);
			}

		}
		int[] res = helper_ageRange_by_grp.get(grpNum);
		if (res == null) {
			res = helper_ageRange_by_grp.get(~(grpNum % NUM_AGE_GRP));
			helper_ageRange_by_grp.put(grpNum, res);
		}
		return res;
	}

	protected int getAge(int pid, int time, HashMap<Integer, Object[]> population) {
		Object[] person = population.get(pid);
		return (int) person[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE]
				+ (time - (int) person[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT]);

	}

	private double[] getNumberParnterSetting(int grp) {
		if (helper_partnership_setting == null) {
			helper_partnership_setting = new HashMap<>();
			double[][] numPartnerEnts = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_PARTNERSHIP_BY_SNAP];

			for (double[] numPartnerEnt : numPartnerEnts) {
				// Format:
				// {AGE_GRP (or ~(grpNum % NUM_AGE_GRP)), time_range,
				// duration_mean, duration_sd,
				// Probability_0.., Min_Partner_0..., Max_Partner_0, ...}
				int grpNum = (int) numPartnerEnt[0];
				double[] ent = Arrays.copyOfRange(numPartnerEnt, 1, numPartnerEnt.length);
				int num_opt = (ent.length - HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START) / 3;

				for (int i = 1; i < num_opt; i++) {
					ent[HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START
							+ i] = ent[HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START + i - 1]
									+ ent[HELPER_NUM_PARTNERSHIP_SETTING_NUM_PARTNER_RANGE_START + i];
				}
				helper_partnership_setting.put(grpNum, ent);
			}

		}

		double[] number_partner_setting = helper_partnership_setting.get(grp);
		if (number_partner_setting == null) {
			number_partner_setting = helper_partnership_setting.get(~(grp % NUM_AGE_GRP)); // By age
			helper_partnership_setting.put(grp, number_partner_setting);
		}

		return number_partner_setting;

	}

}
