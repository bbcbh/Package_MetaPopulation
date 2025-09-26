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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import person.AbstractIndividualInterface;
import relationship.ContactMap;
import util.LineCollectionEntry;

public class Runnable_MetaPopulation_Transmission_RMP_MultiInfection extends Runnable_ClusterModel_MultiTransmission {

	public static final int SITE_VAGINA = 0;
	public static final int SITE_PENIS = SITE_VAGINA + 1;
	public static final int SITE_ANY = SITE_PENIS + 1; // Not Used
	// Initialise during initialse
	protected final File dir_demographic;
	protected int lastIndivdualUpdateTime = 0;

	// Pop stat will be the stat prior to simulation
	protected HashMap<Integer, int[]> indiv_map = new HashMap<>();
	public static final int INDIV_MAP_CURRENT_GRP = 0;
	public static final int INDIV_MAP_ENTER_POP_AGE = INDIV_MAP_CURRENT_GRP + 1;
	public static final int INDIV_MAP_ENTER_POP_AT = INDIV_MAP_ENTER_POP_AGE + 1;
	public static final int INDIV_MAP_EXIT_POP_AT = INDIV_MAP_ENTER_POP_AT + 1;
	public static final int INDIV_MAP_HOME_LOC = INDIV_MAP_EXIT_POP_AT + 1;
	public static final int INDIV_MAP_ENTER_GRP = INDIV_MAP_HOME_LOC + 1;
	public static final int INDIV_MAP_CURRENT_LOC = INDIV_MAP_ENTER_GRP + 1;
	public static final int LENGTH_INDIV_MAP = INDIV_MAP_CURRENT_LOC + 1;

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

	protected int lastTestSch_update = -1;

	protected static final int FIELD_TESTING_RATE_COVERAGE = FIELD_TESTING_RATE_BY_RISK_CATEGORIES_TEST_RATE_PARAM_START;
	protected static final int FIELD_TESTING_TREATMENT_DELAY_START = FIELD_TESTING_RATE_COVERAGE + 1;

	// Key = time
	// V= new Object[] {
	// new int[] {infId, pid_t},
	// (int[][]) inf_stage
	// (int[][]) cumul_treatment_by_person
	protected HashMap<Integer, ArrayList<Object[]>> schedule_treatment = new HashMap<>();

	// Default parameter
	private static final int NUM_INF = 4;
	private static final int NUM_SITE = 3;
	private static final int NUM_ACT = 1;
	private static final int NUM_GRP_PER_GENDER = 3;

	private static final int[] COL_SEL_INF_GENDER = null;

//	private static final int[] COL_SEL_INF_GENDER_SITE_AT = new int[] { 97, 99, 113, 115, 129, 131, // NG Male
//			145, 146, 161, 162, 177, 178, // NG Female
//			193, 195, 209, 211, 225, 227, // CT Male
//			241, 242, 257, 258, 273, 274, // CT Female
//			289, 291, 305, 307, 321, 323, // TV Male
//			337, 338, 353, 354, 369, 370, // TV Female
//	};

	// For infection tracking
	// Key = pid, V = [inf_id][infection_start_time_1,
	// infection_clear_time_1, infection_clear_reason...];
	protected HashMap<Integer, ArrayList<ArrayList<Integer>>> infection_history = new HashMap<>();
	private static final int INFECTION_HIST_CLEAR_NATURAL_RECOVERY = -1;
	private static final int INFECTION_HIST_CLEAR_TREATMENT = -2;
	private static final int INFECTION_HIST_OVERTREATMENT = -3;

	// Movement
	protected HashMap<String, LineCollectionEntry> movementCollections = new HashMap<>();
	protected HashMap<Integer, ArrayList<Integer>> visitor_pids_by_loc = new HashMap<>();
	protected int lastMovement_update = -1;
		
	// Output
	protected static final String key_pop_size = "EXPORT_POP_SIZE";
	protected static final String FILENAME_EXPORT_POP_SIZE = "Pop_size_%d_%d.csv";

	private boolean preloadFile = false;

	public Runnable_MetaPopulation_Transmission_RMP_MultiInfection(long cMap_seed, long sim_seed, Properties prop) {
		super(cMap_seed, sim_seed, null, prop, NUM_INF, NUM_SITE, NUM_ACT);
		this.setBaseDir((File) prop.get(Simulation_MetaPop.PROP_BASEDIR));
		this.setBaseProp(prop);
		this.dir_demographic = new File(baseProp.getProperty(Simulation_MetaPop.PROP_CONTACT_MAP_LOC));

		Pattern pattern_movement_csv = Pattern.compile(Runnable_Demographic_Generation.FILENAME_FORMAT_MOVEMENT
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
				LineCollectionEntry ent = new LineCollectionEntry(mv, preloadFile);
				ent.loadNextLine();
				ent.loadNextLine(); // Skip Header
				movementCollections.put(m.group(1), ent);
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}

		

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
		for (ArrayList<Integer> pid_by_grp : current_pids_by_gps.values()) {
			pids.addAll(pid_by_grp);
		}
		return pids.toArray(new Integer[0]);
	}

	@Override
	public int getPersonGrp(Integer personId) {
		int[] indiv_stat = indiv_map.get(personId);
		int grp = indiv_stat[INDIV_MAP_CURRENT_GRP];
		if (grp < 0) { // Expired person
			grp = ((indiv_stat[INDIV_MAP_ENTER_GRP] / NUM_GRP_PER_GENDER) + 1) * NUM_GRP_PER_GENDER - 1;
		}
		return grp;
	}

	@Override
	protected void handleRemovePerson(Integer pid) {
		// Do nothing
	}

	@Override
	public void scheduleNextTest(Integer personId, int lastTestTime, int mustTestBefore, int last_test_infIncl,
			int last_test_siteIncl) {
		// Do nothing as testing is set up through initialisation and postTimeStep
	}

	@Override
	protected void postTimeStep(int currentTime) {
		super.postTimeStep(currentTime);
		if (lastTestSch_update >= 0
				&& (currentTime + 1) == lastTestSch_update + AbstractIndividualInterface.ONE_YEAR_INT) {
			setAnnualTestingSchdule(currentTime + 1);
			lastTestSch_update = currentTime;
		}
		// Delay treatment
		ArrayList<Object[]> sch_tr = schedule_treatment.remove(currentTime);
		if (sch_tr != null) {
			for (Object[] ent : sch_tr) {
				int[] int_stat = (int[]) ent[0];
				int infId = int_stat[0];
				int pid = int_stat[1];
				int[][] cumul_treatment_by_person = (int[][]) ent[1];
				cumul_treatment_by_person[infId][getPersonGrp(pid)]++;
				applyTreatment(currentTime, infId, pid, map_currrent_infection_stage.get(pid));
			}
		}
		// Movement
		loadMovement(currentTime);

		

		// Store pop size
		if (currentTime % nUM_TIME_STEPS_PER_SNAP == 0) {
			@SuppressWarnings("unchecked")
			HashMap<Integer, int[]> countMap = (HashMap<Integer, int[]>) sim_output.get(key_pop_size);
			if (countMap == null) {
				countMap = new HashMap<>();
				sim_output.put(key_pop_size, countMap);
			}
			int[] pop_size = new int[NUM_GRP];
			for (int g = 0; g < pop_size.length; g++) {
				pop_size[g] = current_pids_by_gps.get(g).size();
			}
			countMap.put(currentTime, pop_size);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	protected void postSimulation() {
		super.postSimulation();
		String key, fileName;
		HashMap<Integer, int[]> countMap;
		String filePrefix = this.getRunnableId() == null ? "" : this.getRunnableId();
		PrintWriter pWri;

		key = key_pop_size;

		countMap = (HashMap<Integer, int[]>) sim_output.get(key);
		fileName = String.format(filePrefix + FILENAME_EXPORT_POP_SIZE, cMAP_SEED, sIM_SEED);
		printCountMap(countMap, fileName, "Group_%d", new int[] { NUM_GRP }, null);

		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_GEN_INCIDENCE_FILE) != 0) {

			key = String.format(SIM_OUTPUT_KEY_CUMUL_INCIDENCE,
					Simulation_ClusterModelTransmission.SIM_SETTING_KEY_GEN_INCIDENCE_FILE);
			countMap = (HashMap<Integer, int[]>) sim_output.get(key);
			fileName = String.format(filePrefix + Simulation_ClusterModelTransmission.FILENAME_CUMUL_INCIDENCE_PERSON,
					cMAP_SEED, sIM_SEED);
			printCountMap(countMap, fileName, "Inf_%d_Group_%d", new int[] { NUM_INF, NUM_GRP }, COL_SEL_INF_GENDER);

		}

		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_GEN_PREVAL_FILE) != 0) {

			key = String.format(SIM_OUTPUT_KEY_INFECTIOUS_GENDER_COUNT,
					Simulation_ClusterModelTransmission.SIM_SETTING_KEY_GEN_PREVAL_FILE);
			countMap = (HashMap<Integer, int[]>) sim_output.get(key);
			fileName = String.format(
					filePrefix + "Infectious_" + Simulation_ClusterModelTransmission.FILENAME_PREVALENCE_PERSON,
					cMAP_SEED, sIM_SEED);
			printCountMap(countMap, fileName, "Inf_%d_Gender_%d", new int[] { NUM_INF, NUM_GRP }, COL_SEL_INF_GENDER);

//			key = String.format(SIM_OUTPUT_KEY_INFECTED_AT_GENDER_COUNT,
//					Simulation_ClusterModelTransmission.SIM_SETTING_KEY_GEN_PREVAL_FILE);
//			countMap = (HashMap<Integer, int[]>) sim_output.get(key);
//			fileName = String.format(
//					filePrefix + "Infected_" + Simulation_ClusterModelTransmission.FILENAME_PREVALENCE_SITE, cMAP_SEED,
//					sIM_SEED);
//			printCountMap(countMap, fileName, "Inf_%d_Gender_%d_Infected_SiteInc_%d",
//					new int[] { NUM_INF, NUM_GRP, 1 << (NUM_SITE + 1) }, COL_SEL_INF_GENDER_SITE_AT);

		}
		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_TRACK_INFECTION_HISTORY) > 0) {

			Integer[] pids = infection_history.keySet().toArray(new Integer[infection_history.size()]);
			Arrays.sort(pids);
			try {
				pWri = new PrintWriter(new File(baseDir,
						String.format(filePrefix + Simulation_ClusterModelTransmission.FILENAME_INFECTION_HISTORY,
								cMAP_SEED, sIM_SEED)));
				for (Integer pid : pids) {
					ArrayList<ArrayList<Integer>> hist = infection_history.get(pid);
					for (int infId = 0; infId < hist.size(); infId++) {
						pWri.print(pid.toString());
						pWri.print(',');
						pWri.print(infId);
						for (Integer timeEnt : hist.get(infId)) {
							pWri.print(',');
							pWri.print(timeEnt);
						}
						pWri.println();
					}
				}

				pWri.close();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}

		

		for (LineCollectionEntry movementEntry : movementCollections.values()) {
			try {
				movementEntry.closeReader();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	protected int initaliseCMap(ContactMap cMap, Integer[][] edges_array, int edges_array_pt, int startTime,
			HashMap<Integer, ArrayList<Integer[]>> removeEdges) {
		int res = super.initaliseCMap(cMap, edges_array, edges_array_pt, startTime, removeEdges);
		lastTestSch_update = startTime;
		setAnnualTestingSchdule(startTime);

		loadMovement(startTime);

		return res;
	}

	@Override
	protected void testPerson(int currentTime, int pid_t, int infIncl, int siteIncl,
			int[][] cumul_treatment_by_person) {
		if (pid_t < 0) { // Assume test and treat as normal with symptoms
			super.testPerson(currentTime, pid_t, infIncl, siteIncl, cumul_treatment_by_person);
		} else {
			double[][] testRateDefs = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_TRANSMISSION_TESTING_RATE_BY_RISK_CATEGORIES];
			// Check which testRateDef fit
			double[] testRateDefMatch = null;
			int pid = pid_t;
			for (double[] testRateDef : testRateDefs) {
				
				int gIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_GENDER_INCLUDE_INDEX];
				int sIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_SITE_INCLUDE_INDEX];
				int iIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_INF_INCLUDE_INDEX];
				if ((1 << getPersonGrp(pid) & gIncl) != 0 && sIncl == siteIncl && iIncl == infIncl) {
					testRateDefMatch = testRateDef;
				}
			}
			if (testRateDefMatch == null) {
				System.err.printf(
						"Warning!. Mating test defintion for [%d,%d,%d,%d] NOT found. Use default test person instead.\n",
						currentTime, pid, infIncl, siteIncl);
				super.testPerson(currentTime, pid, infIncl, siteIncl, cumul_treatment_by_person);
			} else {							
				for (int infId = 0; infId < NUM_INF; infId++) {
					if ((infIncl & 1 << infId) != 0) {
						boolean applyTreatment = false;
						double[] test_properties;
						int[][] inf_stage = null;
						int tested_stage_inc;
						for (int siteId = 0; siteId < NUM_SITE && !applyTreatment; siteId++) {
							// Test for the site
							test_properties = lookupTable_test_treatment_properties
									.get(String.format("%d,%d", infId, siteId));

							if (test_properties != null) {
								inf_stage = map_currrent_infection_stage.get(pid);
								if (inf_stage != null && inf_stage[infId][siteId] >= 0) {
									double testSensitivity = 0;
									int stage_pt = FIELD_DX_TEST_PROPERTIES_ACCURACY_START;
									while (testSensitivity == 0 && stage_pt < test_properties.length) {
										// TEST_ACCURACY_1, TARGET_STAGE_INC_1, TREATMENT_SUC_STAGE_1 ..
										tested_stage_inc = (int) test_properties[stage_pt + 1];
										if ((tested_stage_inc & 1 << inf_stage[infId][siteId]) != 0) {
											testSensitivity = test_properties[stage_pt];
										}
										stage_pt += 3;
									}
									if (testSensitivity > 0) {
										applyTreatment |= RNG.nextDouble() < testSensitivity;
									}
								}
							}
						}
						if (applyTreatment) {
							
							//TODO: Set up multiple delay option here
							int numTreatmentDelayOption = (testRateDefMatch.length
									- FIELD_TESTING_TREATMENT_DELAY_START) / 2;

							double pTreat = RNG.nextDouble();
							int pt = Arrays.binarySearch(testRateDefMatch, FIELD_TESTING_TREATMENT_DELAY_START,
									FIELD_TESTING_TREATMENT_DELAY_START + numTreatmentDelayOption, pTreat);

							if (pt < 0) {
								pt = ~pt;
							}
							if ((pt + numTreatmentDelayOption + 1) < testRateDefMatch.length) { // Miss out on treatment
																								// otherwise
								int delay = (int) testRateDefMatch[pt + numTreatmentDelayOption];
								delay += RNG.nextInt((int) testRateDefMatch[pt + numTreatmentDelayOption + 1] - delay);
								if (delay <= 1) {
									cumul_treatment_by_person[infId][getPersonGrp(pid)]++;
									applyTreatment(currentTime, infId, pid, inf_stage);
								} else {
									ArrayList<Object[]> sch_treat = schedule_treatment.get(currentTime + delay);
									if (sch_treat == null) {
										sch_treat = new ArrayList<>();
										schedule_treatment.put(currentTime + delay, sch_treat);
									}
									sch_treat.add(new Object[] { new int[] { infId, pid }, cumul_treatment_by_person });
								}
							}
						}
					}
				}

			}

		}
	}

	@Override
	protected void applyTreatment(int currentTime, int infId, int pid, int[][] inf_stage) {

		int[] preTreatment_stage = Arrays.copyOf(inf_stage[infId], inf_stage[infId].length);

		super.applyTreatment(currentTime, infId, pid, inf_stage);

		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_TRACK_INFECTION_HISTORY) > 0) {
			ArrayList<Integer> infHist = infection_history.get(pid).get(infId);

			boolean nonInfected = true;
			boolean treatment_suc = false;

			for (int i = 0; i < preTreatment_stage.length; i++) {
				nonInfected &= preTreatment_stage[i] == AbstractIndividualInterface.INFECT_S;
				treatment_suc |= preTreatment_stage[i] >= 0 && preTreatment_stage[i] != inf_stage[infId][i];
			}
			if (nonInfected) {
				infHist.add(currentTime);
				infHist.add(currentTime);
				infHist.add(INFECTION_HIST_OVERTREATMENT);
			} else if (treatment_suc) {
				if (infHist.get(infHist.size() - 1) > 0) {
					infHist.add(currentTime);
					infHist.add(INFECTION_HIST_CLEAR_TREATMENT);
				} else {
					System.err.printf("Infection history error: %s -> %s.\n", Arrays.toString(preTreatment_stage),
							Arrays.toString(inf_stage[infId]));

				}
			}
		}

	}

	@Override
	public int addInfectious(Integer infectedPId, int infectionId, int site_id, int stage_id, int infectious_time,
			int state_duration_adj) {
		int res = super.addInfectious(infectedPId, infectionId, site_id, stage_id, infectious_time, state_duration_adj);

		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_TRACK_INFECTION_HISTORY) > 0) {
			ArrayList<ArrayList<Integer>> hist_all = infection_history.get(infectedPId);
			if (hist_all == null) {
				hist_all = new ArrayList<>();
				for (int i = 0; i < NUM_INF; i++) {
					hist_all.add(new ArrayList<>());
				}
				infection_history.put(infectedPId, hist_all);
			}
			ArrayList<Integer> hist_by_inf = hist_all.get(infectionId);
			// Check for new infection (i.e. previously recovered naturally or through
			// treatment
			if (hist_by_inf.size() == 0 || hist_by_inf.get(hist_by_inf.size() - 1) < 0) {
				hist_by_inf.add(infectious_time);
			}
		}
		return res;
	}

	@Override
	protected int[] handleNoNextStage(Integer pid, int infection_id, int site_id, int current_infection_stage,
			int current_time) {
		int[] res = super.handleNoNextStage(pid, infection_id, site_id, current_infection_stage, current_time);
		// res = {next_stage, duration}
		if ((simSetting & 1 << Simulation_ClusterModelTransmission.SIM_SETTING_KEY_TRACK_INFECTION_HISTORY) > 0) {
			ArrayList<Integer> infhist = infection_history.get(pid).get(infection_id);
			if (infhist.size() > 0 && infhist.get(infhist.size() - 1) > 0) {
				// Key=PID,V=int[INF_ID][SITE]{infection_stage}
				int[] inf_stat = map_currrent_infection_stage.get(pid)[infection_id];
				boolean all_clear = true;
				for (int s = 0; s < inf_stat.length; s++) {
					all_clear &= (s == site_id ? res[0] : inf_stat[s]) == AbstractIndividualInterface.INFECT_S;
				}
				if (all_clear) {
					ArrayList<Integer> infHist = infection_history.get(pid).get(infection_id);
					infHist.add(current_time);
					infHist.add(INFECTION_HIST_CLEAR_NATURAL_RECOVERY);
				}
			}

		}

		return res;
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

	protected void setAnnualTestingSchdule(int testStartTime) {
		double[][] testRateDefs = (double[][]) getRunnable_fields()[RUNNABLE_FIELD_TRANSMISSION_TESTING_RATE_BY_RISK_CATEGORIES];
		for (double[] testRateDef : testRateDefs) {
			int gIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_GENDER_INCLUDE_INDEX];
			int num_test_candidate_per_def = 0;
			// Risk group not used
			// int rIncl = (int)
			// testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_RISK_GRP_INCLUDE_INDEX];

			int sIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_SITE_INCLUDE_INDEX];
			int iIncl = (int) testRateDef[FIELD_TESTING_RATE_BY_RISK_CATEGORIES_INF_INCLUDE_INDEX];

			for (int g = 0; g < NUM_GRP; g++) {
				if ((gIncl & 1 << g) != 0) {
					num_test_candidate_per_def += current_pids_by_gps.get(g).size();
				}
			}
			int num_tests_peformed_per_def = (int) Math
					.round(num_test_candidate_per_def * testRateDef[FIELD_TESTING_RATE_COVERAGE]);

			int person_index = 0;
			for (int g = 0; g < NUM_GRP; g++) {
				if ((gIncl & 1 << g) != 0) {
					for (Integer pid : current_pids_by_gps.get(g)) {
						if (RNG.nextInt(num_test_candidate_per_def - person_index) < num_tests_peformed_per_def) {
							int testDate = testStartTime + RNG.nextInt(AbstractIndividualInterface.ONE_YEAR_INT);
							if (testDate < exitPopAt(pid)) {
								ArrayList<int[]> day_sch = schedule_testing.get(testDate);
								if (day_sch == null) {
									day_sch = new ArrayList<>();
									schedule_testing.put(testDate, day_sch);
								}
								int[] test_entry = new int[] { pid, iIncl, sIncl };
								int pt = Collections.binarySearch(day_sch, test_entry, new Comparator<int[]>() {
									@Override
									public int compare(int[] o1, int[] o2) {
										int res = 0;
										int pt_arr = 0;
										while (res == 0 && pt_arr < 3) {
											res = Integer.compare(o1[pt_arr], o2[pt_arr]);
											pt_arr++;
										}
										return res;
									}
								});
								if (pt < 0) {
									day_sch.add(~pt, test_entry);
								}
								num_tests_peformed_per_def--;
							}
						}
						person_index++;
					}
				}
			}

		}
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
			indiv_stat[INDIV_MAP_CURRENT_LOC] = indiv_stat[INDIV_MAP_HOME_LOC];

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

				int nextGrp = checkGrp + 1;

				if (indiv_stat[INDIV_MAP_EXIT_POP_AT] > 0 && indiv_stat[INDIV_MAP_EXIT_POP_AT] < changeTime) {
					changeTime = indiv_stat[INDIV_MAP_EXIT_POP_AT];
					nextGrp = -1;
					reach_max_age_grp = true;
				}

				if ((checkGrp % NUM_GRP_PER_GENDER) + 1 < NUM_GRP_PER_GENDER) {
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
