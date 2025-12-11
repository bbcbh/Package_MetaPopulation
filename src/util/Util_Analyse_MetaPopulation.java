package util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sim.Abstract_Runnable_ClusterModel;
import sim.Runnable_MetaPopulation_MultiTransmission;
import sim.Simulation_ClusterModelTransmission;

public class Util_Analyse_MetaPopulation {

	public static void extract_inf_num_infection_to_csv(File scenario_dirs_incl, int[][] colIndex, String fname)
			throws IOException, FileNotFoundException {
		extract_inf_stat_to_csv(new File[] { scenario_dirs_incl }, scenario_dirs_incl, colIndex,
				"Infectious_Prevalence_Person_", fname);
	}

	public static void extract_inf_incidence_to_csv(File scenario_dirs_incl, int[][] colIndex, String fname)
			throws IOException, FileNotFoundException {
		extract_inf_stat_to_csv(new File[] { scenario_dirs_incl }, scenario_dirs_incl, colIndex, "Incidence_Person_",
				fname);
	}

	public static void extract_inf_stat_to_csv(File[] scenario_dirs_incl, File output_dir, int[][] colIndex,
			String csvFeader, String fileOutputFormat) throws IOException, FileNotFoundException {
		Comparator<File> cmp_file_suffix = generate_file_comparator_by_suffix();

		ArrayList<String> array_qsub = new ArrayList<>();
		ArrayList<ArrayList<StringBuilder>> lines_all_inf = new ArrayList<>();
		for (int p = 0; p < colIndex.length; p++) {
			lines_all_inf.add(new ArrayList<>());
		}

		for (File resultSetDir : scenario_dirs_incl) {
			File[] singleResultSets = resultSetDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isDirectory() && pathname.getName().startsWith(resultSetDir.getName());
				}
			});

			Arrays.sort(singleResultSets, cmp_file_suffix);

			Pattern pattern_num_inf_src = Pattern.compile(csvFeader + "(-?\\d+).csv.7z");

			boolean completedSet = true;
			for (File singleResultSet : singleResultSets) {
				File[] zips;
				zips = singleResultSet.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pattern_num_inf_src.matcher(pathname.getName()).matches();
					}
				});

				if (zips.length != 1) {
					System.err.printf("Error. Number of zip in %s != 1\n", singleResultSet.getName());
					array_qsub.add(String.format("qsub %s.pbs\n", singleResultSet.getName()));
					completedSet = false;
				} else if (zips[0].length() == 0) {
					System.err.printf("Error. Zip file of length 0 in %s != 1\n", singleResultSet.getName());
					array_qsub.add(String.format("qsub %s.pbs\n", singleResultSet.getName()));
					completedSet = false;
				}

			}

			if (!completedSet) {
				for (String resub : array_qsub) {
					System.out.print(resub);
				}

				System.exit(-1);
			}

			for (File singleResultSet : singleResultSets) {

				// System.out.printf("Current Result Set: %s\n",
				// singleResultSet.getAbsolutePath());

				File[] zips;

				zips = singleResultSet.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pattern_num_inf_src.matcher(pathname.getName()).matches();
					}
				});

				if (zips.length != 1) {
					System.err.printf("Error. Number of zip in %s != 1\n", singleResultSet.getAbsolutePath());
				}

				// Should have one map only
				HashMap<String, ArrayList<String[]>> linesMap = util.Util_7Z_CSV_Entry_Extract_Callable
						.extractedLinesFrom7Zip(zips[0]);

				Pattern pattern_inf_stat_header = Pattern
						.compile("\\[(.+),(\\d+)\\]" + csvFeader + "(-?\\d+)_(-?\\d+).csv");

				String[] keys = linesMap.keySet().toArray(new String[0]);
				Arrays.sort(keys, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						Matcher m1 = pattern_inf_stat_header.matcher(o1);
						Matcher m2 = pattern_inf_stat_header.matcher(o2);
						if (m1.matches() && m2.matches()) {
							int res = Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
							if (res == 0) {
								res = Long.compare(Long.parseLong(m1.group(2)), Long.parseLong(m2.group(2)));
							}
							return res;

						} else {
							return o1.compareTo(o2);
						}
					}
				});

				for (String key : keys) {
					ArrayList<String[]> lines_from_sim = linesMap.get(key);
					for (int p = 0; p < colIndex.length; p++) {
						ArrayList<StringBuilder> lines_inf = lines_all_inf.get(p);
						for (int lineNum = 0; lineNum < lines_from_sim.size(); lineNum++) {
							String[] line_ent = lines_from_sim.get(lineNum);
							while (lineNum >= lines_inf.size()) {
								lines_inf.add(new StringBuilder());
							}
							StringBuilder strBuild = lines_inf.get(lineNum);
							if (strBuild.length() == 0) {
								strBuild.append(line_ent[0]);
							}
							strBuild.append(',');
							if (lineNum == 0) {
								Matcher m = pattern_inf_stat_header.matcher(key);
								if (m.find()) {
									strBuild.append(resultSetDir.getName());
									strBuild.append(':');
									strBuild.append(singleResultSet.getName());
									strBuild.append('(');
									strBuild.append(m.group(2));
									strBuild.append('_');
									strBuild.append(m.group(3));
									strBuild.append('_');
									strBuild.append(m.group(4));
									strBuild.append(')');
								} else {
									strBuild.append(key);
								}
							} else {
								int numInf = 0;
								for (int col : colIndex[p]) {
									numInf += Integer.parseInt(line_ent[col]);
								}
								strBuild.append(numInf);
							}

						}
					}
				}

			}

			PrintWriter[] pWriters_num_infect = new PrintWriter[colIndex.length];
			for (int p = 0; p < colIndex.length; p++) {
				pWriters_num_infect[p] = new PrintWriter(new File(output_dir, String.format(fileOutputFormat, p)));
				for (StringBuilder lines : lines_all_inf.get(p)) {
					pWriters_num_infect[p].println(lines.toString());
				}
				pWriters_num_infect[p].close();
			}

		}
	}

	public static Comparator<File> generate_file_comparator_by_suffix() {
		Pattern pattern_suffix = Pattern.compile(".*_(\\d+)");
		Comparator<File> cmp_file_suffix = new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Matcher m1 = pattern_suffix.matcher(o1.getName());
				Matcher m2 = pattern_suffix.matcher(o2.getName());
				m1.find();
				m2.find();
				return Integer.compare(Integer.parseInt(m1.group(1)), Integer.parseInt(m2.group(1)));
			}
		};
		return cmp_file_suffix;
	}

	public static void extract_infection_history_to_csv(File scenario_dir, HashMap<Integer, String[]> indiv_map_by_cmap,
			int switchTime, int[] inf_modelled) throws IOException, FileNotFoundException {

		Pattern pattern_suffix = Pattern.compile(".*_(\\d+)");
		Comparator<File> cmp_file_suffix = new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Matcher m1 = pattern_suffix.matcher(o1.getName());
				Matcher m2 = pattern_suffix.matcher(o2.getName());
				m1.find();
				m2.find();
				return Integer.compare(Integer.parseInt(m1.group(1)), Integer.parseInt(m2.group(1)));
			}
		};

		ArrayList<ArrayList<StringBuilder>> lines_all_inf = new ArrayList<>();
		for (int p = 0; p < inf_modelled.length; p++) {
			lines_all_inf.add(new ArrayList<>());
		}

		File[] resultSetDirs = scenario_dir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory() && pathname.getName().startsWith(scenario_dir.getName());
			}
		});
		Arrays.sort(resultSetDirs, cmp_file_suffix);

		for (File resultSetDir : resultSetDirs) {

			PrintWriter[] pWriters = new PrintWriter[inf_modelled.length];

			Pattern pattern_inf_hist_src = Pattern.compile("InfectHist_(-?\\d+).csv.7z");
			for (int p = 0; p < pWriters.length; p++) {
				pWriters[p] = new PrintWriter(
						new File(resultSetDir, String.format("Inf_%d_duration.csv", inf_modelled[p])));
				pWriters[p].println(String.format("Infection_duration_from_%d,Recovery_Reason,Start_Grp", switchTime));
			}

			File[] zips = resultSetDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pattern_inf_hist_src.matcher(pathname.getName()).matches();
				}
			});

			for (File zipFile : zips) {

				HashMap<String, ArrayList<String[]>> res = util.Util_7Z_CSV_Entry_Extract_Callable
						.extractedLinesFrom7Zip(zipFile);
				for (ArrayList<String[]> lines : res.values()) {
					for (String[] lineEnt : lines) {
						// pid,inf_id,infection_start_time_1, infection_clear_time_1,
						// infection_clear_reason...
						int col = Arrays.binarySearch(inf_modelled, Integer.parseInt(lineEnt[1]));
						if (col >= 0) {
							for (int start = 2; start < lineEnt.length; start += 3) {
								if (start + 2 < lineEnt.length) {
									int inf_start = Integer.parseInt(lineEnt[start]);
									int inf_finished = Integer.parseInt(lineEnt[start + 1]);
									int dur = inf_finished - inf_start;
									if (inf_finished >= switchTime) {
										pWriters[col].printf("%d,%d,%s\n", dur, Integer.parseInt(lineEnt[start + 2]),
												indiv_map_by_cmap.get(Integer.parseInt(lineEnt[0]))[1]);
									}
								}
							}
						}
					}
				}

			}
		}

	}

	public static HashMap<Long, HashMap<Integer, String[]>> generate_demographic_mapping_from_file(File demograhicDir)
			throws FileNotFoundException, IOException {
		HashMap<Long, HashMap<Integer, String[]>> map_indiv_map = new HashMap<>();
		Pattern pattern_demograhic = Pattern.compile("POP_STAT_(-?\\d+).csv");
		File[] demographicFiles = demograhicDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pattern_demograhic.matcher(pathname.getName()).matches();
			}
		});
		for (File file_demo : demographicFiles) {
			Matcher m = pattern_demograhic.matcher(file_demo.getName());
			m.find();
			long key = Long.parseLong(m.group(1));
			String[] demographic = util.Util_7Z_CSV_Entry_Extract_Callable.extracted_lines_from_text(file_demo);

			HashMap<Integer, String[]> indiv_map = new HashMap<>();
			for (int i = 1; i < demographic.length; i++) {
				String line = demographic[i];
				String[] sp = line.split(",");
				indiv_map.put(Integer.parseInt(sp[0]), sp);
			}
			map_indiv_map.put(key, indiv_map);
		}
		return map_indiv_map;
	}

	public final static String SETTING_GLOBAL = "SETTING_GLOBAL"; // Map<String, Object>
	public final static String SETTING_GRP_INCL = "SETTING_GRP_INCL"; // Integer
	public final static String SETTING_INF_INCL = "SETTING_INF_INCL"; // Integer
	public final static String SETTING_SAMPLE_FREQ = "SETTING_SAMPLE_FREQ"; // Integer
	public final static String SETTING_EVENT_COUNT_MIN_LIMIT = "SETTING_EVENT_COUNT_MIN_LIMIT"; // Integer
	public final static String SETTING_PROB_MAP = "SETTING_PROB_MAP"; // Map<Integer, double[]>, with key = inf_id

	public final static String SETTING_BY_INDIVDUAL_AGE = "SETTING_BY_INDIVDUAL_AGE"; // int[]
	public final static String SETTING_PROB_COUNT_MAX_LIMIT = "SETTING_PROB_COUNT_MAX_LIMIT"; // Integer
	public final static String SETTING_SUBOUTCOMES = "SETTING_SUBOUTCOMES"; // String[]

	public final static Pattern sim_key_pattern = Pattern.compile("(.*):.*_(\\d+)\\((-?\\d+)_(-?\\d+)_(-?\\d+)\\)");
	
	public final static Map<String, Class<?>> CLASSMAP_SETTING = Map.ofEntries(
			Map.entry(SETTING_GRP_INCL, Integer.class),
			Map.entry(SETTING_INF_INCL, Integer.class),
			Map.entry(SETTING_SAMPLE_FREQ, Integer.class),
			Map.entry(SETTING_EVENT_COUNT_MIN_LIMIT, Integer.class),
			Map.entry(SETTING_PROB_MAP, Map.class),
			Map.entry(SETTING_BY_INDIVDUAL_AGE, int[].class),
			Map.entry(SETTING_PROB_COUNT_MAX_LIMIT, Integer.class),			
			Map.entry(SETTING_SUBOUTCOMES, String[].class));
	

	// morbidity_setting_all = Number[] {grp_incl}

	public static void extracted_InfectHist(File[] sce_dir, File output_dir, int[] sample_time,
			HashMap<String, ArrayList<String>> sim_sel_map, HashMap<Long, HashMap<Integer, String[]>> demographic,
			String[] morbidity_key_arr, Map<String, Map<String, Object>> morbidity_setting_all)
			throws IOException, FileNotFoundException {
		Pattern pattern_inf_hist_zip = Pattern.compile(
				Simulation_ClusterModelTransmission.FILENAME_INFECTION_HISTORY_ZIP.replaceAll("%d", "(-?\\\\d+)"));
		Pattern pattern_inf_preval_header = Pattern.compile(String.format("\\[(.+),(\\d+)\\]%s",
				Simulation_ClusterModelTransmission.FILENAME_INFECTION_HISTORY.replaceAll("%d", "(-?\\\\d+)")));

		final int GRP_INDEX_INF_HISTORY_HEADER_ROW_INDEX = 2;
		final int GRP_INDEX_INF_HISTORY_HEADER_CMAP_SEED = 3;
		final int GRP_INDEX_INF_HISTORY_HEADER_SIM_SEED = 4;

		String[] dir_suffix_sel = sim_sel_map.keySet().toArray(new String[0]);
		Arrays.sort(dir_suffix_sel);

		// Global factor - only support grp and inf so far
		int global_grp_incl = Integer.MAX_VALUE;
		int gloval_inf_incl = Integer.MAX_VALUE;

		Map<String, Object> global_map = (Map<String, Object>) morbidity_setting_all.get(SETTING_GLOBAL);

		if (global_map != null) {
			for (Entry<String, Object> ent_global : global_map.entrySet()) {
				if (ent_global.getKey().equals(SETTING_GRP_INCL)) {
					global_grp_incl = ((Integer) ent_global.getValue()).intValue();
				} else if (ent_global.getKey().equals(SETTING_INF_INCL)) {
					gloval_inf_incl = ((Integer) ent_global.getValue()).intValue();
				} else {
					System.err.printf("Warning!. Global setting \"%s\" not defined.\n");
				}
			}
		}

		// Set up PrintWriters
		ArrayList<String> priWriter_keys_list = new ArrayList<>();
		HashMap<String, PrintWriter> priWriter_map = new HashMap<>();

		for (String morbid_key : morbidity_key_arr) {
			// Main PrintWriter
			String priWriterMapKey = morbid_key;
			priWriter_keys_list.add(priWriterMapKey);

			// Sub_outcome
			String[] morbidity_sub_outcome = (String[]) morbidity_setting_all.get(morbid_key).get(SETTING_SUBOUTCOMES);
			if (morbidity_sub_outcome != null) {
				for (String sub_outcome_name : morbidity_sub_outcome) {
					priWriter_keys_list.add(sub_outcome_name);
				}
			}
		}
		for (String priWriKey : priWriter_keys_list) {
			PrintWriter pWri = new PrintWriter(new File(output_dir, priWriKey));
			priWriter_map.put(priWriKey, pWri);

			pWri.print("SIM_ID");
			int time_gap = 1;
			if (morbidity_setting_all.get(priWriKey).containsKey(SETTING_SAMPLE_FREQ)) {
				time_gap = ((Number) morbidity_setting_all.get(priWriKey).get(SETTING_SAMPLE_FREQ)).intValue();

			}
			int prob_count = 0;
			if (morbidity_setting_all.get(priWriKey).containsKey(SETTING_PROB_COUNT_MAX_LIMIT)) {
				prob_count = (Integer) morbidity_setting_all.get(priWriKey).get(SETTING_PROB_COUNT_MAX_LIMIT);
			}

			int s_time = sample_time[0];
			int s_end = sample_time[sample_time.length - 1];
			String prefix = "";

			if (morbidity_setting_all.get(priWriKey).containsKey(SETTING_BY_INDIVDUAL_AGE)) {
				int[] age_offset = (int[]) morbidity_setting_all.get(priWriKey).get(SETTING_BY_INDIVDUAL_AGE);
				s_time = age_offset[0];
				s_end = age_offset[1];
				prefix = "Age_";
			}

			while (s_time <= s_end) {
				if (prob_count > 0) {
					for (int i = 0; i <= prob_count; i++) {
						pWri.print(',');
						pWri.print(prefix);
						pWri.print(s_time);
						pWri.print("_P(");
						pWri.print(i);
						pWri.print(')');
					}

				} else {
					pWri.print(',');
					pWri.print(prefix);
					pWri.print(s_time);
				}
				s_time += time_gap;
			}
			pWri.println();
			pWri.flush();
		}

		for (File resultSetDir : sce_dir) {
			File[] singleResultSets = resultSetDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					boolean res = pathname.isDirectory();
					res &= pathname.getName().startsWith(resultSetDir.getName());
					if (res && dir_suffix_sel.length > 0) { // Otherwise include all
						String suffix = pathname.getName().substring(resultSetDir.getName().length() + 1);
						res &= Arrays.binarySearch(dir_suffix_sel, suffix) >= 0;
					}
					return res;
				}
			});

			for (File singleResultSet : singleResultSets) {
				// Reading files

				String sim_sel_key = singleResultSet.getName().substring(resultSetDir.getName().length() + 1);
				String[] sim_sel_values = null;

				// Lv 1: (String) simKey
				// Lv 2: (String) morbid_key
				HashMap<String, Map<String, String>> output_map_all = new HashMap<>();

				if (sim_sel_map.containsKey(sim_sel_key)) {
					sim_sel_values = sim_sel_map.get(sim_sel_key).toArray(new String[0]);
					Arrays.sort(sim_sel_values);
				}

				File[] inf_hist_zips = singleResultSet.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pattern_inf_hist_zip.matcher(pathname.getName()).matches();
					}
				});

				if (inf_hist_zips.length != 1) {
					System.err.printf("Error. Number of zip in %s != 1\n", singleResultSet.getAbsolutePath());
				} else {
					// Start of thread potentially
					File outcomeZipFile = inf_hist_zips[0];

					long tic = System.currentTimeMillis();

					// Lv 1: (String) simKey
					// Lv 2: (String) morbid_key
					HashMap<String, Map<String, String>> output_map = new HashMap<>();

					// Should have one map only
					HashMap<String, ArrayList<String[]>> linesMap = util.Util_7Z_CSV_Entry_Extract_Callable
							.extractedLinesFrom7Zip(outcomeZipFile);

					System.out.printf("Extracting of %s completed. Time elapsed = %.3fs\n",
							outcomeZipFile.getAbsolutePath(), (System.currentTimeMillis() - tic) / 1000.0);

					Matcher m_zip = pattern_inf_hist_zip.matcher(inf_hist_zips[0].getName());
					m_zip.matches();

					long cmap_seed = Long.parseLong(m_zip.group(1));
					HashMap<Integer, String[]> lookup_demograhic = demographic.get(cmap_seed);

					for (Entry<String, ArrayList<String[]>> ent : linesMap.entrySet()) {
						Matcher m = pattern_inf_preval_header.matcher(ent.getKey());
						String simKey, seedIdentifier;

						if (m.find()) {
							seedIdentifier = String.format("(%s_%s_%s)",
									m.group(GRP_INDEX_INF_HISTORY_HEADER_ROW_INDEX),
									m.group(GRP_INDEX_INF_HISTORY_HEADER_CMAP_SEED),
									m.group(GRP_INDEX_INF_HISTORY_HEADER_SIM_SEED));
							simKey = String.format("%s:%s%s", resultSetDir.getName(), singleResultSet.getName(),
									seedIdentifier);

						} else {
							seedIdentifier = String.format("(%s)", ent.getKey());
							simKey = String.format("%s:%s_%s", resultSetDir.getName(), singleResultSet.getName(),
									seedIdentifier);
						}

						if (sim_sel_values == null || Arrays.binarySearch(sim_sel_values, seedIdentifier) >= 0) {
							// Key: (Integer) person_id
							// Value:{[Infect_start, Infect_end, infId]}
							HashMap<Integer, int[][]> infection_hist_extract_by_person_id = new HashMap<>();

							for (String[] line : ent.getValue()) {
								int person_id = Integer.parseInt(line[0]);
								int inf_id = Integer.parseInt(line[1]);
								String[] indiv_stat = lookup_demograhic.get(person_id);
								int grp = Integer
										.parseInt(indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP + 1]);
								int enter_at = Integer.parseInt(
										indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT + 1]);
								int exit_at = Integer
										.parseInt(indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT + 1]);

								// Only include individual within sample time window
								boolean incl_person = ((1 << grp) & global_grp_incl) != 0
										&& ((1 << inf_id) & gloval_inf_incl) != 0 && sample_time[0] < exit_at
										&& enter_at <= sample_time[sample_time.length - 1];
								if (incl_person) {
									ArrayList<int[]> infection_hist_extract_entry = new ArrayList<>();

									for (int inf_hist_index = 2; inf_hist_index < line.length; inf_hist_index += 3) {
										int inf_start = Integer.parseInt(line[inf_hist_index]);
										int inf_end = inf_hist_index + 1 < line.length
												? Integer.parseInt(line[inf_hist_index + 1])
												: exit_at;
										int treatment_type = inf_hist_index + 2 < line.length
												? Integer.parseInt(line[inf_hist_index + 2])
												: 0;

										// Include infection if in range, or morbidity_prob_map has
										// entry
										if (treatment_type != Runnable_MetaPopulation_MultiTransmission.INFECTION_HIST_OVERTREATMENT) {
											infection_hist_extract_entry.add(new int[] { inf_start, inf_end, inf_id });
										}
									}
									if (infection_hist_extract_entry.size() > 0) {
										int[][] inf_hist = infection_hist_extract_entry.toArray(new int[0][]);
										int[][] org_inf_hist = infection_hist_extract_by_person_id.get(person_id);

										if (org_inf_hist != null) {
											int org_end = org_inf_hist.length;
											org_inf_hist = Arrays.copyOf(org_inf_hist,
													org_inf_hist.length + inf_hist.length);
											System.arraycopy(inf_hist, 0, org_inf_hist, org_end, inf_hist.length);
											inf_hist = org_inf_hist;
										}
										infection_hist_extract_by_person_id.put(person_id, inf_hist);
									}
								}

							} // End of reading (String[] line : ent.getValue()) {

							// System.out.printf("Extraction of infection history for %s completed. Time
							// elapsed = %.3fs\n",
							// simKey, (System.currentTimeMillis() - tic) / 1000.0);

							// Calculation
							Matcher m_simkey = sim_key_pattern.matcher(simKey);
							if (!m_simkey.matches()) {
								System.err.printf("Ill-formed simKey='%s'.Entry skipped.\n", simKey);
							} else {
								// For all
								HashMap<String, int[]> sample_columns_map = new HashMap<>(); // by time gap or age
								HashMap<String, double[]> sample_data_map = new HashMap<>();

								// For incidence count
								ArrayList<String> morbid_incidence_count = new ArrayList<>();
								HashMap<String, int[]> indivdual_incidence_record_map = new HashMap<>();

								// For morbidity based on infection probability
								ArrayList<String> morbid_prob = new ArrayList<>();

								ArrayList<String> morbid_all_outcomes = new ArrayList<>();

								for (String morbid_key : morbidity_key_arr) {
									morbid_all_outcomes.add(morbid_key);
									if (morbidity_setting_all.get(morbid_key).containsKey(SETTING_PROB_MAP)) {
										morbid_prob.add(morbid_key);
										String[] morbidity_sub_outcome = (String[]) morbidity_setting_all
												.get(morbid_key).get(SETTING_SUBOUTCOMES);
										for (String sub_outcome_name : morbidity_sub_outcome) {
											morbid_all_outcomes.add(sub_outcome_name);
										}
									} else {
										morbid_incidence_count.add(morbid_key);
										indivdual_incidence_record_map.put(morbid_key, null);
									}
								}

								// Set up sampling arrays
								for (String morbid_key : morbid_all_outcomes) {
									// Set up sample time array
									int time_gap = ((Number) morbidity_setting_all.get(morbid_key)
											.get(SETTING_SAMPLE_FREQ)).intValue();

									int s_time = sample_time[0];
									int s_end = sample_time[sample_time.length - 1];

									int data_arr_multiplier = 1;
									if (morbidity_setting_all.get(morbid_key)
											.containsKey(SETTING_PROB_COUNT_MAX_LIMIT)) {
										data_arr_multiplier += (Integer) morbidity_setting_all.get(morbid_key)
												.get(SETTING_PROB_COUNT_MAX_LIMIT); // +1 to include P(0)
									}
									if (morbidity_setting_all.get(morbid_key).containsKey(SETTING_BY_INDIVDUAL_AGE)) {
										int[] age_offset = (int[]) morbidity_setting_all.get(morbid_key)
												.get(SETTING_BY_INDIVDUAL_AGE);
										s_time = age_offset[0];
										s_end = age_offset[1];
									}
									int[] res_map_sample_time = sample_columns_map.get(morbid_key);

									if (res_map_sample_time == null) {
										ArrayList<Integer> sample_time_list = new ArrayList<>();
										while (s_time <= s_end) {
											sample_time_list.add(s_time);
											s_time += time_gap;
										}
										res_map_sample_time = new int[sample_time_list.size()];
										for (int i = 0; i < sample_time_list.size(); i++) {
											res_map_sample_time[i] = sample_time_list.get(i).intValue();
										}
										sample_columns_map.put(morbid_key, res_map_sample_time);
									}
									sample_data_map.put(morbid_key,
											new double[data_arr_multiplier * res_map_sample_time.length]);
									if (indivdual_incidence_record_map.containsKey(morbid_key)) {
										indivdual_incidence_record_map.put(morbid_key,
												new int[res_map_sample_time.length]);
									}

								}
								// Incidence count
								Long cMapSeed = Long.parseLong(m_simkey.group(4));

								for (Entry<Integer, int[][]> inf_hist_ent : infection_hist_extract_by_person_id
										.entrySet()) {
									int person_id = inf_hist_ent.getKey();
									String[] indiv_stat = demographic.get(cMapSeed).get(person_id);
									int grp = Integer
											.parseInt(indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_GRP + 1]);
									int enter_age = Integer.parseInt(
											indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AGE + 1]);
									int enter_at = Integer.parseInt(
											indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_ENTER_POP_AT + 1]);
									int exit_at = Integer.parseInt(
											indiv_stat[Abstract_Runnable_ClusterModel.POP_INDEX_EXIT_POP_AT + 1]);

									int[][] inf_hist = inf_hist_ent.getValue();

									// Clear individual incidence
									for (int[] indivdual_incid : indivdual_incidence_record_map.values()) {
										Arrays.fill(indivdual_incid, 0);
									}
									// inf_event format: new int[] { inf_start, inf_end, inf_id }
									for (int[] inf_event : inf_hist) {
										// Incidence count
										for (String morbid_key : morbid_incidence_count) {
											int inf_incl = ((Number) morbidity_setting_all.get(morbid_key)
													.get(SETTING_INF_INCL)).intValue();
											int grp_incl = ((Number) morbidity_setting_all.get(morbid_key)
													.get(SETTING_GRP_INCL)).intValue();
											int time_gap = ((Number) morbidity_setting_all.get(morbid_key)
													.get(SETTING_SAMPLE_FREQ)).intValue();

											int[] morbidty_indivdual_incidence_record = indivdual_incidence_record_map
													.get(morbid_key);
											int[] morbidty_sample_time = sample_columns_map.get(morbid_key);

											if (((1 << grp) & grp_incl) != 0 && ((1 << inf_event[2]) & inf_incl) != 0) {

												int t_pt = Arrays.binarySearch(morbidty_sample_time, inf_event[0]);
												if (t_pt < 0) {
													t_pt = ~t_pt;
												}
												if (t_pt < morbidty_sample_time.length
														&& morbidty_sample_time[t_pt] - time_gap < inf_event[0]) {
													morbidty_indivdual_incidence_record[t_pt]++;
												}
											}
										}

									} // End of checking infection history

									// Incidence or prevalence count
									for (String morbid_key : morbid_incidence_count) {
										int[] morbidty_indivdual_incidence_record = indivdual_incidence_record_map
												.get(morbid_key);
										double[] data_by_sample_time = sample_data_map.get(morbid_key);
										int infection_count_min_limit = -1;
										if (morbidity_setting_all.get(morbid_key)
												.containsKey(SETTING_EVENT_COUNT_MIN_LIMIT)) {
											infection_count_min_limit = ((Integer) morbidity_setting_all.get(morbid_key)
													.get(SETTING_EVENT_COUNT_MIN_LIMIT)).intValue();

											for (int t = 0; t < data_by_sample_time.length; t++) {
												if (morbidty_indivdual_incidence_record[t] >= infection_count_min_limit) {
													data_by_sample_time[t]++;
												}
											}
										} else {

											int inf_incl = ((Number) morbidity_setting_all.get(morbid_key)
													.get(SETTING_INF_INCL)).intValue();
											int grp_incl = ((Number) morbidity_setting_all.get(morbid_key)
													.get(SETTING_GRP_INCL)).intValue();
											if (((1 << grp) & grp_incl) != 0) {

												int[] morbidty_sample_time = sample_columns_map.get(morbid_key);
												boolean[] hasInf = new boolean[morbidty_sample_time.length];
												for (int[] inf_event : inf_hist) {
													if (((1 << inf_event[2]) & inf_incl) != 0) {
														int data_sample_time_pt_start = Arrays
																.binarySearch(morbidty_sample_time, inf_event[0]);
														if (data_sample_time_pt_start < 0) {
															data_sample_time_pt_start = ~data_sample_time_pt_start;
														}
														int data_sample_time_pt_end = Arrays.binarySearch(
																morbidty_sample_time, Math.min(inf_event[1], exit_at));
														if (data_sample_time_pt_end < 0) {
															data_sample_time_pt_end = ~data_sample_time_pt_end;
														}
														if (data_sample_time_pt_start >= 0
																&& data_sample_time_pt_end < data_by_sample_time.length) {
															for (int t = data_sample_time_pt_start; t < data_sample_time_pt_end; t++) {
																if (!hasInf[t]) {
																	data_by_sample_time[t]++;
																	hasInf[t] = true;
																}
															}
														}
													}

												}
											}
										}
									}

									// Check Probability based
									for (String morbid_key : morbid_prob) {
										int inf_incl = ((Number) morbidity_setting_all.get(morbid_key)
												.get(SETTING_INF_INCL)).intValue();
										int grp_incl = ((Number) morbidity_setting_all.get(morbid_key)
												.get(SETTING_GRP_INCL)).intValue();

										double[] data_by_sample_time = sample_data_map.get(morbid_key);

										@SuppressWarnings("unchecked")
										Map<Integer, double[]> morbidity_prob_map = (Map<Integer, double[]>) (morbidity_setting_all
												.get(morbid_key).get(SETTING_PROB_MAP));

										String[] morbidity_sub_outcome = (String[]) morbidity_setting_all
												.get(morbid_key).get(SETTING_SUBOUTCOMES);

										HashMap<Integer, Double> prob_number_of_morbidity_event = new HashMap<>();
										prob_number_of_morbidity_event.put(0, 1.0);

										int[] morbidty_sample_time = sample_columns_map.get(morbid_key);

										// Generate adjusted infection history record
										int pt_inf_hist_adj = 0;
										int[][] inf_hist_adj = new int[inf_hist.length][3];

										for (int[] inf_event : inf_hist) {
											if (((1 << grp) & grp_incl) != 0 && ((1 << inf_event[2]) & inf_incl) != 0) {

												inf_hist_adj[pt_inf_hist_adj][0] = inf_event[0];
												inf_hist_adj[pt_inf_hist_adj][1] = inf_event[1];
												inf_hist_adj[pt_inf_hist_adj][2] = inf_event[2];
												double[] morbidity_setting = morbidity_prob_map
														.get(inf_hist_adj[pt_inf_hist_adj][2]);

												if (morbidity_setting[0] > 0) {
													inf_hist_adj[pt_inf_hist_adj][1] = Math.min(
															inf_hist_adj[pt_inf_hist_adj][0]
																	+ (int) morbidity_setting[0],
															inf_hist_adj[pt_inf_hist_adj][1]);
												}
												pt_inf_hist_adj++;
											}
										}

										inf_hist_adj = Arrays.copyOf(inf_hist_adj, pt_inf_hist_adj);

										Arrays.sort(inf_hist_adj, new Comparator<int[]>() {
											@Override
											public int compare(int[] o1, int[] o2) {
												int res = 0;
												for (int i = 0; i < Math.min(o1.length, o2.length) && res == 0; i++) {
													res = Integer.compare(o1[i], o2[i]);
												}
												return res;
											}
										});

										// Key = end inf time, V = prob_morbidity_by_inf_end
										HashMap<Integer, ArrayList<Double>> main_morbidity_rec = new HashMap<>();
										int past_inc_count = 0;

										for (int[] inf_event : inf_hist_adj) {
											int inf_start = inf_event[0];
											if (inf_start < morbidty_sample_time[morbidty_sample_time.length - 1]) {
												int inf_end = Math.min(inf_event[1],
														morbidty_sample_time[morbidty_sample_time.length - 1]);
												int inf_id = inf_event[2];
												double[] morbidity_setting = morbidity_prob_map.get(inf_id);
												int numSetting = (morbidity_setting.length - 1) / 2;
												int pt_p = Arrays.binarySearch(morbidity_setting, 1, numSetting,
														past_inc_count);
												if (pt_p < 0) {
													pt_p = ~pt_p;
												}

												double prob_morbidity_per_day = morbidity_setting[Math
														.min(numSetting + pt_p, morbidity_setting.length - 1)];
												double prob_morbidity_by_inf_end = 1
														- Math.pow(1 - prob_morbidity_per_day, inf_end - inf_start);

												// Update event count probability for pre-sample time
												if (inf_end < morbidty_sample_time[0]) {
													update_prob_number_morbidity_event(prob_number_of_morbidity_event,
															prob_morbidity_by_inf_end);
												} else {
													int pt_inf_end = Arrays.binarySearch(morbidty_sample_time, inf_end);
													if (pt_inf_end < 0) {
														pt_inf_end = ~pt_inf_end;
													}
													// Calculate the probability of main morbidity
													data_by_sample_time[pt_inf_end] += prob_morbidity_by_inf_end;

													ArrayList<Double> prob = main_morbidity_rec.get(inf_end);
													if (prob == null) {
														prob = new ArrayList<>();
														main_morbidity_rec.put(inf_end, prob);
													}
													prob.add(prob_morbidity_by_inf_end);
												}
												past_inc_count++;
											}

										}

										// Suboutcome setting
										if (morbidity_sub_outcome != null) {

											Integer[] event_prob_time = main_morbidity_rec.keySet()
													.toArray(new Integer[0]);
											Arrays.sort(event_prob_time);

											double[][] data_arr_indiv_all = new double[morbidity_sub_outcome.length][];

											for (int sub_outcome_index = 0; sub_outcome_index < morbidity_sub_outcome.length; sub_outcome_index++) {
												String sub_outcome_name = morbidity_sub_outcome[sub_outcome_index];
												data_arr_indiv_all[sub_outcome_index] = new double[sample_data_map
														.get(sub_outcome_name).length];
											}

											for (Integer event_time : event_prob_time) {

												ArrayList<Double> event_prop_arr = main_morbidity_rec
														.remove(event_time);

												// If multiple event occurs on same time, use 1- Product((1-p_n))
												double prob_non_event = 1;
												for (Double prob_morbidity_by_inf_end : event_prop_arr) {
													prob_non_event *= (1 - prob_morbidity_by_inf_end.doubleValue());
												}
												update_prob_number_morbidity_event(prob_number_of_morbidity_event,
														1 - prob_non_event);

												for (int sub_outcome_index = 0; sub_outcome_index < morbidity_sub_outcome.length; sub_outcome_index++) {
													String sub_outcome_name = morbidity_sub_outcome[sub_outcome_index];
													int max_event_count = 1;
													if (morbidity_setting_all.get(sub_outcome_name)
															.containsKey(SETTING_PROB_COUNT_MAX_LIMIT)) {
														max_event_count = (Integer) morbidity_setting_all
																.get(sub_outcome_name)
																.get(SETTING_PROB_COUNT_MAX_LIMIT);
													}

													int[] search_col = sample_columns_map.get(sub_outcome_name);
													double[] data_arr_indiv = data_arr_indiv_all[sub_outcome_index];

													int sample_time_search_from = event_time;
													int sample_time_search_end = exit_at;
													if (morbidity_setting_all.get(sub_outcome_name)
															.containsKey(SETTING_BY_INDIVDUAL_AGE)) {
														sample_time_search_from = enter_age + event_time - enter_at;
														sample_time_search_end = enter_age + exit_at - enter_at;
													}

													int update_start = Arrays.binarySearch(search_col,
															sample_time_search_from);
													if (update_start < 0) {
														update_start = ~update_start;
													}
													int update_end = Arrays.binarySearch(search_col,
															sample_time_search_end);
													if (update_end < 0) {
														update_end = ~update_end;
													}

													double[] indivdual_event_prob = new double[max_event_count + 1];

													for (Entry<Integer, Double> prob_event : prob_number_of_morbidity_event
															.entrySet()) {
														indivdual_event_prob[Math.min(prob_event.getKey(),
																indivdual_event_prob.length - 1)] += prob_event
																		.getValue();

													}

													for (int event_num = 0; event_num < indivdual_event_prob.length; event_num++) {
														for (int update_pt = update_start; update_pt < Math
																.min(update_end, search_col.length); update_pt++) {
															data_arr_indiv[update_pt * (max_event_count + 1)
																	+ event_num] = indivdual_event_prob[event_num];

														}
													}
												}
											}
											for (int sub_outcome_index = 0; sub_outcome_index < morbidity_sub_outcome.length; sub_outcome_index++) {
												String sub_outcome_name = morbidity_sub_outcome[sub_outcome_index];
												double[] data_arr = sample_data_map.get(sub_outcome_name);
												for (int i = 0; i < data_arr.length; i++) {
													data_arr[i] += data_arr_indiv_all[sub_outcome_index][i];
												}
											}
										} // End of checking sub outcome
									} // End of for (String morbid_key : morbid_prob)
								} // End of checking individual infection history

								// Output map

								Map<String, String> output_by_zip = new HashMap<>();

								for (String morbid_key : morbid_all_outcomes) {
									double[] data_arr = sample_data_map.get(morbid_key);
									StringBuilder strBld = new StringBuilder();
									strBld.append(simKey);
									for (double val : data_arr) {
										strBld.append(',');
										if (morbidity_setting_all.get(morbid_key).containsKey(SETTING_PROB_MAP)) {
											strBld.append(String.format("%f", val));
										} else {
											strBld.append(String.format("%d", (int) val));
										}
									}
									output_by_zip.put(morbid_key, strBld.toString());
								}
								output_map.put(simKey, output_by_zip);
							}

							// System.out.printf("Infection history calculation for %s completed. Time
							// elapsed. = %.3fs\n",
							// simKey, (System.currentTimeMillis() - tic) / 1000.0);

						} // End of read all from selected sim

					} // End of looking up inf_hist_zips

					// Potential end of thread

					System.out.printf(
							"Infection history calculation for zip file %s completed. Time elapsed. = %.3fs\n",
							outcomeZipFile.getAbsolutePath(), (System.currentTimeMillis() - tic) / 1000.0);

					// Combine output for all
					output_map_all.putAll(output_map);

				} // End of if (inf_hist_zips.length == 1)

				// Print Result (per singleResultSet)
				String[] simKeys = output_map_all.keySet().toArray(new String[0]);
				Arrays.sort(simKeys, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						Matcher m1 = sim_key_pattern.matcher(o1);
						Matcher m2 = sim_key_pattern.matcher(o2);
						int res = 0;
						if (m1.matches() && m2.matches()) {
							for (int g = 2; g < m1.groupCount() && res == 0; g++) {
								res = Integer.compare(Integer.parseInt(m1.group(g)), Integer.parseInt(m2.group(g)));
							}
						}
						return res;
					}
				});
				for (String simKey : simKeys) {
					Map<String, String> output_map = output_map_all.remove(simKey);
					for (String morbid_key : output_map.keySet()) {
						priWriter_map.get(morbid_key).println(output_map.get(morbid_key));
					}
				}
				for (PrintWriter pri : priWriter_map.values()) {
					pri.flush();
				}
			} // End of for (File singleResultSet : singleResultSets) {

		} // End of for (File resultSetDir : scenario_dirs_incl) {

		for (PrintWriter pri : priWriter_map.values()) {
			pri.close();
		}

	}

	private static void update_prob_number_morbidity_event(HashMap<Integer, Double> prob_number_of_morbidity_event,
			double prob_morbidity_by_inf_end) {
		Integer[] event_count_arr = prob_number_of_morbidity_event.keySet().toArray(new Integer[0]);
		Arrays.sort(event_count_arr);
		double preProb = 0;
		for (int event_count : event_count_arr) {
			double preProb_store = prob_number_of_morbidity_event.get(event_count);
			prob_number_of_morbidity_event.put(event_count,
					prob_number_of_morbidity_event.get(event_count) * (1 - prob_morbidity_by_inf_end)
							+ preProb * prob_morbidity_by_inf_end);
			preProb = preProb_store;
		}

		prob_number_of_morbidity_event.put(event_count_arr[event_count_arr.length - 1] + 1,
				preProb * prob_morbidity_by_inf_end);
	}

	public static final String XML_SETTING_SAMPLE_TIME = "XML_SETTING_SAMPLE_TIME";
	public static final String XML_SETTING_DEMOGRAPHIC_DIR = "XML_SETTING_DEMOGRAPHIC_DIR";
	public static final String XML_SETTING_MORBIDITY_KEY_ARR = "XML_SETTING_MORBIDITY_KEY_ARR";
	public static final String XML_SETTING_MORBIDITY_FORMAT = "XML_SETTING_MORBIDITY_FORMAT_%s";

	public static void exportSimSelMap(HashMap<String, ArrayList<String>> sim_sel_map, File xml_prop)
			throws FileNotFoundException, IOException {
		String[] sim_sel_key = sim_sel_map.keySet().toArray(new String[0]);
		Arrays.sort(sim_sel_key, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(Integer.parseInt(o1), Integer.parseInt(o2));
			}
		});
		Properties export_prop = new Properties();
		for (String k : sim_sel_key) {
			StringBuilder line = new StringBuilder();
			for (String ent : sim_sel_map.get(k)) {
				if (line.length() != 0) {
					line.append(',');
				}
				line.append(ent);
			}
			export_prop.put(k, line.toString());
		}
		System.out.printf("Col select map exported to %s.\n", xml_prop.getAbsolutePath());
		DateTimeFormatter dateformatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		FileOutputStream fout = new FileOutputStream(xml_prop);
		export_prop.storeToXML(fout, String.format("Generated at %s", LocalDateTime.now().format(dateformatter)));
		fout.close();
	}

	public static HashMap<String, ArrayList<String>> importSimSelMap(File xml_prop)
			throws InvalidPropertiesFormatException, IOException {
		Properties prop = new Properties();
		FileInputStream fin = new FileInputStream(xml_prop);
		prop.loadFromXML(fin);
		fin.close();
		HashMap<String, ArrayList<String>> sim_sel_map = new HashMap<>();
		for (String k : prop.stringPropertyNames()) {
			String[] val = prop.getProperty(k).split(",");
			sim_sel_map.put(k, new ArrayList<>(List.of(val)));
		}
		return sim_sel_map;
	}

	public static void exportMorbiditySetting(String demoFileLoc, int[] sample_time, String[] morbidity_keys,
			Map<String, Map<String, Object>> morbidity_setting, File xml_setting)
			throws FileNotFoundException, IOException {
		String morbidity_key_arr_str = Arrays.deepToString(morbidity_keys).replaceAll("\\s", "");
		Properties setting_prop = new Properties();
		setting_prop.put(XML_SETTING_SAMPLE_TIME, Arrays.toString(sample_time));
		setting_prop.put(XML_SETTING_DEMOGRAPHIC_DIR, demoFileLoc);
		setting_prop.put(XML_SETTING_MORBIDITY_KEY_ARR,
				morbidity_key_arr_str.substring(1, morbidity_key_arr_str.length() - 1));

		String settingFormat = XML_SETTING_MORBIDITY_FORMAT;

		for (Entry<String, Map<String, Object>> ent : morbidity_setting.entrySet()) {
			StringBuilder strBuilder = new StringBuilder();
			for (String mapKey : ent.getValue().keySet()) {
				String entStr;

				Object val = ent.getValue().get(mapKey);

				if (mapKey.equals(SETTING_PROB_MAP)) {
					StringBuilder sub_ent = new StringBuilder();
					@SuppressWarnings("unchecked")
					Map<Integer, double[]> prob_map = (Map<Integer, double[]>) val;
					for (Entry<Integer, double[]> entDouble : prob_map.entrySet()) {
						if (sub_ent.length() != 0) {
							sub_ent.append(',');
						}
						sub_ent.append(
								String.format("%d:%s", entDouble.getKey(), Arrays.toString(entDouble.getValue())));
					}

					entStr = sub_ent.toString().replaceAll("\\s", "");

				} else {
					if (val instanceof Object[]) {
						entStr = Arrays.deepToString((Object[]) val);
					} else if (val instanceof int[]) {
						entStr = Arrays.toString((int[]) val);
					} else {
						entStr = val.toString();
					}

				}

				strBuilder.append(String.format("\n%s=%s", mapKey, entStr));
			}

			setting_prop.put(String.format(settingFormat, ent.getKey()), strBuilder.toString());

		}

		DateTimeFormatter dateformatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		FileOutputStream fout = new FileOutputStream(xml_setting);
		setting_prop.storeToXML(fout, String.format("Generated at %s", LocalDateTime.now().format(dateformatter)));
		fout.close();
	}

}
