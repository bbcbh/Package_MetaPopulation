package sim;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import util.LineCollectionEntry;

public abstract class Abstract_Runnable_MetaPopulation_Transmission_RMP_MultiInfection
		extends Runnable_ClusterModel_MultiTransmission {	
	
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
	protected static final Comparator<int[]> comparator_grp_change = new Comparator<int[]>() {
		@Override
		public int compare(int[] o1, int[] o2) {
			int res = 0;
			for (int i = 0; i < Math.min(o1.length, o2.length) && res == 0; i++) {
				res = Integer.compare(o1[i], o2[i]);
			}
			return res;
		}
	};
	
	// Infection history	
	protected HashMap<Integer, ArrayList<ArrayList<Integer>>> infection_history = new HashMap<>();
	public static final int INFECTION_HIST_CLEAR_NATURAL_RECOVERY = -1;
	public static final int INFECTION_HIST_CLEAR_TREATMENT = -2;
	public static final int INFECTION_HIST_OVERTREATMENT = -3;
	

	// Individual movement	
	// Key = LocationFrom_LocationTo
	protected HashMap<String, LineCollectionEntry> movementCollections = new HashMap<>();
	protected HashMap<Integer, ArrayList<Integer>> visitor_pids_by_loc = new HashMap<>();
	protected int lastMovement_update = -1;
	
	
	// Others	
	protected static final String key_pop_size = "EXPORT_POP_SIZE";
	protected static final String FILENAME_EXPORT_POP_SIZE = "Pop_size_%d_%d.csv";

	protected final File dir_demographic;	
	private boolean preloadMovementFile = false;

	public Abstract_Runnable_MetaPopulation_Transmission_RMP_MultiInfection(long cMap_seed, long sim_seed,
			Properties prop, int NUM_INF, int NUM_SITE, int NUM_ACT) {
		super(cMap_seed, sim_seed, null, prop, NUM_INF, NUM_SITE, NUM_ACT);
		this.setBaseDir((File) prop.get(Simulation_Gen_MetaPop.PROP_BASEDIR));
		this.setBaseProp(prop);
		this.dir_demographic = new File(baseProp.getProperty(Simulation_Gen_MetaPop.PROP_CONTACT_MAP_LOC));

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
				LineCollectionEntry ent = new LineCollectionEntry(mv, preloadMovementFile);
				ent.loadNextLine();
				ent.loadNextLine(); // Skip Header
				movementCollections.put(m.group(1), ent);
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
	}

}
