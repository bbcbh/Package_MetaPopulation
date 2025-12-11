package sim;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

public class Simulation_MetaPopulation extends Simulation_ClusterModelTransmission {	
	
	public static final String PROP_BASEDIR = "PROP_BASEDIR";
	public static final String PROP_LOC_MAP = "PROP_LOC_MAP";
	public static final String PROP_PRELOAD_FILES = "PROP_PRELOAD_FILES";
	public static final String PROP_INDIV_STAT = "PROP_INDIV_STAT";
	public static final String PROP_PARNTER_EXTRA_SOUGHT = "PROP_PARNTER_EXTRA_SOUGHT";
	public static final String PROP_CONTACT_MAP_LOC = "PROP_CONTACT_MAP_LOC";
		
    // Demographic
	public static final String DIR_NAME_FORMAT_DEMOGRAPHIC = "Demographic_%d";
	public static final String FILENAME_FORMAT_CMAP_BY_POP = "ContactMap_Pop_%d_%d.csv"; // POP_ID , SEED
	public static final String FILENAME_FORMAT_EXTRA_PARTNER_SOUGHT = "Extra_partner_sought_%d.csv"; // SEED
	public static final String FILENAME_FORMAT_POP_INDEX_MAP = "PopIndex_Mapping_%d.csv"; // SEED
	public static final String FILENAME_FORMAT_DEMOGRAPHIC = "Demographic_%d_%d.csv"; // POP_ID, SEED 
	public static final String FILENAME_FORMAT_MOVEMENT = "Movement_%s_%d.csv"; // POPSRC_POPTAR , SEED	
	
	// File header	
	public static final String FILE_HEADER_EXTRA_PARTNER_SOUGHT = "TIME_FROM,PID,EXTRA_PARTNER_SOUGHT";
	public static final String FILE_HEADER_POP_STAT = "ID,GRP,ENTER_POP_AGE,ENTER_POP_AT,EXIT_POP_AT,HOME_LOC";
	public static final String FILE_HEADER_POP_INDEX_MAP = "POP_INDEX,POP_ID";	
	public static final String FILE_HEADER_DEMOGRAPHIC = "PID,ENTER_AT,EXIT_AT,ENTER_AGE,ENTER_GRP";
	public static final String FILE_HEADER_MOVEMENT = "TIME,PID";

	@Override
	protected void loadMultipleCMap(ArrayList<Long> cMapSeeds, File contactMapDir,
			HashMap<Long, ArrayList<File>> cmap_file_collection) {

		for (Long cMapSeed : cMapSeeds) {
			File match_dir = new File(contactMapDir,
					String.format(Simulation_MetaPopulation.DIR_NAME_FORMAT_DEMOGRAPHIC, cMapSeed));

			if (match_dir.exists()) {	
				String cMap_format = Runnable_ClusterModel_ContactMap_Generation_MultiMap.MAPFILE_FORMAT
						.replaceFirst("%d", "(\\\\d+)");
				cMap_format = cMap_format.replaceFirst("%d", cMapSeed.toString());

				final Pattern pattern_cMap = Pattern.compile(cMap_format);

				File[] cMapFiles = match_dir.listFiles(new FileFilter() {

					@Override
					public boolean accept(File pathname) {
						return pattern_cMap.matcher(pathname.getName()).matches();
					}
				});
				cmap_file_collection.put(cMapSeed, new ArrayList<>(Arrays.asList(cMapFiles)));			
				

			} else {
				System.err.printf("Error! Matching MetaPopulation Directory %s not found. Exiting.\n",
						match_dir.getAbsolutePath());
				System.exit(-1);

			}

		}

	}

}
