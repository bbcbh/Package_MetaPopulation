package sim;

import java.io.File;
import java.util.Properties;

import map.Map_Location_Mobility;

/**
 * For generation of demographic and mobility
 */

public class Runnable_ContactMap_Generation_Map_Location_Mobility
		extends Runnable_ClusterModel_ContactMap_Generation_MultiMap {

	public static final String POP_TYPE = "MetaPop_By_Location_Mobility";

	// 2: RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH
	// Format: File path. Noted that it assumes node info will be
	// filename_NoteInfo.csv
	public static final int RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH = Runnable_ClusterModel_ContactMap_Generation_MultiMap.RUNNABLE_FIELD_CONTACT_MAP_GEN_MULTIMAP_NUMBER_OF_GRP;

	private Properties loadedProperties;
	private Map_Location_Mobility loc_map;
	private File baseDir;
	private int popId;

	public Runnable_ContactMap_Generation_Map_Location_Mobility(long mapSeed, 
			int popId,Properties loadedProperties) {
		super(mapSeed);
		// Load properties
		this.loadedProperties = loadedProperties;
		try {
			this.loc_map = (Map_Location_Mobility) loadedProperties.get(Simulation_MetaPop.PROP_LOC_MAP);
			this.baseDir = (File) loadedProperties.get(Simulation_MetaPop.PROP_BASEDIR);			
		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
		this.popId = popId;

	}

	@Override
	public void run() {
		// TODO: TO be implemented

	}

}
