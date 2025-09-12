package sim;

import java.io.File;
import java.util.Properties;

import map.Map_Location_IREG;


/**
 * For generation of demographic and mobility
 */

public class Runnable_ContactMap_Generation_LocationMap_IREG extends Runnable_ClusterModel_ContactMap_Generation_MultiMap {

	public static final String POP_TYPE = "MetaPop_By_Location_IREG";	
	
	private Properties loadedProperties;	
	private Map_Location_IREG loc_map;
	private File baseDir;	
	
	public Runnable_ContactMap_Generation_LocationMap_IREG(long mapSeed, Properties loadedProperties) {
		super(mapSeed);
		// Load properties
		this.loadedProperties = loadedProperties;		
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
		// TODO: TO be implemented		
		
		
		
	}

}
