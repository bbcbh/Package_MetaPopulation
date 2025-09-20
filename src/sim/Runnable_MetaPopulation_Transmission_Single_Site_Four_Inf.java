package sim;

import java.io.File;
import java.util.Properties;

public class Runnable_MetaPopulation_Transmission_Single_Site_Four_Inf extends Runnable_ClusterModel_MultiTransmission {	
	
	public static final int SITE_VAGINA = 0;
	public static final int SITE_PENIS = SITE_VAGINA + 1;
	public static final int SITE_ANY = SITE_PENIS + 1;
	
	File dir_demographic;
	
	public Runnable_MetaPopulation_Transmission_Single_Site_Four_Inf(long cMap_seed, long sim_seed, Properties prop) {
		super(cMap_seed, sim_seed, null, prop, 4, 3, 1);					
		
		this.setBaseDir((File) prop.get(Simulation_MetaPop.PROP_BASEDIR));
		this.setBaseProp(prop);		
		dir_demographic = new File(prop.getProperty(Simulation_MetaPop.PROP_CONTACT_MAP_LOC));						
	}

	@Override
	public void initialse() {		
		super.initialse();		
		//TODO: To be implemented
	}
	
	

}
