package sim;

import java.util.Arrays;
import java.util.Properties;

import map.Map_Location_Mobility;

public class Runnable_ContactMap_Generation_LocationMap implements Runnable {
	
	private Properties loadedProperties;
	private Map_Location_Mobility loc_map;	
	private long mapSeed;
	
	public Runnable_ContactMap_Generation_LocationMap(long mapSeed, Properties loadedProperties) {		
		this.mapSeed = mapSeed;
		this.loadedProperties = loadedProperties;		
		try {
			this.loc_map = (Map_Location_Mobility) loadedProperties.get(Simulation_MetaPop.PROP_LOC_MAP);			
		} catch (NullPointerException ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}		
	}

	@Override
	public void run() {		
		Integer[] pop_ids = loc_map.getNode_info().keySet().toArray(new Integer[0]);
		Arrays.sort(pop_ids);
		
		long tic = System.currentTimeMillis();
		
		StepWise_ContactMap_Generation_Map_Location_Mobility[] steps = new StepWise_ContactMap_Generation_Map_Location_Mobility[pop_ids.length];
		for (int i = 0; i < pop_ids.length; i++) {
			int pop_id = pop_ids[i];
			steps[i] = new StepWise_ContactMap_Generation_Map_Location_Mobility(mapSeed, pop_id, loadedProperties);
		}
		
		int currentTime = 0;
		int max_time = Integer.parseInt(
				loadedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SNAP]))
				* Integer.parseInt(loadedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));
		
		while (currentTime < max_time) {
			for (int i = 0; i < pop_ids.length; i++) {			
				steps[i].updateDemogrpahic();
			}			
			for (int i = 0; i < pop_ids.length; i++) {			
				steps[i].updateMovement();
			}
			for (int i = 0; i < pop_ids.length; i++) {
				steps[i].advanceTimeStep();
			}												
			currentTime++;											
		}
		for (int i = 0; i < pop_ids.length; i++) {
			steps[i].finalise();
		}
		
		System.out.printf("Contact map generation for cMap=%d completed. Time required = %.3fs\n", mapSeed,
				(System.currentTimeMillis() - tic) / 1000.0);
		
	}

}
