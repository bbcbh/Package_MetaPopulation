package sim;

import java.io.IOException;
import java.util.Properties;

public class Simulation_RMP extends Simulation_ClusterModelTransmission {		
	
	public static void main(String[] args) throws IOException, InterruptedException {
		final String USAGE_INFO = String.format(
				"Usage: java %s - PROP_FILE_DIRECTORY "
						+ "<-export_skip_backup> <-printProgress> <-seedMap=SEED_MAP>\n",
						Simulation_RMP.class.getName());
		if (args.length < 1) {
			System.out.println(USAGE_INFO);
			System.exit(0);
		} else {
			Simulation_ClusterModelTransmission.launch(args);
		}
	}
	
	
	@Override
	public Abstract_Runnable_ClusterModel_Transmission generateDefaultRunnable(long cMap_seed, long sim_seed, 
			Properties loadProperties) {		
		return null;
	}

}
