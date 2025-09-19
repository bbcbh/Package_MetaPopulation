package map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Map_Location with mobility info included.
 */

public class Map_Location_Mobility extends Abstract_Map_Location {
	private static final long serialVersionUID = -5125249131068160422L;

	// Default header based on IREG from ABS
	public String nodeInfo_header = "POP_ID,Name" + ",M_15-19_I,M_20-24_I,M_25-29_I,M_30-34_I,M_35-39_I"
			+ ",F_15-19_I,F_20-24_I,F_25-29_I,F_30-34_I,F_35-39_I"
			+ ",M_15-19_N,M_20-24_N,M_25-29_N,M_30-34_N,M_35-39_N"
			+ ",F_15-19_N,F_20-24_N,F_25-29_N,F_30-34_N,F_35-39_N"; 

	public static final int INDEX_POP_ID = 0;
	public static final int INDEX_NAME = 1;

	public static final String NODE_INFO_POP_SIZE = "NODE_INFO_POP_SIZE";	
	public static final String NODE_INFO_AWAY = "NODE_INFO_AWAY";	
	
	public Map_Location_Mobility() {
		// Use default header
	}
	public Map_Location_Mobility(String custom_nodeInfo_header) {
		nodeInfo_header = custom_nodeInfo_header;
	}	
	
	
	public void exportNodeInfoToString(PrintWriter pwri_nodeInfo) {
		pwri_nodeInfo.println(nodeInfo_header);
		for (Entry<Integer, HashMap<String, Object>> entry : getNode_info().entrySet()) {
			int[] popSize = (int[]) entry.getValue().get(NODE_INFO_POP_SIZE);
			String nodename = entry.getValue().get(NODE_INFO_NAME).toString();
			StringBuilder nodeInfoStr = new StringBuilder();
			nodeInfoStr.append(entry.getKey());
			nodeInfoStr.append(',');
			nodeInfoStr.append(nodename);
			for (int val : popSize) {
				nodeInfoStr.append(',');
				nodeInfoStr.append(val);
			}

			pwri_nodeInfo.println(nodeInfoStr.toString());
		}
	}

	public void importNodeInfoFromString(BufferedReader reader) throws IOException {
		if (getNode_info() == null) {
			setNode_info(new HashMap<>());
		}
		fillNodeInfoFromString(reader, NODE_INFO_POP_SIZE);
	}

	public void importAwayInfoFromString(BufferedReader reader) throws IOException {
		if (getNode_info() == null) {
			setNode_info(new HashMap<>());
		}
		fillNodeInfoFromString(reader,NODE_INFO_AWAY);
	}

	private void fillNodeInfoFromString(BufferedReader reader, String mapType)
			throws IOException {
		String line;
		String[] headerTxt = nodeInfo_header.split(",");

		while ((line = reader.readLine()) != null) {
			if (!line.equals(nodeInfo_header)) {
				String[] ent = line.split(",");
				Integer id = Integer.parseInt(ent[INDEX_POP_ID]);
				HashMap<String, Object> info = getNode_info().get(id);
				if (info == null) {
					info = new HashMap<>();
					getNode_info().put(id, info);
				}
				info.put(Abstract_Map_Location.NODE_INFO_NAME, ent[INDEX_NAME]);				
				if (NODE_INFO_POP_SIZE.equals(mapType)) {
					int[] intVal = new int[headerTxt.length - 2];
					for (int i = 0; i < intVal.length; i++) {
						intVal[i] = Integer.parseInt(ent[i + 2]);
					}
					info.put(mapType, intVal);
				}else {
					float[] floatVal = new float[headerTxt.length - 2];
					for (int i = 0; i < floatVal.length; i++) {
						floatVal[i] = Float.parseFloat(ent[i + 2]);
					}
					info.put(mapType, floatVal);
				}			
			}

		}

	}

	
}
