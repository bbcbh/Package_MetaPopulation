package map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map.Entry;

public class Map_Location_IREG extends Abstract_Map_Location {
	private static final long serialVersionUID = -5125249131068160422L;

	public static final String nodeInfo_header = "POP_ID,Name"
			+ ",M_15-19_I,M_20-24_I,M_25-29_I,M_30-34_I,M_35-39_I"
			+ ",F_15-19_I,F_20-24_I,F_25-29_I,F_30-34_I,F_35-39_I"
			+ ",M_15-19_N,M_20-24_N,M_25-29_N,M_30-34_N,M_35-39_N"
			+ ",M_15-19_N,M_20-24_N,M_25-29_N,M_30-34_N,M_35-39_N";

	public static final int INDEX_POP_ID = 0;
	public static final int INDEX_NAME = 1;	

	public static final String NODE_INFO_POP_SIZE = "NODE_INFO_POP_SIZE";
	public static final Class<int[]> NODE_INFO_POP_SIZE_CLASS = int[].class;

	public void exportNodeInfoToString(PrintWriter pwri_nodeInfo) {
		pwri_nodeInfo.println(nodeInfo_header);
		for (Entry<Integer, HashMap<String, Object>> entry : getNode_info().entrySet()) {
			int[] popSize = (int[]) entry.getValue().get(NODE_INFO_POP_SIZE);
			String nodename = entry.getValue().get(NODE_INFO_NAME).toString();
			StringBuilder nodeInfoStr = new StringBuilder();
			nodeInfoStr.append(entry.getKey());
			nodeInfoStr.append(',');
			nodeInfoStr.append(nodename);
			for(int val : popSize) {
				nodeInfoStr.append(',');
				nodeInfoStr.append(val);
			}						
			
			pwri_nodeInfo.println(nodeInfoStr.toString());
		}
	}

	public void importNodeInfoFromString(BufferedReader reader) throws IOException {
		String line;
		String[] headerTxt = nodeInfo_header.split(",");
		if (getNode_info() == null) {
			setNode_info(new HashMap<>());
		}
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
				int[] pop_size = new int[headerTxt.length-2];
				for (int i = 0; i < pop_size.length; i++) {
					pop_size[i] = Integer.parseInt(ent[i+2]);
				}
				info.put(NODE_INFO_POP_SIZE, pop_size);

			}

		}

	}

}
