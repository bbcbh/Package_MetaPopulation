package map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map.Entry;

public class Map_Location_IREG extends Map_Location {
	private static final long serialVersionUID = -5125249131068160422L;

	public static final String nodeInfo_header = "POP_ID,Name,M_Indigenous,F_Indigenous,M_Non_Indigenous,F_Non_Indigenous";

	public static final int INDEX_POP_ID = 0;
	public static final int INDEX_NAME = 1;
	public static final int[] INDICES_POP_SIZES = new int[] { 2, 3, 4, 5 };

	public static final String NODE_INFO_POP_SIZE = "NODE_INFO_POP_SIZE";
	public static final Class<int[]> NODE_INFO_POP_SIZE_CLASS = int[].class;

	public void exportNodeInfoToString(PrintWriter pwri_nodeInfo) {
		pwri_nodeInfo.println(nodeInfo_header);
		for (Entry<Integer, HashMap<String, Object>> entry : getNode_info().entrySet()) {
			int[] p = (int[]) entry.getValue().get(NODE_INFO_POP_SIZE);
			String nodename = entry.getValue().get(NODE_INFO_NAME).toString();
			pwri_nodeInfo.printf("%d,%s,%d,%d,%d,%d\n", entry.getKey(), nodename, p[0], p[1], p[2], p[3]);
		}
	}

	public void importNodeInfoFromString(BufferedReader reader) throws IOException {
		String line;
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
				info.put(Map_Location.NODE_INFO_NAME, ent[INDEX_NAME]);

				int[] pop_size = new int[INDICES_POP_SIZES.length];
				for (int i = 0; i < pop_size.length; i++) {
					pop_size[i] = Integer.parseInt(ent[INDICES_POP_SIZES[i]]);
				}
				info.put(NODE_INFO_POP_SIZE, pop_size);

			}

		}

	}

}
