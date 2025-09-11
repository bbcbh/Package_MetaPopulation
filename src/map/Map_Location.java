package map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

public class Map_Location extends SimpleDirectedWeightedGraph<Integer, DefaultWeightedEdge> {

	private static final long serialVersionUID = 5729842599305619354L;	
	
	public static final String connection_header = "SOURCE,TARGET,WEIGHT";	
	
	
	private HashMap<Integer, HashMap<String, Object>> node_info;
	
	public static final String NODE_INFO_NAME = "NODE_INFO_NAME"; 	
	public static final Class<String> NODE_INFO_NAME_CLASS = String.class;	
	
		
	public Map_Location() {
		super(DefaultWeightedEdge.class);		
	}	
	
	public HashMap<Integer, HashMap<String, Object>> getNode_info() {
		return node_info;
	}

	public void setNode_info(HashMap<Integer, HashMap<String, Object>> node_info) {
		this.node_info = node_info;
	}
	public void exportConnectionsToString(PrintWriter pWri) {
		DefaultWeightedEdge[] edge_arr = this.edgeSet().toArray(new DefaultWeightedEdge[0]);
		pWri.println(connection_header);
		for (DefaultWeightedEdge edge : edge_arr) {
			pWri.printf("%s,%s,%f\n", this.getEdgeSource(edge), this.getEdgeTarget(edge), this.getEdgeWeight(edge));
		}
	}
	public void importConnectionsFromString(BufferedReader reader) throws IOException {
		String line;
		while ((line = reader.readLine()) != null) {
			if (!line.equals(connection_header)) {
				String[] ent = line.split(",");
				Integer[] v_arr = new Integer[2];
				
				for (int i : new int[] { 0, 1 }) {					
					Integer v = Integer.parseInt(ent[i]);
					if (!this.containsVertex(v)) {
						this.addVertex(v);
					}					
					v_arr[i] = v;
				}
				DefaultWeightedEdge edge = this.addEdge(v_arr[0], v_arr[1]);
				this.setEdgeWeight(edge, Double.parseDouble(ent[2]));
			}
		}
	}
}
