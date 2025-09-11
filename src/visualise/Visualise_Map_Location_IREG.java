package visualise;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultWeightedEdge;

import com.mxgraph.model.mxICell;
import com.mxgraph.util.mxConstants;

import map.Map_Location_IREG;

public class Visualise_Map_Location_IREG extends JGraphXAdapter<Integer, DefaultWeightedEdge> {

	private final Map_Location_IREG map;

	private static final String STYLE_LOC_DEF = "STYLE_LOC_DEF";

	
	@Override
	public boolean isCellsEditable() {		
		return false;
	}

	
	@Override
	public boolean isCellSelectable(Object cell) {		
		return false;
	}

	public Visualise_Map_Location_IREG(Map_Location_IREG map_location) {
		super(map_location);
		map = map_location;

		// Style
		setHtmlLabels(true);							
		HashMap<String, Object> loc_style = new HashMap<>();
		loc_style.put(mxConstants.STYLE_SHAPE, mxConstants.SHAPE_RECTANGLE);
		loc_style.put(mxConstants.STYLE_FILLCOLOR, "#00FFFF");
		loc_style.put(mxConstants.STYLE_OPACITY, 50);
		loc_style.put(mxConstants.STYLE_FONTCOLOR, "#0000FF");		
		loc_style.put(STYLE_LOC_DEF, loc_style);
		getStylesheet().putCellStyle(STYLE_LOC_DEF, loc_style);		
		
		Map<String, Object> edge_style = getStylesheet().getDefaultEdgeStyle();
		edge_style.put(mxConstants.STYLE_FONTSIZE, 0);
		
		// Vertex adjustment
		String[] labels = Map_Location_IREG.nodeInfo_header.split(",");
		int[] pop_indices = Map_Location_IREG.INDICES_POP_SIZES;

		HashMap<Integer, mxICell> vMap = getVertexToCellMap();
		for (Entry<Integer, mxICell> vEnt : vMap.entrySet()) {
			getModel().setStyle(vEnt.getValue(), STYLE_LOC_DEF);
			HashMap<String, Object> nodeInfo = map.getNode_info().get(vEnt.getKey());
			if (nodeInfo == null) {
				System.out.printf("Note: Node #%d hidden due to missing info.\n", vEnt.getKey());
				getModel().setVisible(vEnt.getValue(), false);

			} else {
				StringBuilder nodeDisp = new StringBuilder();
				nodeDisp.append("<html>");
				nodeDisp.append("<table border='0'>");
				// Name
				nodeDisp.append("<tr>");
				nodeDisp.append("<td>");
				nodeDisp.append(vEnt.getKey());
				nodeDisp.append("</td>");
				nodeDisp.append("<td>");
				nodeDisp.append(nodeInfo.get(Map_Location_IREG.NODE_INFO_NAME).toString());
				nodeDisp.append("</td>");
				nodeDisp.append("</tr>");
				// Entry
				int[] popSize = (int[]) nodeInfo.get(Map_Location_IREG.NODE_INFO_POP_SIZE);
				for (int i = 0; i < popSize.length; i++) {
					nodeDisp.append("<tr>");
					nodeDisp.append("<td>");
					nodeDisp.append(labels[pop_indices[i]].substring(0, 3));
					nodeDisp.append("</td>");
					nodeDisp.append("<td>");
					nodeDisp.append(popSize[i]);
					nodeDisp.append("</td>");
					nodeDisp.append("</tr>");
				}
				nodeDisp.append("</table>");
				nodeDisp.append("</html>");
				getModel().setValue(vEnt.getValue(), nodeDisp.toString());
				
				// Size
				vEnt.getValue().getGeometry().setWidth(140);
				vEnt.getValue().getGeometry().setHeight(100);
				
			}

		}

	}
}
