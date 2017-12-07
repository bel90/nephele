package eu.stratosphere.nephele.visualization.swt;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.PaintObjectEvent;
import org.eclipse.swt.custom.PaintObjectListener;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.VerifyEvent;
import org.eclipse.swt.events.VerifyListener;
import org.eclipse.swt.graphics.GlyphMetrics;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * This class is for creating and formating the Text for
 * the Information Box
 * 
 * @author bel
 */
public class SWTInfoBox {
	
	/**
	 * The StyledText Box which shows the Information
	 */
	private StyledText styledText;
	
	/**
	 * Contains the Information about the chosen Vertex
	 */
	private String vertexInformation;
	
	private ManagementVertex actualVertex; 
	
	private GraphVisualizationData actualVisualizationData;

	SWTVisualizationGUI visualizationGUI;
	
	public SWTInfoBox(StyledText infoText, SWTVisualizationGUI visualizationGUI) {
		this.styledText = infoText;
		
		this.vertexInformation = "";
		
		this.styledText.setText("");
		
		this.visualizationGUI = visualizationGUI;
		
		//this.checkpointing = null;
		
		//this.nocheckpointing = null;
	}
	
	public String getInfoString() {
		//Concatinate all Informations and return them
		
		return this.vertexInformation;
	}
	
	public void updateInfoBox() {
		//If there are new Events, the Info Box will be updatet
		if (this.actualVertex != null && this.actualVisualizationData != null) {
			setVertexInformation(this.actualVertex, this.actualVisualizationData);
		}
		
	}
	
	//Wenn ein Knoten angeklickt wird, wird diese Funktion aufgerufen
	public void setVertexInformation(ManagementVertex managementVertex, GraphVisualizationData visualizationData) {
		
		this.actualVertex = managementVertex;
		this.actualVisualizationData = visualizationData;
		
		//Set the String for the styled Text
		String taskName = managementVertex.getName() + " (" + (managementVertex.getIndexInGroup() + 1) + " of "
				+ managementVertex.getNumberOfVerticesInGroup() + ")";
		
		String newInformation = ("Task Name: \n" + taskName
				+ "\n\nExecution State: \n" + managementVertex.getExecutionState().toString()
				+ "\n\nCheckpoint State: \n" + managementVertex.getCheckpointState()
				+ "\n\nInstance Name: \n" + managementVertex.getInstanceName()
				+ "\n\nInstance Type: \n" + managementVertex.getInstanceType()
				+ "\n\nCheckpoint Decision Reason: \n" + managementVertex.getCheckpointDecisionReason()
				+ "\n\nCheckpoint Size: \n" + managementVertex.getCheckpointSize());
		//System.out.println("Vertex Information: " + this.vertexInformation);
		
		if (managementVertex.getRecordSkipped() == true) {
			newInformation = (newInformation + "\n\nRecord skipped:\ntrue");
		}
		if (managementVertex.getReplayTimes() > 0) {
			newInformation = (newInformation + "\n\nReplayed: \n" + managementVertex.getReplayTimes() + " times");
		}
		
		
		this.vertexInformation = newInformation;
		
		
		this.visualizationGUI.addNoCheckpointListener(managementVertex.getID(), visualizationData.getJobID());
		this.visualizationGUI.addCheckpointListener(managementVertex.getID(), visualizationData.getJobID());
		
		this.styledText.setText(getInfoString());

	}
	
}
