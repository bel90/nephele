package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Classes implementing the {@link CheckpointDecisionReasonListener} interface can register for notifications about state changes
 * of a vertex's checkpoint.
 * 
 * @author bel
 */
public interface CheckpointDecisionReasonListener {
	
	/**
	 * Called to notify the reason for the checkpoint decision
	 * 
	 * @param jobID
	 *        the ID of the job the event belongs to
	 * @param vertexID
	 *        the ID of the vertex whose checkpoint state has changed
	 * @param reason
	 *        the reason for the checkpoint decision
	 */
	void checkpointDecisionReason(JobID jobID, ExecutionVertexID vertexID, CheckpointDecisionReason reason);
}
