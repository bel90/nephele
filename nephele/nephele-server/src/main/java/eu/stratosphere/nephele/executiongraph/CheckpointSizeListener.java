package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Classes implementing the {@link CheckpointSizeListener} interface can register for notifications about checkpoint size changes
 * of a vertex's checkpoint.
 * 
 * @author bel
 */
public interface CheckpointSizeListener {
	
	/**
	 * Called to notify the reason for the checkpoint decision
	 * 
	 * @param jobID
	 *        the ID of the job the event belongs to
	 * @param vertexID
	 *        the ID of the vertex whose checkpoint state has changed
	 * @param size
	 *        the size of the checkpoint
	 */
	void checkpointSize(JobID jobID, ExecutionVertexID vertexID, int size);
}
