package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Classes implementing the {@link RecordSkippedListener} interface can register for notifications about record skipping
 * of a vertex.
 * 
 * @author bel
 */
public interface RecordSkippedListener {
	
	/**
	 * Called to notify that a record were skipped
	 * 
	 * @param jobID
	 *        the ID of the job the event belongs to
	 * @param vertexID
	 *        the ID of the vertex who has skipped records
	 */
	void recordSkipped(JobID jobID, ExecutionVertexID vertexID);
}
