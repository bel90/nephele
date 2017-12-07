package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class can be used to propagate that a Record is Skipped from the
 * task manager to the job manager.
 * <p>
 * This class is thread-safe.
 * 
 * @author bel
 */
public class TaskRecordSkipped {
	
	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	/**
	 * Creates a new task record skipped.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param id
	 *        the ID of the task 
	 */
	public TaskRecordSkipped(final JobID jobID, final ExecutionVertexID id) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (id == null) {
			throw new IllegalArgumentException("Argument id must not be null");
		}

		this.jobID = jobID;
		this.executionVertexID = id;
	}

	/**
	 * Creates an empty task record skipped.
	 */
	public TaskRecordSkipped() {
		this.jobID = null;
		this.executionVertexID = null;
	}

	/**
	 * Returns the ID of the job this update belongs to.
	 * 
	 * @return the ID of the job this update belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the ID of the vertex this update refers to
	 * 
	 * @return the ID of the vertex this update refers to
	 */
	public ExecutionVertexID getVertexID() {
		return this.executionVertexID;
	}

}
