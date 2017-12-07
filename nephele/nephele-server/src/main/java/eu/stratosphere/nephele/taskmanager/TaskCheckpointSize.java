package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class can be used to propagate updates about a task's checkpoint decision reason from the
 * task manager to the job manager.
 * <p>
 * This class is thread-safe.
 * 
 * @author bel
 */
public class TaskCheckpointSize {
	
	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final int size;

	/**
	 * Creates a new task checkpoint decision reason.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param id
	 *        the ID of the task whose checkpoint state has changed
	 * @param checkpointDecisionReason
	 *        the new checkpoint decision reason to be reported
	 */
	public TaskCheckpointSize(final JobID jobID, final ExecutionVertexID id, final int size) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (id == null) {
			throw new IllegalArgumentException("Argument id must not be null");
		}

		if (size <= 0) {
			throw new IllegalArgumentException("Argument size must be greater 0");
		}

		this.jobID = jobID;
		this.executionVertexID = id;
		this.size = size;
	}

	/**
	 * Creates an empty task checkpoint decision reason.
	 */
	public TaskCheckpointSize() {
		this.jobID = null;
		this.executionVertexID = null;
		this.size = 0;
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

	/**
	 * Returns the new checkpoint decision reason.
	 * 
	 * @return the new checkpoint decision reason.
	 */
	public int getCheckpointSize() {
		return this.size;
	}

}
