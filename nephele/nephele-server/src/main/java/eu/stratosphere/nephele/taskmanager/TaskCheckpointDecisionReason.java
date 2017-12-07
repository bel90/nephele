package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.executiongraph.CheckpointDecisionReason;
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
public class TaskCheckpointDecisionReason {
	
	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final CheckpointDecisionReason reason;

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
	public TaskCheckpointDecisionReason(final JobID jobID, final ExecutionVertexID id, final CheckpointDecisionReason reason) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (id == null) {
			throw new IllegalArgumentException("Argument id must not be null");
		}

		if (reason == null) {
			throw new IllegalArgumentException("Argument checkpointDecisionReason must not be null");
		}

		this.jobID = jobID;
		this.executionVertexID = id;
		this.reason = reason;
	}

	/**
	 * Creates an empty task checkpoint decision reason.
	 */
	public TaskCheckpointDecisionReason() {
		this.jobID = null;
		this.executionVertexID = null;
		this.reason = null;
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
	public CheckpointDecisionReason getCheckpointDecisionReason() {
		return this.reason;
	}

}
