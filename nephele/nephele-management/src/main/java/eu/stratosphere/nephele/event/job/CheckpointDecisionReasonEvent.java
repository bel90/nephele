package eu.stratosphere.nephele.event.job;

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * An {@link CheckpointDecisionReasonEvent} can be used to notify other objects about the reason
 * for the checkpoint decision
 * 
 * @author bel
 */
public class CheckpointDecisionReasonEvent extends AbstractEvent implements ManagementEvent  {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private final ManagementVertexID managementVertexID;

	/**
	 * The new state of the vertex's checkpoint.
	 */
	private final String checkpointDecisionReason;

	/**
	 * Constructs a checkpoint decision reason event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 * @param reason
	 *        the checkpoint decision reason of the vertex's checkpoint
	 */
	public CheckpointDecisionReasonEvent(final long timestamp, final ManagementVertexID managementVertexID,
			final String reason) {

		super(timestamp);
		this.managementVertexID = managementVertexID;
		this.checkpointDecisionReason = reason;
	}

	/**
	 * Constructs a new checkpoint decision reason change event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public CheckpointDecisionReasonEvent() {

		this.managementVertexID = null;
		this.checkpointDecisionReason = null;
	}

	/**
	 * Returns the ID of the vertex this event refers to.
	 * 
	 * @return the ID of the vertex this event refers to
	 */
	public ManagementVertexID getVertexID() {
		return this.managementVertexID;
	}

	/**
	 * Returns the checkpoint decision reason of the vertex's checkpoint.
	 * 
	 * @return the checkpoint decision reason of the vertex's checkpoint
	 */
	public String getCheckpointDecisionReason() {
		return this.checkpointDecisionReason;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof CheckpointDecisionReasonEvent)) {
			return false;
		}

		CheckpointDecisionReasonEvent decisionReasonEvent = (CheckpointDecisionReasonEvent) obj;
		if (!decisionReasonEvent.getCheckpointDecisionReason().equals(this.checkpointDecisionReason)) {
			return false;
		}

		if (!decisionReasonEvent.getVertexID().equals(this.managementVertexID)) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.checkpointDecisionReason != null) {
			return this.checkpointDecisionReason.hashCode();
		}

		if (this.managementVertexID != null) {
			return this.managementVertexID.hashCode();
		}

		return super.hashCode();
	}
	
	@Override
	public String toString() {
		
		return timestampToString(getTimestamp()) + ":\t Vertex with ID " + this.managementVertexID + 
				" checkpoint decision reason switched to " + this.checkpointDecisionReason;
	}
	
	
}
