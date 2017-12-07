package eu.stratosphere.nephele.event.job;

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * An {@link CheckpointSizeEvent} can be used to notify other objects about the size
 * for the checkpoint 
 * 
 * @author bel
 */
public class CheckpointSizeEvent extends AbstractEvent implements ManagementEvent  {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private final ManagementVertexID managementVertexID;

	/**
	 * The new size of the vertex's checkpoint.
	 */
	private final int checkpointSize;

	/**
	 * Constructs a checkpoint size event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 * @param reason
	 *        the checkpoint size of the vertex's checkpoint
	 */
	public CheckpointSizeEvent(final long timestamp, final ManagementVertexID managementVertexID,
			final int size) {

		super(timestamp);
		this.managementVertexID = managementVertexID;
		this.checkpointSize = size;
	}

	/**
	 * Constructs a new checkpoint size change event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public CheckpointSizeEvent() {

		this.managementVertexID = null;
		this.checkpointSize = 0;
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
	 * Returns the checkpoint size of the vertex's checkpoint.
	 * 
	 * @return the checkpoint size of the vertex's checkpoint
	 */
	public int getCheckpointSize() {
		return this.checkpointSize;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof CheckpointSizeEvent)) {
			return false;
		}

		CheckpointSizeEvent sizeEvent = (CheckpointSizeEvent) obj;
		if (!(sizeEvent.getCheckpointSize() == this.checkpointSize)) {
			return false;
		}

		if (!sizeEvent.getVertexID().equals(this.managementVertexID)) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.managementVertexID != null) {
			return this.managementVertexID.hashCode();
		}

		return super.hashCode();
	}
	
	@Override
	public String toString() {
		
		return timestampToString(getTimestamp()) + ":\t Vertex with ID " + this.managementVertexID + 
				" checkpoint size switched to " + this.checkpointSize;
	}
	
	
}
