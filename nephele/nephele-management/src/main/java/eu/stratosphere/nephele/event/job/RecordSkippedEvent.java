package eu.stratosphere.nephele.event.job;

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * An {@link RecordSkippedEvent} can be used to notify other objects about 
 * a skipped Record
 * 
 * @author bel
 */
public class RecordSkippedEvent extends AbstractEvent implements ManagementEvent  {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private final ManagementVertexID managementVertexID;

	/**
	 * Constructs a checkpoint size event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 */
	public RecordSkippedEvent(final long timestamp, final ManagementVertexID managementVertexID) {

		super(timestamp);
		this.managementVertexID = managementVertexID;
	}

	/**
	 * Constructs a new record skipped event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public RecordSkippedEvent() {

		this.managementVertexID = null;
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
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof RecordSkippedEvent)) {
			return false;
		}

		RecordSkippedEvent sizeEvent = (RecordSkippedEvent) obj;

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
				" skipped a Record.";
	}
	
	
}
