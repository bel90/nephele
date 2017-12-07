package eu.stratosphere.nephele.executiongraph;

/**
 * This enumeration defines the possibles reasons why a checkpoint is made or not.
 * @author bel
 *
 */
public enum CheckpointDecisionReason {
	
	UNDECIDED,
	
	/**
	 * If the Decision is made in CheckpointDecision:
	 */
	NEVER,
	
	ALWAYS, 
	
	NETWORK,
	
	NETWORKNOT,

	CPU,
	
	INOUTRATE,
	
	DYNAMICNOT,
	
	/**
	 * The Initial Checkpoint States can be UNDECIDED, NONE or PARTIAL
	 * (COMPLETE is not possible)
	 */
	
	INITIAL_UNDECIDED,
	
	INITIAL_NONE,
	
	INITIAL_PARTIAL,
	
	/**
	 * The User changed the Checkpoint manually
	 */
	
	USER_CHECKPOINTING,
	
	USER_NO_CHECKPOINTING;

}
