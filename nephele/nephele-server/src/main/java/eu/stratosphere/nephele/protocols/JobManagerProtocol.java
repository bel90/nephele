/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.rpc.RPCProtocol;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointDecisionReason;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointSize;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointState;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.TaskRecordSkipped;

/**
 * The job manager protocol is implemented by the job manager and offers functionality
 * to task managers which allows them to register themselves, send heart beat messages
 * or to report the results of a task execution.
 * 
 * @author warneke
 */
public interface JobManagerProtocol extends RPCProtocol {

	/**
	 * Sends a heart beat to the job manager.
	 * 
	 * @param instanceConnectionInfo
	 *        the information the job manager requires to connect to the instance's task manager
	 * @param hardwareDescription
	 *        a hardware description with details on the instance's compute resources.
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	void sendHeartbeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription)
			throws IOException, InterruptedException;

	/**
	 * Reports an update of a task's execution state to the job manager.
	 * 
	 * @param taskExecutionState
	 *        the new task execution state
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	void updateTaskExecutionState(TaskExecutionState taskExecutionState) throws IOException, InterruptedException;

	/**
	 * Reports an update of a task's checkpoint state to the job manager.
	 * 
	 * @param taskCheckpointState
	 *        the new checkpoint state of the task
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	void updateCheckpointState(TaskCheckpointState taskCheckpointState) throws IOException, InterruptedException;
	
	/**
	 * Reports an update of a task's checkpoint decision reason to the job manager.
	 * 
	 * @param taskCheckpointDecisionReason
	 *        the new checkpoint decision reason of the task
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 * @author bel
	 */
	void updateCheckpointDecisionReason(TaskCheckpointDecisionReason taskCheckpointDecisionReason) throws IOException, InterruptedException;
	
	/**
	 * Reports an update of a task's checkpoint size to the job manager.
	 * 
	 * @param taskCheckpointSize
	 *        the new checkpoint size of the task
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 * @author bel
	 */
	void newCheckpointSize(TaskCheckpointSize taskCheckpointSize) throws IOException, InterruptedException;
	
	/**
	 * Reports an update that a Record was skipped to the job manager.
	 * 
	 * @param taskRecordSkipped
	 *        the information that a record was skipped
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 * @author bel
	 */
	void recordSkipped(TaskRecordSkipped taskRecordSkipped) throws IOException, InterruptedException;
	
	
}
