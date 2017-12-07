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

package eu.stratosphere.pact.common.plan;

/**
 * Implementing this interface allows a PlanAssembler to have a description
 * of the plan which can be shown to the user. For a more detailed description
 * of what should be included in the description see getDescription().
 * 
 * @author Moritz Kaufmann
 */
public interface PlanAssemblerDescription extends PlanAssembler
{
	/**
	 * Returns a description of the plan that is generated by the assembler and
	 * also of the arguments if they are available. The description should be simple
	 * HTML as it should be renderable in different environments (console,
	 * web interface, ...).
	 * Typical things that should be included are:
	 * - expected input format
	 * - description of output
	 * - description of what is done
	 * - description of arguments to customize plan if available
	 * 
	 * @return A description of the plan assembler and of its arguments if available.
	 */
	String getDescription();
}
