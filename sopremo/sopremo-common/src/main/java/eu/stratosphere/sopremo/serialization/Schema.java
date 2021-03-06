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
package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntSet;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * The schema-interface can be implemented to provide the functionality to convert between {@link PactRecord}s and
 * {@link IJsonNode}s.
 * 
 * @author Arvid Heise
 */
public interface Schema extends ISerializableSopremoType {

	// public static Schema Default = new Default();

	/**
	 * Returns the indices of all values that are especially separated to be used as keys.<br>
	 * The index of the payload should not be returned.
	 * 
	 * @return the indices
	 */
	public IntSet getKeyIndices();

	/**
	 * Specifies the expected classes of the fields of the {@link PactRecord}.
	 * 
	 * @return the classes of the {@link PactRecord}
	 */
	public Class<? extends Value>[] getPactSchema();

	/**
	 * Determines the index of the given expression within this schema.
	 * 
	 * @param expression
	 *        the expressionthat should be used
	 * @return the index
	 */
	public IntSet indicesOf(EvaluationExpression expression);

	// public static class Default implements Schema {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 4142913511513235355L;
	//
	// private static final Class<? extends Value>[] PactSchema = SchemaUtils.combineSchema(JsonNodeWrapper.class);
	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.type.Schema#getPactSchema()
	// */
	// @Override
	// public Class<? extends Value>[] getPactSchema() {
	// return PactSchema;
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.type.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode,
	// * eu.stratosphere.pact.common.type.PactRecord)
	// */
	// @Override
	// public PactRecord jsonToRecord(final IJsonNode value, PactRecord target) {
	// if (target == null)
	// target = new PactRecord(new JsonNodeWrapper());
	// else if (target.getNumFields() < 1) {
	// target.setField(0, new JsonNodeWrapper());
	// }
	// target.getField(0, JsonNodeWrapper.class).setValue(value);
	// // if (value instanceof IArrayNode) {
	// // target.getField(0, JsonNodeWrapper.class).setValue(((IArrayNode) value).get(0));
	// // target.getField(1, JsonNodeWrapper.class).setValue(((IArrayNode) value).get(1));
	// // } else {
	// // target.getField(0, JsonNodeWrapper.class).setValue(NullNode.getInstance());
	// // target.getField(1, JsonNodeWrapper.class).setValue(value);
	// // }
	// return target;
	// }
	//
	// /* (non-Javadoc)
	// * @see
	// eu.stratosphere.sopremo.serialization.Schema#indicesOf(eu.stratosphere.sopremo.expressions.EvaluationExpression)
	// */
	// @Override
	// public int[] indicesOf(EvaluationExpression expression) {
	// if (expression == EvaluationExpression.KEY)
	// return new int[] { 0 };
	// return new int[] { 1 };
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.type.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord,
	// * eu.stratosphere.sopremo.type.IJsonNode)
	// */
	// @Override
	// public IJsonNode recordToJson(final PactRecord record, final IJsonNode target) {
	// return record.getField(0, JsonNodeWrapper.class).getValue();
	// // final JsonNodeWrapper key = record.getField(0, JsonNodeWrapper.class);
	// // final JsonNodeWrapper value = record.getField(1, JsonNodeWrapper.class);
	// // return JsonUtil.asArray(key.getValue(), value.getValue());
	// // IJsonNode.Type type = IJsonNode.Type.values()[record.getField(0, JsonNodeWrapper.class).getValue()];
	// // if (target == null || target.getType() != type)
	// // target = InstantiationUtil.instantiate(type.getClazz(), IJsonNode.class);
	// // record.getFieldInto(1, target);
	// // return target;
	// }
	// }

	/**
	 * Converts the given {@link IJsonNode} to a {@link PactRecord}. If possible the given target-record will be reused.
	 * 
	 * @param value
	 *        the {@link IJsonNode}, which shall be transformed into a {@link PactRecord} using this Schema
	 * @param target
	 *        the target {@link PactRecord} or <code>null</code>, when it shall be created
	 * @param context
	 *        TODO
	 * @return the converted {@link IJsonNode}
	 */
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target);

	/**
	 * Converts the given {@link PactRecord} to an appropriate {@link IJsonNode}. If possible the given target-node will
	 * be reused.
	 * 
	 * @param record
	 *        which shall be transformed to a matching {@link IJsonNode} using this Schema
	 * @param target
	 *        in which the record shall be transformed into or <code>null</code>, when target shall be created
	 * @return transfomed Record
	 */
	public IJsonNode recordToJson(PactRecord record, IJsonNode target);
}
