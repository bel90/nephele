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

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.testing.PactRecordEqualer;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class HeadArraySchemaTest {
	@Test
	public void shouldConvertFromJsonToRecord() {
		final HeadArraySchema schema = new HeadArraySchema(2);
		final IArrayNode array = new ArrayNode();
		array.add(IntNode.valueOf(1));
		final PactRecord result = schema.jsonToRecord(array, null);

		final PactRecord expected = new PactRecord(3);
		expected.setField(0, SopremoUtil.wrap(IntNode.valueOf(1)));
		expected.setField(2, SopremoUtil.wrap(new ArrayNode()));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, schema.getPactSchema()));
	}

	@Test
	public void shouldConvertFromRecordToJson() {
		final PactRecord record = new PactRecord();
		final HeadArraySchema schema = new HeadArraySchema(2);

		record.setField(0, SopremoUtil.wrap(IntNode.valueOf(0)));
		record.setField(1, SopremoUtil.wrap(IntNode.valueOf(1)));
		record.setField(2, SopremoUtil.wrap(new ArrayNode(IntNode.valueOf(2))));

		final IArrayNode expected = new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2));
		final IJsonNode result = schema.recordToJson(record, null);

		Assert.assertEquals(expected, result);
	}

	@Test
	public void shouldKeepIdentityOnConversion() {
		final PactRecord record = new PactRecord();
		final HeadArraySchema schema = new HeadArraySchema(5);

		record.setField(0, SopremoUtil.wrap(IntNode.valueOf(0)));
		record.setField(1, SopremoUtil.wrap(IntNode.valueOf(1)));
		record.setField(5, SopremoUtil.wrap(new ArrayNode()));

		final IJsonNode node = schema.recordToJson(record, null);
		final PactRecord result = schema.jsonToRecord(node, null);

		Assert.assertTrue(PactRecordEqualer.recordsEqual(record, result, schema.getPactSchema()));
	}
}
