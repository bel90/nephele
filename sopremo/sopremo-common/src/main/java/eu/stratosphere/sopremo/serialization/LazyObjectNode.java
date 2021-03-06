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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.AbstractObjectNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.util.AbstractIterator;
import eu.stratosphere.util.ConcatenatingIterable;
import eu.stratosphere.util.ConcatenatingIterator;

/**
 * This {@link IObjectNode} supports {@link PactRecord}s more efficient by working directly with the record instead of
 * transforming it to a JsonNode. The record is handled by a {@link ObjectSchema}.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 * @author Arvid Heise
 */
public class LazyObjectNode extends AbstractObjectNode {

	/**
	 * .
	 */
	private static final long serialVersionUID = 5777496928208571589L;

	private final PactRecord record;

	private final ObjectSchema schema;

	/**
	 * Initializes a LazyObjectNode with the given {@link PactRecord} and the given {@link ObjectSchema}.
	 * 
	 * @param record
	 *        the PactRecord that should be used
	 * @param schema
	 *        the ObjectSchema that should be used
	 */
	public LazyObjectNode(final PactRecord record, final ObjectSchema schema) {
		this.record = record;
		this.schema = schema;
	}

	@Override
	public void clear() {
		for (int i = 0; i < this.schema.getMappingSize(); i++)
			this.record.setNull(i);
		this.getOtherField().clear();
	}

	/**
	 * Returns the record.
	 * 
	 * @return the record
	 */
	PactRecord getRecord() {
		return this.record;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#compareToSameType(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public int compareToSameType(final IJsonNode other) {
		final LazyObjectNode node = (LazyObjectNode) other;
		final Iterator<Entry<String, IJsonNode>> entries1 = this.iterator(), entries2 = node.iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final Entry<String, IJsonNode> entry1 = entries1.next(), entry2 = entries2.next();
			final int keyComparison = entry1.getKey().compareTo(entry2.getKey());
			if (keyComparison != 0)
				return keyComparison;

			final int valueComparison = entry1.getValue().compareTo(node.get(entry1.getKey()));
			if (valueComparison != 0)
				return valueComparison;
		}

		if (!entries1.hasNext())
			return entries2.hasNext() ? -1 : 0;
		if (!entries2.hasNext())
			return 1;
		return 0;
	}

	private boolean fieldInSchema(final int index) {
		return index != -1;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#get(java.lang.String)
	 */
	@Override
	public IJsonNode get(final String fieldName) {
		final int index = this.schema.hasMapping(fieldName);
		if (this.fieldInSchema(index)) {
			IJsonNode node;
			if (this.record.isNull(index))
				node = MissingNode.getInstance();
			else
				node = SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
			return node;

			// return SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
		}
		return this.getOtherField().get(fieldName);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getFieldNames()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Iterable<String> getFieldNames() {
		return new ConcatenatingIterable<String>(this.schema.getMappings(),
			this.getOtherField().getFieldNames());
	}

	private IObjectNode getOtherField() {
		return (IObjectNode) SopremoUtil.unwrap(this.record.getField(this.schema.getMappingSize(),
			JsonNodeWrapper.class));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#iterator()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Entry<String, IJsonNode>> iterator() {

		final Iterator<Entry<String, IJsonNode>> iterator2 = this.getOtherField().iterator();
		final Iterator<Entry<String, IJsonNode>> iterator1 = new AbstractIterator<Map.Entry<String, IJsonNode>>() {

			int lastIndex = 0;

			@Override
			protected Entry<String, IJsonNode> loadNext() {
				while (this.lastIndex < LazyObjectNode.this.schema.getMappingSize()) {
					final String key = LazyObjectNode.this.schema.getMappings().get(this.lastIndex);
					if (!LazyObjectNode.this.record.isNull(this.lastIndex)) {
						final IJsonNode value = SopremoUtil.unwrap(LazyObjectNode.this.record.getField(this.lastIndex,
							JsonNodeWrapper.class));
						this.lastIndex++;
						return new AbstractMap.SimpleEntry<String, IJsonNode>(key, value);
					}

					this.lastIndex++;

				}
				return this.noMoreElements();

			}
		};

		return new ConcatenatingIterator<Map.Entry<String, IJsonNode>>(iterator1, iterator2);

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#put(java.lang.String, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IObjectNode put(final String fieldName, final IJsonNode value) {
		final int index = this.schema.hasMapping(fieldName);
		if (this.fieldInSchema(index)) {
			if (value.isMissing())
				this.record.setNull(index);
			else
				this.record.setField(index, value);

		} else if (value.isMissing())
			this.getOtherField().remove(fieldName);
		else
			this.getOtherField().put(fieldName, value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#putAll(eu.stratosphere.sopremo.type.JsonObject)
	 */
	@Override
	public IObjectNode putAll(final IObjectNode jsonNode) {
		for (final Entry<String, IJsonNode> entry : jsonNode)
			this.put(entry.getKey(), entry.getValue());
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#remove(java.lang.String)
	 */
	@Override
	public IJsonNode remove(final String fieldName) {
		final int index = this.schema.hasMapping(fieldName);
		if (this.fieldInSchema(index)) {
			IJsonNode node;
			if (this.record.isNull(index))
				node = MissingNode.getInstance();
			else {
				node = SopremoUtil.unwrap(this.record.getField(index, JsonNodeWrapper.class));
				this.record.setNull(index);
			}
			return node;
		}
		return this.getOtherField().remove(fieldName);
	}

	@Override
	public int size() {
		final IObjectNode others = this.getOtherField();
		// we have to manually iterate over our record to get his size
		// because there is a difference between NullNode and MissingNode
		int count = 0;
		for (int i = 0; i < this.schema.getMappingSize(); i++)
			if (!this.record.isNull(i))
				count++;
		return count + others.size();
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return 0;
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		throw new UnsupportedOperationException("Use other ObjectNode Implementation instead");
	}
}
