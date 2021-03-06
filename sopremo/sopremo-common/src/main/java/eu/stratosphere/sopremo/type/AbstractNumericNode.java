package eu.stratosphere.sopremo.type;

import eu.stratosphere.pact.common.type.Key;

/**
 * Abstract class to provide basic implementations for numeric type nodes.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public abstract class AbstractNumericNode extends AbstractJsonNode implements INumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 677420673530449343L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#isFloatingPointNumber()
	 */
	@Override
	public boolean isFloatingPointNumber() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#isIntegralNumber()
	 */
	@Override
	public boolean isIntegralNumber() {
		return false;
	}

	public static void checkNumber(IJsonNode node) {
		if (!(node instanceof INumericNode))
			throw new IllegalArgumentException("Not a number " + node);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#clone()
	 */
	@Override
	public AbstractNumericNode clone() {
		return (AbstractNumericNode) super.clone();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#isCopyable(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public boolean isCopyable(IJsonNode otherNode) {
		return otherNode instanceof INumericNode;
	}

	@Override
	public int compareTo(final Key other) {
		if (((IJsonNode) other).getType().isNumeric())
			return this.getDecimalValue().compareTo(((INumericNode) other).getDecimalValue());

		return super.compareTo(other);
	}
}
