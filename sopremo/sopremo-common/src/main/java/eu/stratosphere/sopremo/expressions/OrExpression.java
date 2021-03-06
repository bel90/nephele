package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.expressions.tree.GenericListChildIterator;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Represents a logical OR.
 */
@OptimizerHints(scope = Scope.ANY)
public class OrExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1988076954287158279L;

	private final List<BooleanExpression> expressions;

	/**
	 * Initializes an OrExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param expressions
	 *        the expressions which evaluate to the input for this OrExpression
	 */
	public OrExpression(final BooleanExpression... expressions) {
		this(Arrays.asList(expressions));
	}

	/**
	 * Initializes an OrExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param expressions
	 *        the expressions which evaluate to the input for this OrExpression
	 */
	public OrExpression(final List<BooleanExpression> expressions) {
		this.expressions = new ArrayList<BooleanExpression>(expressions);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final OrExpression other = (OrExpression) obj;
		return this.expressions.equals(other.expressions);
	}

	@Override
	public BooleanNode evaluate(final IJsonNode node) {
		// we can ignore 'target' because no new Object is created
		for (final EvaluationExpression booleanExpression : this.expressions)
			if (booleanExpression.evaluate(node) == BooleanNode.TRUE)
				return BooleanNode.TRUE;
		return BooleanNode.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ExpressionParent#iterator()
	 */
	@Override
	public ChildIterator iterator() {
		return new GenericListChildIterator<BooleanExpression>(this.expressions.listIterator()) {
			/*
			 * (non-Javadoc)
			 * @see
			 * eu.stratosphere.sopremo.expressions.tree.GenericListChildIterator#convert(eu.stratosphere.sopremo.expressions
			 * .EvaluationExpression)
			 */
			@Override
			protected BooleanExpression convert(EvaluationExpression childExpression) {
				return BooleanExpression.ensureBooleanExpression(childExpression);
			}
		};
	}

	/**
	 * Returns the expressions.
	 * 
	 * @return the expressions
	 */
	public List<BooleanExpression> getExpressions() {
		return this.expressions;
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = super.hashCode();
		result = prime * result + this.expressions.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#createCopy()
	 */
	@Override
	protected EvaluationExpression createCopy() {
		return new OrExpression(SopremoUtil.deepClone(this.expressions));
	}

	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		this.appendChildExpressions(appendable, this.expressions, " OR ");
	}

	/**
	 * Creates an OrExpression with the given {@link BooleanExpression}.
	 * 
	 * @param expression
	 *        the expression that should be used as the condition
	 * @return the created OrExpression
	 */
	public static OrExpression valueOf(final BooleanExpression expression) {
		if (expression instanceof OrExpression)
			return (OrExpression) expression;
		return new OrExpression(expression);
	}

	/**
	 * Creates an OrExpression with the given {@link BooleanExpression}s.
	 * 
	 * @param childConditions
	 *        the expressions that should be used as conditions for the created OrExpression
	 * @return the created OrExpression
	 */
	public static OrExpression valueOf(final List<? extends EvaluationExpression> childConditions) {
		final List<BooleanExpression> booleans = BooleanExpression.ensureBooleanExpressions(childConditions);
		if (booleans.size() == 1)
			return valueOf(booleans.get(0));
		return new OrExpression(booleans.toArray(new BooleanExpression[booleans.size()]));
	}

}