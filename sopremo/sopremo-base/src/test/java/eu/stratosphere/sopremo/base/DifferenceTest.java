package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class DifferenceTest extends EqualCloneTest<Difference> {
	@Test
	public void shouldSupportArraysOfPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Difference difference = new Difference();
		difference.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(difference);
		sopremoPlan.getInput(0).
			addArray(1, 2).
			addArray(3, 4).
			addArray(5, 6);
		sopremoPlan.getInput(1).
			addArray(1, 2).
			addArray(3, 4).
			addArray(7, 8);
		sopremoPlan.getExpectedOutput(0).
			addArray(5, 6);

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportComplexObject() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().put(CoreFunctions.class);

		final Difference difference = new Difference();
		difference.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(difference);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Peter Parker", "password", "q1w2e3r4", "id", 4);
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);

		sopremoPlan.run();
	}

	@Test
	public void shouldSupportPrimitives() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final Difference difference = new Difference();
		difference.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2).
			addValue(3);
		sopremoPlan.getInput(1).
			addValue(1).
			addValue(2).
			addValue(4);
		sopremoPlan.getExpectedOutput(0).
			addValue(3);

		sopremoPlan.run();
	}

	/**
	 * Checks whether difference of one source produces the source again.
	 */
	@Test
	public void shouldSupportSingleSource() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final Difference difference = new Difference();
		difference.setInputs(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2).
			addValue(3);
		sopremoPlan.getExpectedOutput(0).
			addValue(1).
			addValue(2).
			addValue(3);

		sopremoPlan.run();
	}

	/**
	 * Checks whether difference of more than two source produces the correct result.
	 */
	@Test
	public void shouldSupportThreeSources() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final Difference difference = new Difference();
		difference.setInputs(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(difference);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2).
			addValue(3);
		sopremoPlan.getInput(1).
			addValue(1).
			addValue(2).
			addValue(4);
		sopremoPlan.getInput(2).
			addValue(2).
			addValue(3).
			addValue(5);
		sopremoPlan.getExpectedOutput(0).
			setEmpty();

		sopremoPlan.run();
	}
}
