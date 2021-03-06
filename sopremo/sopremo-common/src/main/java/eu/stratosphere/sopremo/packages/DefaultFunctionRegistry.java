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
package eu.stratosphere.sopremo.packages;

import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.sopremo.aggregation.Aggregation;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.function.Callable;
import eu.stratosphere.sopremo.function.JavaMethod;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class DefaultFunctionRegistry extends DefaultRegistry<Callable<?, ?>> implements IFunctionRegistry {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7660926261580208403L;

	private Map<String, Callable<?, ?>> methods = new HashMap<String, Callable<?, ?>>();

	private NameChooser nameChooser = new DefaultNameChooser(1, 0, 2, 3);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.AbstractMethodRegistry#findMethod(java.lang.String)
	 */
	@Override
	public Callable<?, ?> get(String name) {
		return this.methods.get(name);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.AbstractMethodRegistry#registerMethod(java.lang.String,
	 * eu.stratosphere.sopremo.function.MeteorMethod)
	 */
	@Override
	public void put(String name, Callable<?, ?> method) {
		this.methods.put(name, method);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IMethodRegistry#getRegisteredMethods()
	 */
	@Override
	public Set<String> keySet() {
		return Collections.unmodifiableSet(this.methods.keySet());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("Method registry: {");
		boolean first = true;
		for (final Entry<String, Callable<?, ?>> method : this.methods.entrySet()) {
			appendable.append(method.getKey()).append(": ");
			method.getValue().appendAsString(appendable);
			if (first)
				first = false;
			else
				appendable.append(", ");
		}
		appendable.append("}");
	}

	private static boolean isCompatibleSignature(final Method method) {
		final Class<?> returnType = method.getReturnType();
		if (!IJsonNode.class.isAssignableFrom(returnType))
			return false;

		final Class<?>[] parameterTypes = method.getParameterTypes();
		// check if the individual parameters match
		for (int index = 0; index < parameterTypes.length; index++)
			if (!IJsonNode.class.isAssignableFrom(parameterTypes[index])
				&& !(index == parameterTypes.length - 1 && method.isVarArgs() &&
				IJsonNode.class.isAssignableFrom(parameterTypes[index].getComponentType())))
				return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IFunctionRegistry#put(java.lang.String, java.lang.Class, java.lang.String)
	 */
	@Override
	public void put(String registeredName, Class<?> clazz, String staticMethodName) {
		final List<Method> functions = this.getCompatibleMethods(
			ReflectUtil.getMethods(clazz, staticMethodName, Modifier.STATIC | Modifier.PUBLIC));

		if (functions.isEmpty())
			throw new IllegalArgumentException(
				String.format("Method %s not found in class %s", staticMethodName, clazz));

		Callable<?, ?> javaMethod = this.get(registeredName);
		if (javaMethod == null || !(javaMethod instanceof JavaMethod))
			this.put(registeredName, javaMethod = this.createJavaMethod(registeredName, functions.get(0)));
		for (Method method : functions)
			((JavaMethod) javaMethod).addSignature(method);
	}

	protected JavaMethod createJavaMethod(String registeredName, final Method implementation) {
		final JavaMethod javaMethod = new JavaMethod(registeredName);
		javaMethod.addSignature(implementation);
		return javaMethod;
	}

	@Override
	public void put(final Class<?> javaFunctions) {
		final List<Method> functions = this.getCompatibleMethods(
			ReflectUtil.getMethods(javaFunctions, null, Modifier.STATIC | Modifier.PUBLIC));

		for (final Method method : functions)
			this.put(method);

		final List<Field> fields =
			ReflectUtil.getFields(javaFunctions, null, Modifier.STATIC | Modifier.FINAL | Modifier.PUBLIC);

		for (final Field field : fields)
			try {
				if (Aggregation.class.isAssignableFrom(field.getType())) {
					final Aggregation aggregation = (Aggregation) field.get(null);
					String name = this.getName(field);
					if (name == null)
						name = aggregation.getName();
					this.put(name, new AggregationFunction(aggregation));
				} else if (SopremoFunction.class.isAssignableFrom(field.getType())) {
					final SopremoFunction function = (SopremoFunction) field.get(null);
					String name = this.getName(field);
					if (name == null)
						name = function.getName();
					this.put(name, function);
				}
			} catch (Exception e) {
				SopremoUtil.LOG.warn(String.format("Cannot access constant %s: %s", field, e));
			}
		if (FunctionRegistryCallback.class.isAssignableFrom(javaFunctions))
			((FunctionRegistryCallback) ReflectUtil.newInstance(javaFunctions)).registerFunctions(this);
	}

	protected String getName(AccessibleObject object) {
		String name = null;
		final Name nameAnnotation = object.getAnnotation(Name.class);
		if (nameAnnotation != null)
			name = this.nameChooser.choose(nameAnnotation.noun(), nameAnnotation.verb(),
				nameAnnotation.adjective(),
				nameAnnotation.preposition());
		return name;
	}

	@Override
	public void put(final Method method) {
		Callable<?, ?> javaMethod = this.get(method.getName());
		if (javaMethod == null || !(javaMethod instanceof JavaMethod))
			this.put(method.getName(), javaMethod = this.createJavaMethod(method.getName(), method));
		((JavaMethod) javaMethod).addSignature(method);
	}

	private List<Method> getCompatibleMethods(final List<Method> methods) {
		final List<Method> functions = new ArrayList<Method>();
		for (final Method method : methods) {
			final boolean compatibleSignature = isCompatibleSignature(method);
			if (SopremoUtil.LOG.isDebugEnabled())
				SopremoUtil.LOG.debug(String.format("Method %s is %s compatible", method, compatibleSignature ? ""
					: " not"));
			if (compatibleSignature)
				functions.add(method);
		}
		return functions;
	}
}
