/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
                        Head of the Database Research Group
                        Department of Mathematics and Computer Science
                        University of Marburg
                        Germany

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library;  If not, see <http://www.gnu.org/licenses/>. 

    http://code.google.com/p/xxl/

*/

package xxl.core.predicates;

import java.util.List;

import xxl.core.functions.Function;

/**
 * This class provides a wrapper that wraps a {@link Function function} to a
 * {@link Predicate predicate}. Only functions that's
 * {@link Function#invoke(Object)} method return nothing but
 * <code>Boolean</code> object can be wrapped.
 *
 * @param <P> the type of the predicate's parameters.
 * @see xxl.core.functions.PredicateFunction
 */
public class FunctionPredicate<P> extends AbstractPredicate<P> {

	/**
	 * A reference to the wrapped function. This reference is used to perform
	 * method calls on the underlying function.
	 */
	protected Function<? super P, Boolean> function;

	/**
	 * Constructs a new function predicate that wraps the specified function.
	 *
	 * @param function the function to be wrapped.
	 */
	public FunctionPredicate(Function<? super P, Boolean> function) {
		this.function = function;
	}

	/**
	 * Returns the result of the function as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying function with the given arguments and returns the
	 * primitive <code>boolean</code> value of it's result.
	 *
	 * @param arguments the arguments to the function.
	 * @return the result of the function as a primitive boolean value.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		return function.invoke(arguments);
	}

	/**
	 * Returns the result of the function as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying function and returns the primitive <code>boolean</code>
	 * value of it's result.
	 *
	 * @return the result of the function as a primitive boolean value.
	 */
	@Override
	public boolean invoke() {
		return function.invoke();
	}

	/**
	 * Returns the result of the function as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying function with the given argument and returns the
	 * primitive <code>boolean</code> value of it's result.
	 *
	 * @param argument the argument to the function.
	 * @return the result of the function as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument) {
		return function.invoke(argument);
	}

	/**
	 * Returns the result of the function as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying function with the given arguments and returns the
	 * primitive <code>boolean</code> value of it's result.
	 *
	 * @param argument0 the first argument to the function.
	 * @param argument1 the second argument to the function.
	 * @return the result of the function as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return function.invoke(argument0, argument1);
	}
}
