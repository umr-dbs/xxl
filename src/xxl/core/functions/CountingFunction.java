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

package xxl.core.functions;

import java.util.List;

/**
 * A DecoratorFunction that provides additional information about
 * the number of function calls its inner function. 
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 */
public class CountingFunction<P,R> extends DecoratorFunction<P,R> {
	
	/**
	 * Returns a new instance of the class CountingFunction. The generic types
	 * the returned function are resolved by this method.
	 * 
	 * @param <X> the type of the wrapped function's parameters.
	 * @param <Y> the return type of the wrapped function.
	 * @param function the function to be wrapped.
	 * @return a new instance of the class CountingFunction.
	 */
	public static <X, Y> CountingFunction<X, Y> createFunction(Function<X, Y> function) {
		return new CountingFunction<X, Y>(function);
	}

	/**
	 * Counter for function calls.
	 */
	protected long calls;
	
	/**
	 * Decorates the given function by a call counter.
	 * 
	 * @param function the function to be decorated or wrapped.
	 */
	public CountingFunction(Function<P, R> function){
		super(function);
		this.calls = 0;
	}

	/**
	 * Passes the arguments to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 * 
	 * <p>Increments the call counter appropriately.</p>
	 *
	 * @param arguments arguments passed to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(List<? extends P> arguments) {
		calls++;
		return function.invoke(arguments);
	}

	/**
	 * Passes the (empty) argument to the decorated resp. wrapped function by
	 * calling the wrapped function's invoke method.
	 * 
	 * <p>Increments the call counter appropriately.</p>
	 *
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke() {
		calls++;
		return function.invoke();
	}

	/**
	 * Passes the argument to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 * 
	 * <p>Increments the call counter appropriately.</p>
	 *
	 * @param argument argument passed to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(P argument) {
		calls++;
		return function.invoke(argument);
	}

	/**
	 * Passes the arguments to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 * 
	 * <p>Increments the call counter appropriately.</p>
	 *
	 * @param argument0 first argument passed to the wrapped function.
	 * @param argument1 second argument passed to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(P argument0, P argument1) {
		calls++;
		return function.invoke(argument0, argument1);
	}
	
	/**
	 * Returns the number of function calls.
	 * 
	 * @return number of function calls.
	 */
	public long getNoOfCalls() {
		return calls;
	}
	
	/**
	 * Resets the call counter.
	 */
	public void resetCounter() {
		this.calls = 0;
	}
	
}
