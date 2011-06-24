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

import xxl.core.util.Decorator;

/**
 * A decorator-function decorates (wraps) a function by passing given arguments
 * to the invoke method of the wrapped function. To enhance the functionality
 * of a function just inherit from this class and override the method of your
 * choice.
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 */
public class DecoratorFunction<P, R> implements Function<P, R>, Decorator<Function<? super P, ? extends R>> {

	/**
	 * The function to be decorated.
	 */
	protected Function<? super P, ? extends R> function;

	/**
	 * Constructs a new decorator-function wrapping the given function.
	 * 
	 * @param function the function to be decorated or wrapped.
	 */
	public DecoratorFunction(Function<? super P, ? extends R> function){
		this.function = function;
	}

	/**
	 * Constructs a new decorator-function without a function to decorate. This
	 * constuctor is only used by classes inherited from this class providing
	 * an enhanced functionality.
	 * 
	 * <p><b>Note</b>: Do not use this constructor by calling it directly.</p>
	 */
	public DecoratorFunction() {
		this(null);
	}

	/**
	 * Passes the arguments to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 *
	 * @param arguments arguments passed to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(List<? extends P> arguments) {
		return function.invoke(arguments);
	}

	/**
	 * Passes the (empty) argument to the decorated resp. wrapped function by
	 * calling the wrapped function's invoke method.
	 *
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke() {
		return function.invoke();
	}

	/**
	 * Passes the argument to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 *
	 * @param argument argument passed to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(P argument) {
		return function.invoke(argument);
	}

	/**
	 * Passes the arguments to the decorated resp. wrapped function by calling
	 * the wrapped function's invoke method.
	 *
	 * @param argument0 first argument passed to the wrapped function.
	 * @param argument1 second argument passewd to the wrapped function.
	 * @return the returned object of the wrapped function.
	 */
	@Override
	public R invoke(P argument0, P argument1) {
		return function.invoke(argument0, argument1);
	}

	/**
	 * Returns the decorated function.
	 * 
	 * @return the decorated function.
	 */
	@Override
	public Function<? super P, ? extends R> getDecoree() {
		return function;
	}
	
}
