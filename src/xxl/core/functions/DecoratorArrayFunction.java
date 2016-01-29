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

import java.util.ArrayList;
import java.util.List;

/**
 * A DecoratorArrayFunction invokes each function given to the constructor on
 * the given parameters when calling the invoke method. Meaning this class
 * defines a function from an one dimensional object space to a n-dimensional
 * object space
 * 
 * <p>
 * <tt>f:O &rarr;O^n</tt>&ensp;with&ensp;<tt>f=(f1(a),f2(a),...,fn(a))</tt>
 * </p>
 * 
 * <p>for given functions&ensp;<tt>f1,...,fn</tt>&ensp;with&ensp;
 * <tt>fi:O&rarr;O, i=1,...,n</tt>&ensp; by composing them in a vector-like
 * manner.</p>
 * 
 * @param <P> the type of the decorator list-function's parameters.
 * @param <R> the most general return type of the component type of the list
 *        returned by the decorator list-function's invoke method.
 */
@SuppressWarnings("serial")
public class DecoratorArrayFunction<P, R> extends AbstractFunction<P, List<R>> {

	/**
	 * The functions are separately invoked on the given parameters when
	 * calling the invoke method.
	 */
	protected Function<? super P, ? extends R>[] functions;

	/**
	 * Constructs a new decorator array-function. The function i defined by
	 * <pre>
	 *   f : P --> R^n
	 * </pre>
	 * with
	 * <pre>
	 *   f = ( f1(a), f2(a), ... , fn(a) ) , fi : P --> R
	 * </pre>
	 * 
	 * @param functions the functions are separately invoked on the given
	 *        parameters when calling the invoke method.
	 */
	public DecoratorArrayFunction(Function<? super P, ? extends R>... functions) {
		this.functions = functions;
	}

	/**
	 * Calls the invoke methods of the wrapped functions on the given
	 * parameters. The results of this invocations are gathered in an array
	 * that is returned as the result of the invoke method.
	 * 
	 * @param arguments arguments passed to the given functions.
	 * @return an <code>List&lt;R&gt;</code> containing the results of the
	 *         invoked functions
	 */
	@Override
	public List<R> invoke(List<? extends P> arguments) {
		List<R> result = new ArrayList<R>(functions.length);
		for (Function<? super P, ? extends R> function : functions)
			result.add(function.invoke(arguments));
		return result;
	}
}
