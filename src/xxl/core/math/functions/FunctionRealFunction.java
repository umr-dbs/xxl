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

package xxl.core.math.functions;

import xxl.core.functions.Function;

/**
 * This class provides a wrapper for objects of type
 * {@link xxl.core.functions.Function Function} working with
 * {@link java.lang.Number numerical data} for using them as a
 * {@link xxl.core.math.functions.RealFunction real-valued function}. The
 * wrapped {@link xxl.core.functions.Function Function} has to consume objects
 * of type <code>Double</code> and to return objects of type
 * {@link java.lang.Number Number}.
 *
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.functions.Function
 */

public class FunctionRealFunction implements RealFunction {

	/**
	 * The {@link xxl.core.functions.Function Function} to wrap.
	 */
	protected Function<? super Double, ? extends Number> function;

	/**
	 * Constructs a new object of this class.
	 * 
	 * @param function object of type
	 *        {@link xxl.core.functions.Function Function} to wrap.
	 */
	public FunctionRealFunction(Function<? super Double, ? extends Number> function) {
		this.function = function;
	}

	/**
	 * Evaluates the function f at the double value x.
	 * 
	 * @param x function argument.
	 * @return f(x).
	 */
	public double eval(double x) {
		return function.invoke(x).doubleValue();
	}

	/**
	 * Evaluates the function at the float value x.
	 * 
	 * @param x function argument.
	 * @return f(x).
	 */
	public double eval(float x) {
		return function.invoke((double)x).doubleValue();
	}

	/**
	 * Evaluates the function at the int value x.
	 * 
	 * @param x function argument.
	 * @return f(x).
	 */
	public double eval(int x) {
		return function.invoke((double)x).doubleValue();
	}

	/**
	 * Evaluates the function at the long value x.
	 * 
	 * @param x function argument.
	 * @return f(x).
	 */
	public double eval(long x) {
		return function.invoke((double)x).doubleValue();
	}

	/**
	 * Evaluates the function at the byte value x.
	 * 
	 * @param x function argument.
	 * @return f(x).
	 */
	public double eval(byte x) {
		return function.invoke((double)x).doubleValue();
	}
}
