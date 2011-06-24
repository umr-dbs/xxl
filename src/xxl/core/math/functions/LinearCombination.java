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

/** This class provides a linear combination of two given {@link RealFunction RealFunctions}
 * as a {@link RealFunction RealFunction}. That means this class constructs a new {@link RealFunction} f
 * with f = w1 * f1 + w2 * f2 for given functions f1,f2 and weights w1,w2.
 * 
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 */

public class LinearCombination extends AbstractRealFunctionFunction {

	/** first RealFunction */
	final public RealFunction f1;

	/** scalar of the first RealFunction f1*/
	final public double w1;

	/** second RealFunction */
	final public RealFunction f2;

	/** scalar of the second RealFunction f2*/
	final public double w2;

	/** This class performs a linear combination of the given {@link RealFunction real-valued functions}
	 * with two given scalars. That means, this class constructs a new {@link RealFunction} f
	 * supporting f = w1 * f1 + w2 * f2.
	 * 
	 * @param f1 first RealFunction
	 * @param f2 second RealFunction
	 * @param w1 scalar of the first RealFunction
	 * @param w2 scalar of the second RealFunction
	 */
	public LinearCombination(RealFunction f1, double w1, RealFunction f2, double w2) {
		this.f1 = f1;
		this.f2 = f2;
		this.w1 = w1;
		this.w2 = w2;
	}

	/** This class performs a linear combination of the given {@link xxl.core.functions.Function 
	 * functions}
	 * with two scalars. That means, this class constructs a new {@link RealFunction} f
	 * supporting f = w1 * f1 + w2 * f2.
	 * 
	 * @param f1 first real-valued {@link xxl.core.functions.Function function}
	 * @param w1 scalar of the first function
	 * @param f2 second real-valued {@link xxl.core.functions.Function function}
	 * @param w2 scalar of the second function
	 */
	public LinearCombination(Function f1, double w1, Function f2, double w2) {
		this(new FunctionRealFunction(f1), w1, new FunctionRealFunction(f2), w2);
	}

	/** Evaluates the real-valued function at x.
	 * 
	 * @param x evaluation value
	 * @return  f(x) = w1 * f1(x) + w2 * f2(x)
	 */
	public double eval(double x) {
		return w1 * f1.eval(x) + w2 * f2.eval(x);
	}

	/** Returns a string representation of the object.
	 * 
	 * @return string representation of the object
	 */
	public String toString() {
		return super.toString() + "(" + w1 + " * " + f1 + " + " + w2 + " * " + f2 + ")";
	}
}
