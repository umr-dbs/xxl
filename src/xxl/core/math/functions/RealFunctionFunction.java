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

import xxl.core.functions.AbstractFunction;

/** This class provides a wrapper for objects of type {@link xxl.core.math.functions.RealFunction RealFunction}
 * for using them as {@link xxl.core.functions.Function Function}.
 *
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.functions.Function
 */

public class RealFunctionFunction extends AbstractFunction {

	/** real-valued function to wrap */
	protected RealFunction realFunction;

	/** Constructs a new wrapper for {@link xxl.core.math.functions.RealFunction real-valued functions} 
	 * for using them as {@link xxl.core.functions.Function Function}.
	 *
	 * @param realFunction {@link xxl.core.math.functions.RealFunction RealFunction} to wrap
	 */
	public RealFunctionFunction(RealFunction realFunction) {
		this.realFunction = realFunction;
	}

	/** Evaluates the {@link xxl.core.math.functions.RealFunction real-valued function} at a given point x.
	 * 
	 * @param o function argument of type {@link java.lang.Number Number} 
	 * @return f(o) as an object of type <tt>Double</tt> 
	 */
	public Object invoke(Object o) {
		return new Double(realFunction.eval(((Number) o).doubleValue()));
	}
}
