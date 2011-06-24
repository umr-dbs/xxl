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

/** 
 * Classes implementing this interface are either analytically or numerically
 * integrable, i.e., the primitive or an approximation of it 
 * can be computed by calling the method <code>primitive</code>. The primitive
 * is returned as a {@link xxl.core.math.functions.RealFunction real-valued function}.
 * 
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.functions.RealFunctionArea
 * @see xxl.core.math.numerics.integration.TrapezoidalRuleRealFunctionArea
 * @see xxl.core.math.numerics.integration.SimpsonsRuleRealFunctionArea
 */

public interface Integrable {

	/** Returns the primitive of the implementing class as
	 * a {@link xxl.core.math.functions.RealFunction real-valued function}.
	 *
	 * @return primitive as {@link xxl.core.math.functions.RealFunction}
	 */
	public abstract RealFunction primitive();
}
