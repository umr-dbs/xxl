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

package xxl.core.math.statistics.nonparametric.kernels;

import xxl.core.math.functions.Integrable;
import xxl.core.math.functions.RealFunction;

/**
 * This class models the <tt>Uniform kernel function</tt>. Thus, it extends
 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction KernelFunction}.
 * Since the primitive is known, this class also 
 * implements {@link xxl.core.math.functions.Integrable Integrable}.
 * 
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.Kernels
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 */

public class UniformKernel extends KernelFunction implements Integrable {

	/**
	 * Constructs a new UniformKernel and initializes the parameters.
	 *
	 */
	public UniformKernel() {
		AVG = 0.0;
		VAR = 1.0 / 3.0; // 1/3
		R = 0.5; // 1/2
	}

	/**
	 * Evaluates the Uniform kernel at x.
	 * 
	 * @param x point to evaluate
	 * @return value of the Uniform kernel at x
	 */
	public double eval(double x) {
		return 0.5 * xxl.core.math.Maths.characteristicalFunction(x, -1.0, 1.0);
	}

	/** Returns the primitive of the Uniform kernel function
	 * as {@link xxl.core.math.functions.RealFunction real-valued function}.
	 *
	 * @return primitive of the Uniform kernel function
	 */
	public RealFunction primitive() {
		return new RealFunction() {
			public double eval(double x) {
				return (x <= 1.0) ? (0.5 * x + 0.5) * xxl.core.math.Maths.characteristicalFunction(x, -1.0, 1.0) : 1.0;
			}
		};
	}
}
