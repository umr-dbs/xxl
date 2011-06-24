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

import xxl.core.math.Statistics;
import xxl.core.math.functions.Differentiable;
import xxl.core.math.functions.RealFunction;

/**
 * This class models the <tt>Gaussian kernel function</tt>. Thus, it extends
 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction KernelFunction}.
 * Since the primitive is known, this class also 
 * implements {@link xxl.core.math.functions.Integrable Integrable}.
 * 
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.Kernels
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 */

public class GaussianKernel extends KernelFunction implements Differentiable {

	/**
	 * Constructs a new GaussianKernel and initializes the parameters.
	 *
	 */
	public GaussianKernel() {
		AVG = 0.0;
		VAR = 1.0;
		R = 0.5 * Math.sqrt(Math.PI);
	}

	/**
	 * Evaluates the Gaussian kernel at x.
	 * 
	 * @param x point to evaluate
	 * @return value of the Gaussian kernel at x
	 */
	public double eval(double x) {
		return Statistics.gaussian(x);
	}

	/** Returns the first derivative of the Gaussian kernel function
	 * as {@link xxl.core.math.functions.RealFunction real-valued function}.
	 * For further derivatives see {@link Kernels#normalDerivatives( int, double)}.
	 *
	 * @return first derivative of the Gaussian kernel function
	 */
	public RealFunction derivative() {
		return new RealFunction() {
			public double eval(double x) {
				return Kernels.normalDerivatives(1, x);
			}
		};
	}
}
