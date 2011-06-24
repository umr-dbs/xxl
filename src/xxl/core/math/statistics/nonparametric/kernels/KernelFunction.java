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

import xxl.core.math.functions.RealFunction;

/** This class provides the usage of <tt>kernel functions</tt> for
 * statistical purposes. Especially in nonparametric statistics
 * <tt>kernel estimators</tt> play an important role. In accordance to the 
 * chosen <tt>kernel function</tt> the resulting <tt>kernel estimators</tt> are 
 * smooth and inherit the basic properties of the <tt>kernel function</tt>.
 * For this function different choices are possible
 * whereas a pdf is advisable. 
 * <br>
 * The statistical analysis of <tt>kernel estimators</tt> is much simpler
 * than for histograms whereas histograms are another relevant topic
 * in nonparametric statistics. For statistical applications
 * often some parameters respectively constants of the <tt>kernel 
 * function</tt> are required, e.g., the mean.
 * <br>
 * Fulfilling those requirements, this class provides an interface for one-dimensional <tt>kernel functions</tt> 
 * and their most important constants.<br>
 * Every implementing kernel function must provide the constant values of its
 * mean, its square root of the 2nd moment about the mean and its roughness.
 * These informations are required for computing the {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths bandwidths}
 * for kernel estimators (e.g. for the {@link xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator kernel
 * density estimator}).
 * <br>
 * For further informations about kernel estimators and especially their usage in statistical purposes see
 * [Sco92] David W. Scott. Multivariate Density Estimation:
 * Theory, Practice, and Visualization. John Wiley & Sons, inc. 1992.
 *
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelCDF
 */

public abstract class KernelFunction implements RealFunction {

	/** mean of the kernel function */
	protected double AVG;

	/** 2nd moment (variance) of the kernel function */
	protected double VAR;

	/** roughness of the kernel function */
	protected double R;

	/** Evaluates the kernel function at given point x.
	 * 
	 * @param x function argument
	 * @return f(x)
	 */
	public abstract double eval(double x);

	/** Returns the mean of the kernel function.
	 * 
	 * @return mean of the kernel function.
	 */
	public double avg() {
		return AVG;
	}

	/** Returns the variance of the kernel function.
	 * 
	 * @return variance of the kernel function, i.e.
	 * the second moment
	 */
	public double variance() {
		return VAR;
	}

	/** Returns the standard deviation of the kernel function.
	 * 
	 * @return standard deviation of the kernel function.
	 */
	public double stdDev() {
		return Math.sqrt(variance());
	}

	/** Returns the roughness of the kernel function.
	 * 
	 * @return roughness of the kernel function. 
	 * See [Sco92] David W. Scott. Multivariate Density Estimation:
	 * Theory, Practice, and Visualization. John Wiley & Sons, inc. 1992. pages 131 ff.
	 * for further infomation.
	 * ( R(f) = integral( f(s)^2 dx) ). See [Sco92] page 281 (notation).
	 */
	public double r() {
		return R;
	}
}
