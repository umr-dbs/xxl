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

import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.functions.Integrable;
import xxl.core.math.functions.RealFunction;

/**
 * This class implements an abstract preimplementation 
 * of a <tt>kernel based cdf estimator</tt> {@link xxl.core.math.statistics.nonparametric.kernels.AbstractKernelCDF AbstractKernelCDF}
 * that uses reflection for avoiding boundary effects.
 * Additionally, this class provides a FACTORY for generating <tt>reflection kernel based cdf estimators</tt>.
 * 
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelCDF
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelCDF
 */

public class ReflectionKernelCDF extends AbstractKernelCDF {

	/** Provides a factory for a
	 * {@link xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelCDF reflection based kernel distribution estimator}
	 * , i.e.,&nbsp;an estimator
	 * using reflection for boundary treatment.
	 * The parameters needed for construction are passed to the factory by an
	 * <tt>Object[]</tt> <code>o</code> containing: <BR>
	 * <code>o[0]</code>: used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} <BR>
	 * <code>o[1]</code>: used sample as <tt>Object[]</tt> containing numerical data <BR>
	 * <code>o[2]</code>: used bandwidth as Object of type <tt>Number</tt> <BR>
	 * <code>o[3]</code>: minimum of the data space as Object of type <tt>Number</tt><BR>
	 * <code>o[4]</code>: maximum of the data space as Object of type <tt>Number</tt><BR>
	 */
	public static Function FACTORY = new AbstractFunction<Object,ReflectionKernelCDF>() {
		public ReflectionKernelCDF invoke(List<? extends Object> list) {
			return new ReflectionKernelCDF((KernelFunction) list.get(0),
				// kernel function
		 (Object[]) list.get(1), // sample
		 ((Number) list.get(2)).doubleValue(), // bandwidth
		 ((Number) list.get(3)).doubleValue(), // minimum
		 ((Number) list.get(4)).doubleValue() // maximum
	);
		}
	};

	/** left border of the range supported by the estimator */
	protected double min;

	/** right border of the range supported by the estimator */
	protected double max;

	/**
	* Constructs a new kernel based cdf estimator.
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} with restricted
	* support [-1,1]
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>Number</tt>.
	* @param h used bandwidth for computing the estimation
	* @param min left border of the range supported by the estimator
	* @param max right border of the range supported by the estimator
	* @throws IllegalArgumentException if a kernel function without restricted support to
	* <tt>[-1,1]</tt> is given or a kernel function is given that doesn't implement the
	* interface {@link xxl.core.math.functions.Integrable Integrable}
	*/
	public ReflectionKernelCDF(KernelFunction kf, Object[] sample, double h, double min, double max)
		throws IllegalArgumentException {
		super(kf, sample, h);
		this.min = min;
		this.max = max;
	}

	/** Evaluates the cdf estimator at the given point x.
	 * 
	 * @param x argument where to evaluate the cdf
	 * @return value of the estimated cdf at x
	 */
	protected double evalKDE(double x) {
		if (x > max)
			return 1.0;
		if (x < min)
			return 0.0;
		// ---
		double r = 0.0;
		RealFunction prim = ((Integrable) kf).primitive();
		int size = sample.length;
		for (int i = 0; i < size; i++) {
			double xi = ((Number) sample[i]).doubleValue();
			if (xi <= x - h)
				r += 1.0;
			else {
				if (xi <= x + h) {
					r += prim.eval((x - xi) / h);
				}
				// left border
				if (x <= min + h)
					r += prim.eval((x - ReflectionKernelCDF.verticalFlip(xi, min)) / h);
				// right border
				if (x >= max - h)
					r += prim.eval((x - ReflectionKernelCDF.verticalFlip(xi, max)) / h);
			}
		}
		return r / size;
	}

	/** Returns an estimation of the window (interval) selectivity.
	 * 
	 * @param left left border of the window
	 * @param right right border of the window
	 * @return estimation of the window selectivity
	 */
	public double windowQuery(Object left, Object right) {
		if (hasChanged())
			reinit();
		double a = ((Number) left).doubleValue();
		double b = ((Number) right).doubleValue();
		double r = 0.0;
		int size = sample.length;
		for (int i = 0; i < size; i++) {
			double xi = ((Number) sample[i]).doubleValue();
			if ((xi >= a - h) & (xi <= b + h)) {
				if ((xi >= a + h) & (xi <= b - h))
					r += 1.0;
				else {
					r += weight(xi, a, b);
				}
			}
			// left border
			if (a <= min + h)
				r += weight(ReflectionKernelCDF.verticalFlip(xi, min), a, b);
			// right border
			if (b >= max - h)
				r += weight(ReflectionKernelCDF.verticalFlip(xi, max), a, b);
		}
		return r / size;
	}

	/** Computes the weight a single kernel function gives to an interval.
	 * 
	 * @param xi sample point where to place the 'bump' of the kernel function
	 * @param a left border of the interval
	 * @param b right border of the interval
	 * @return the weight a single kernel function gives to an interval
	 */
	private double weight(double xi, double a, double b) {
		double al = Math.max((a - xi) / h, -1.0);
		// temp var for restricted kernel support
		double bl = Math.min((b - xi) / h, 1.0);
		// temp var for restricted kernel support
		RealFunction prim = ((Integrable) kf).primitive();
		return prim.eval(bl) - prim.eval(al);
	}

	/** Sets the new bounds, e.g., lower and upper border for the given data or the treated domain space.
	 * 
	 * @param newMin new lower border
	 * @param newMax new upper border
	 */
	public void setBounds(double newMin, double newMax) {
		min = newMin;
		max = newMax;
	}

	/** Flip the given x value at the given y-axis (x coordinate of the x-Axis).
	 * 
	 * @param x point to flip
	 * @param yaxis where to flip the point
	 * @return flipped x value as 2 * yaxis -x
	 */
	public static double verticalFlip(double x, double yaxis) {
		return 2 * yaxis - x;
	}
}
