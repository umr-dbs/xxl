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
import xxl.core.math.functions.AbstractRealFunctionFunction;
import xxl.core.math.functions.Differentiable;
import xxl.core.math.functions.RealFunction;

/** This class provides a difference kernel estimator (dke). A <tt>dke</tt>
 * is an estimator for the local differences of a continuous probability
 * density function. As proposed in<br>
 * [Qui94]: P. Qiu. Estimation of the number of jumps of the jump regression functions.
 * Communications in Statistics-Theory and Methods 23. 2141-2155. 1994,<br>
 * a dke could be used as an estimator for change points based
 * upon a sample drawn from a continuous entirety. 
 *
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 */

public class DifferenceKernelEstimator extends AbstractRealFunctionFunction implements Differentiable {

	/**
	 * Provides a FACTORY for constructing dke's.
	 */
	public static Function FACTORY = new AbstractFunction<Object,DifferenceKernelEstimator>() {
		public DifferenceKernelEstimator invoke(List<? extends Object> list) {
			KernelFunction kf = (KernelFunction) list.get(0);
			Object[] sample = (Object[]) list.get(1);
			double h = ((Number) list.get(2)).doubleValue();
			return new DifferenceKernelEstimator(kf, sample, h);
		}
	};

	/** used transformed kernel function on the left side of the difference */
	protected TransformedKernelFunction tkfLeft;

	/** used transformed kernel function on the right side of the difference */
	protected TransformedKernelFunction tkfRight;

	/** used bandwidth for estimation */
	protected double h;

	/** used sample */
	protected Object[] sample;

	/** indicates whether sample or something else has changed */
	protected boolean hasChanged = true;

	/** Constructs a new difference kernel estimator, which could be used for change-point-detection. 
	 *
	 * @param kf1 used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} for the left transformation
	 * @param l1 left interval border of the used interval for the left transformation.
	 * @param c1 transformed interval width of the left interval
	 *
	 * @param kf2 used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} for the right transformation
	 * @param l2 right interval border of the used interval for the right transformation.
	 * @param c2 transformed interval width of the right interval
	 *
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param h used bandwidth the compute the difference kernel estimator
	 *
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
	 * @see xxl.core.math.statistics.nonparametric.kernels.TransformedKernelFunction
	 */
	public DifferenceKernelEstimator(
		KernelFunction kf1,
		double l1,
		double c1,
		KernelFunction kf2,
		double l2,
		double c2,
		Object[] sample,
		double h) {
		tkfLeft = new TransformedKernelFunction(kf1, l1, c1);
		tkfRight = new TransformedKernelFunction(kf2, l2, c2);
		this.sample = sample;
		this.h = h;
	}

	/** Constructs a new difference kernel estimator, which could be used for change-point-detection. 
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} for the transformation
	 *
	 * @param l left interval border of the used interval for transformation.
	 * The left border will be given as <TT>(-1.0) * l - c</TT>.
	 *
	 * @param c transformed interval width
	 *
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 *
	 * @param h used bandwidth the compute the difference kernel estimator
	 *
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
	 * @see xxl.core.math.statistics.nonparametric.kernels.TransformedKernelFunction
	 */
	public DifferenceKernelEstimator(KernelFunction kf, double l, double c, Object[] sample, double h) {
		this(kf, (-1.0) * l - c, c, kf, l, c, sample, h);
	}

	/** Constructs a new difference kernel estimator, which could be used for change-point-detection. 
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} for the transformation
	 * to [-2.0, -1.0] and [1.0, 2.0]. This complies to calling the constructor with
	 * <code>DifferenceKernelEstimator ( kf, -2.0, 1.0, kf, 1.0, 1.0, sample, h)</code>.
	 *
	 * @param sample sample of a data set given as <tt>Object []</tt> containing
	 * objects of type <tt>Number</tt>
	 *
	 * @param h used bandwidth the compute the difference kernel estimator
	 *
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
	 * @see xxl.core.math.statistics.nonparametric.kernels.TransformedKernelFunction
	 */
	public DifferenceKernelEstimator(KernelFunction kf, Object[] sample, double h) {
		this(kf, 1.0, 1.0, sample, h);
	}

	/** Evaluates the difference kernel estimator at given point x.
	 * 
	 * @param x function argument
	 * @return value of the evaluated difference kernel estimator at x
	 */
	public double eval(double x) {
		double xi = 0.0;
		int size = sample.length;
		double r = 0.0;
		for (int i = 0; i < size; i++) {
			xi = ((Number) sample[i]).doubleValue();
			r += tkfLeft.eval((xi - x) / h) - tkfRight.eval((xi - x) / h);
		}
		r = r / size;
		r = r / h;
		return r;
	}

	/** Sets a new sample. If the sample has changed, i.e., <code>old_sample.equals( new_sample)</code>
	 * returns <code>false</code>, <tt>changed</tt> will be set to true.
	 * 
	 * @param newSample new sample
	 */
	public void setSample(Object[] newSample) {
		if (!newSample.equals(sample)) {
			hasChanged = true;
			sample = newSample;
		}
	}

	/** Returns the last used bandwidth for estimation.
	 * 
	 * @return last used bandwidth
	 */
	public double getBandwidth() {
		return h;
	}

	/** Indicates whether something has changed and a recomputation of some parameters
	 * is needed.
	 * 
	 * @return <code>true</code>, if anything has changed, otherwise <code>false</code>.
	 */
	public boolean hasChanged() {
		return hasChanged;
	}

	/** Returns the {@link xxl.core.math.functions.Differentiable derivatives} of the dke
	 * as {{@link xxl.core.math.functions.RealFunction RealFunction}.
	 *
	 * @return derivative of the dke based upon the
	 * {@link xxl.core.math.functions.Differentiable derivatives} of the used
	 * {@link xxl.core.math.statistics.nonparametric.kernels.TransformedKernelFunction transformed}
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel functions}
	 *
	 * @throws UnsupportedOperationException if the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.TransformedKernelFunction transformed kernel functions}
	 * and so the underlying
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel functions}
	 * are not differentiable.
	 */
	public RealFunction derivative() throws UnsupportedOperationException {
		// getting differentiables of transformed kernel functions (if supported)
		final RealFunction f1 = ((Differentiable) tkfLeft).derivative();
		final RealFunction f2 = ((Differentiable) tkfRight).derivative();
		// return the derivative
		return new RealFunction() {
			public double eval(double x) {
				double xi = 0.0;
				int size = sample.length;
				double r = 0.0;
				for (int i = 0; i < size; i++) {
					xi = ((Number) sample[i]).doubleValue();
					r += f2.eval((xi - x) / h) - f1.eval((xi - x) / h);
				}
				r = r / size;
				r = r / (h * h);
				return r;
			}
		};
	}
}
