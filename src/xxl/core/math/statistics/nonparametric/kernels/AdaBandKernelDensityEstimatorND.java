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

import java.util.Iterator;

import xxl.core.functions.AbstractFunction;
import xxl.core.util.Distance;
import xxl.core.util.DoubleArrays;

/**
 * This class provides a native implementation of a n-dimensional kernel based density estimator
 * based upon a sample of data given by objects of type <tt>double []</tt>.
 * This class implements a native kernel density estimator, meaning
 * no boundary treatment is used nor any other optimizations.
 * To every given n-dimensional sample value a bandwidth is needed. This bandwidth
 * is used for every dimension the kernel estimator will be evaluated at. Per default
 * the {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} 
 * rule is used
 * for computing the bandwidths, if they are not given otherwise. But one is able to give
 * otherwise computed bandwidths to this class.
 * For further details about the adaBand rule see <br>
 * [DG01]: Domeniconi, Carlotta. Gunopulos, Dimitios. An Efficent Approach for Approximation Multi-dimensional Range
 * Queries and Nearest Neighbor Classification in Large Datasets. 2001.
 * <br>
 * The estimator is updatable, so one can use it as an aggregate function
 * combined with a special aggregation iterator. But it is recommended <b>not</b>
 * to do so because recomputing the bandwidths could be quiet expensive,
 * especially if {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} is
 * used for computation. <br>
 * The estimator is implemented as a
 * {@link xxl.core.functions.Function function} expecting
 * objects of type <tt>double []</tt>. <br>
 * A sample of any kind of data given by an iterator could
 * be easily obtained through the {@link xxl.core.cursors.mappers.ReservoirSampler reservoir sampler
 * cursor}.
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see java.util.Iterator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelCDF
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil)
 */

public class AdaBandKernelDensityEstimatorND extends AbstractFunction {

	/** used kernel function to estimate the density */
	protected KernelFunctionND kf;

	/** used bandwidths for estimation according to the sample */
	protected double[] h;

	/** used sample given as objects of type <tt>double []</tt> */
	protected Object[] sample;

	/** indicates whether sample or bandwidths have changed */
	protected boolean hasChanged = true;

	/**
	* Constructs an estimator for a n-dimensional density function using the given
	* n-dimensional {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND kernel function}.
	* This class implements a native kernel density estimator, meaning
	* no boundary treatment is used nor any other optimizations.
	* Per default
	* the {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} rule is used
	* for computing the bandwidths, if they are not given otherwise. But one is able to give
	* otherwise computed bandwidths to this class.
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>double []</tt>.
	* @param h used bandwidths according to the used sample
	* @throws IllegalArgumentException if the number of bandwidths doesn't match the number of sample values
	*/
	public AdaBandKernelDensityEstimatorND(KernelFunctionND kf, Object[] sample, double[] h)
		throws IllegalArgumentException {
		this.kf = kf;
		this.sample = sample;
		this.h = h;
	}

	/**
	* Constructs an estimator for a n-dimensional density function using the given
	* n-dimensional {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND kernel function}.
	* This class implements a native kernel density estimator, meaning
	* no boundary treatment is used nor any other optimizations.
	* Per default
	* the {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} rule is used
	* for computing the bandwidths, if they are not given otherwise. 
	* <br>
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>double []</tt>.
	* @param data data used for computing the bandwidths based upon adaBand
	* @param distance {@link xxl.core.util.Distance distance} used for computing the bandwidths based upon adaBand
	* (see {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} for further details)
	* @param quantil parameter for computing the bandwidths based upon adaBand
	* (see {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#adaBand(Object[] sample, Iterator data, Distance distance, int quantil) adaBand} for further details)
	* @throws IllegalArgumentException if the number of bandwidths doesn't match the number of sample values
	*/
	public AdaBandKernelDensityEstimatorND(
		KernelFunctionND kf,
		Object[] sample,
		Iterator data,
		Distance distance,
		int quantil) {
		this(kf, sample, KernelBandwidths.adaBand(sample, data, distance, quantil));
	}

	/** Evaluates the n-dimensional kernel based density estimator at a given point x represented by an
	 * object of type <tt>double []</tt>.
	 * 
	 * @param x argument where to evaluate the density estimation
	 * @throws IllegalStateException if after calling {@link #setSample(Object[]) setSample(Object[])} or
	 * calling {@link #setBandwidth(double[]) setBandwidth(double[])} the number of bandwidth values doesn't
	 * match the number of sample values
	 * @return value of the estimated density at x
	 */
	public Object invoke(Object x) throws IllegalStateException {
		if (hasChanged) {
			if (h.length != sample.length)
				throw new IllegalStateException("number of bandwidths doesn't match number of sample values!");
		}
		double[] x0 = (double[]) x;
		double[] xi = null;
		double b = 0.0;
		int size = sample.length;
		double r = 0.0;
		for (int i = 0; i < size; i++) {
			xi = (double[]) sample[i]; // sample value X_i
			b = h[i]; // bandwidth for the sample value X_i
			//
			r = r + (kf.eval(DoubleArrays.mult(DoubleArrays.substract(x0, xi), (1.0 / b))) / b);
		}
		r = r / size;
		return new Double(r);
	}

	/** Sets a new sample. If the sample has changed, i.e. old_sample.equals (new_sample)
	 * returns false, <tt>changed</tt> will be set true.
	 * 
	 * @param newSample new sample to use
	 */
	public void setSample(Object[] newSample) {
		if (!newSample.equals(sample)) {
			hasChanged = true;
			sample = newSample;
		}
	}

	/** Sets the new bandwidths. If the bandwidths have changed,
	 * <tt>changed</tt> will be set true.
	 * 
	 * @param h new bandwidths to use
	 */
	public void setBandwidth(double[] h) {
		if (!(this.h == h)) {
			hasChanged = true;
			this.h = h;
		}
	}

	/** Returns the last used bandwidths for estimation.
	 * 
	 * @return last used bandwidths	 */
	public double[] getBandwidth() {
		return h;
	}

	/** Indicates whether something has changed. If so, a recomputation may become necessary.
	 * 
	 * @return true, if anything has changed.
	 */
	public boolean hasChanged() {
		return hasChanged;
	}
}
