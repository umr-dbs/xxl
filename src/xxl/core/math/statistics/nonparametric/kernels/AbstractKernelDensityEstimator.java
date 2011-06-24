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
import xxl.core.math.functions.AbstractRealFunctionFunction;

/**
 * One main problem of nonparametric statistics is the estimation of the <tt>probability density
 * function (pdf)</tt> of a random variable. The estimation typically relies on a given sample, in our case
 * data given by objects of type <tt>Number</tt>. For density estimation kernel based techniques are quite 
 * robust and useful.
 * There, for each data point X_i a {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel} 
 * is centered at X_i. Then, the sum is built over all kernels.
 * The kernel density estimator inherits the basic properties of the
 * kernel function. For this function different choices are possible
 * whereas a pdf is advisable (that is restricted to [-1,1] in our case). The mostly used kernels are positive
 * and symmetric with a compact support, e.g., the {@link xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel 
 * Epanechnikow kernel} or the {@link xxl.core.math.statistics.nonparametric.kernels.GaussianKernel Gaussian kernel}.
 * The choice of the bandwidth h, which controls the degree of
 * smoothness, is crucial for the quality of the estimation. There is
 * a trade-off in choosing the bandwidth. If h is too high, the
 * resulting estimate is smooth and may hide local features of the
 * density. For a small bandwidth, the estimate will be spiky and
 * artificial artifacts may be introduced.
 * <br>
 * The estimator itself extends {@link xxl.core.math.functions.AbstractRealFunctionFunction AbstractRealFunctionFunction}. 
 * Thus, it is implemented as a one-dimensional
 * {@link xxl.core.functions.Function function} expecting
 * objects of type <tt>Number</tt> as well as a
 * {@link xxl.core.math.functions.RealFunction RealFunction} expecting
 * data of type <tt>double</tt>. 
 * <br>
 * The estimator is updatable, so one can use it as an aggregate function
 * combined with a special aggregation iterator.
 * Since this class provides a preimplementation it is abstract and implementing classes
 * only need to implement the method {@link #evalKDE(double x)}.
 * 
 * <b>Note</b>: A sample of any kind of data given by an iterator could
 * be easily obtained through the {@link xxl.core.cursors.mappers.ReservoirSampler reservoir sampler
 * cursor}.
 * <br>
 * Generally, a more detailed coverage of kernel based density estimation respectively cdf estimation
 * is given in [Sco92]: David W. Scott. Multivariate Density Estimation:
 * Theory, Practice, and Visualization. 1992.
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelDensityEstimator
 *
 */

public abstract class AbstractKernelDensityEstimator extends AbstractRealFunctionFunction {

	/** used kernel function for density estimation */
	protected KernelFunction kf;

	/** used bandwidth for density estimation */
	protected double h;

	/** used sample */
	protected Object[] sample;

	/** indicates whether sample, variance or bounds have changed */
	protected boolean hasChanged = true;

	/** confidence level*/
	protected double alpha;

	/** last computed function value */
	protected double y;

	/** last evaluated function value */
	protected double xLast;

	/**
	* Gives a skeleton implementation for a kernel based density estimation function.
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>Number</tt>.
	* @param h used bandwidth for computing the estimation
	* @param alpha confidence level used for computing an asymptotic pointwise confidence interval
	*/
	public AbstractKernelDensityEstimator(KernelFunction kf, Object[] sample, double h, double alpha) {
		this.kf = kf;
		this.sample = sample;
		this.h = h;
		this.alpha = alpha;
	}

	/**
	* Gives a skeleton implementation for a kernel based density estimation function.
	*
	* The thumb rule respectively normal scale rule is used
	* for computing the bandwidth.
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>Number</tt>.
	* @param h used bandwidth for computing the estimation
	*/
	public AbstractKernelDensityEstimator(KernelFunction kf, Object[] sample, double h) {
		this(kf, sample, h, -1.0);
	}

	/** Evaluates the kernel based density estimator at a given point x.
	 * 
	 * @param x argument where to evaluate the density estimation
	 * @return value of the estimated density at x
	 */
	public double eval(double x) {
		if (hasChanged())
			reinit();
		y = evalKDE(x);
		// store the last computed function value for further using (i.e. confidence)
		xLast = x;
		return y;
	}

	/** Evaluates the density estimator at given point x.
	 * 
	 * @param x argument where to evaluate the density estimation
	 * @return value of the estimated density at x
	 */
	protected abstract double evalKDE(double x);

	/** Sets a new sample. If the sample has changed, i.e. old_sample.equals (new_sample)
	 * returns false, <tt>changed</tt> will be set true.
	 * 
	 *@param newSample new sample
	 */
	public void setSample(Object[] newSample) {
		if (!newSample.equals(sample)) {
			hasChanged = true;
			sample = newSample;
		}
	}

	/** Returns the last used bandwidth for estimation.
	 * 
	 * @return the last used bandwidth
	 */
	public double getBandwidth() {
		return h;
	}

	/** Sets the new bandwidth used to compute the estimation.
	 * 
	 * @param h new bandwidth
	 */
	public void setBandwidth(double h) {
		this.h = h;
	}

	/** Indicates whether something has changed. Thus, a recomputation may become necessary.
	 * 
	 * @return true, if anything has changed.
	 */
	public boolean hasChanged() {
		return hasChanged;
	}

	/**
	 * Returns the current confidence interval for the last computed function point. 
	 * <BR>
	 * This method computes
	 * a pointwise confidence interval for the kernel density estimator.
	 * For further details see<br>
	 * [Hae91]: W. Haerdle, Smoothing Techniques, With Implementations in S,
	 * Springer, New York, 1991, p. 62.<br>
	 * 
	 * @throws UnsupportedOperationException if no confidence level alpha has been given and
	 * hence no confidence interval for the given value can be computed
	 * @return current epsilon of the confidence interval given by [ f(x)-epsilon_low, f(x)+epsilon_up]
	 */
	public double[] epsilon() throws UnsupportedOperationException {
		if (alpha < 0) {
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		} else {
			double[] re = new double[2];
			if (hasChanged)
				reinit();
			double f2nd =
				Kernels.kernelDerivativeEstimator(
					xLast,
					sample,
					2,
					KernelBandwidths.oversmoothingRule(
						sample.length,
						new GaussianKernel(),
						Math.sqrt(Statistics.sampleVariance(sample))));
			double c = 1.0 / (h * Math.pow(sample.length, 0.2));
			double r = (y * kf.r()) / c;
			double z = Statistics.normalQuantil(1.0 - (0.5 * alpha));
			re[1] = 0.5 * c * c * Math.sqrt(kf.r()) * f2nd - z * Math.sqrt(r);
			// upper confidence interval
			re[0] = 0.5 * c * c * Math.sqrt(kf.r()) * f2nd + z * Math.sqrt(r);
			// lower confidence interval
			re[1] = (-1.0) * re[1] / Math.pow(sample.length, 0.4);
			re[0] = re[0] / Math.pow(sample.length, 0.4);
			return re;
		}
	}

	/**
	 * Returns the current confidence interval for given value x.
	 * <BR>
	 * This method computes
	 * a pointwise confidence interval for the kernel density estimator.
	 * For further details see [Hae91]: W. Haerdle, Smoothing Techniques, With Implementations in S,
	 * Springer, New Yor, 1991, p. 62.<br>
	 * 
	 * @param x argument where to evaluate the confidence interval
	 * @throws UnsupportedOperationException if no confidence level alpha has been given and
	 * hence no confidence interval for the given value could be computed
	 * @return current epsilon of the confidence interval given by [ f(x)-epsilon, f(x)+epsilon]
	 */
	public double[] epsilon(double x) throws UnsupportedOperationException {
		if (alpha < 0)
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		y = eval(x);
		return epsilon();
	}

	/** Returns p=1-alpha.
	 * 
	 * @return given confidence, i.e., p = 1-alpha
	 * @throws UnsupportedOperationException if no confidence level has been given and
	 * hence no confidence interval can be computed
	 */
	public double confidence() throws UnsupportedOperationException {
		if (alpha < 0)
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		else
			return 1 - alpha;
	}

	/** Reinitializes the estimator after changes.*/
	protected void reinit() {
		hasChanged = false;
	}
}
