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

import xxl.core.math.functions.AbstractRealFunctionFunction;
import xxl.core.math.functions.Integrable;
import xxl.core.math.queries.WindowQuery;

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
 * If the primitive of the kernel is known, this approach can be extended towards the estimation
 * of the <tt>cumulative distribution function (cdf)</tt> by simply integrating the kernel based density estimator appropriately. 
 * Picking up this idea, this class provides a skeleton implementation of a <tt>kernel based estimator</tt> for the 
 * cdf.
 * <br>
 * The estimator itself extends {@link xxl.core.math.functions.AbstractRealFunctionFunction AbstractRealFunctionFunction}. 
 * Thus, it is implemented as a one-dimensional
 * {@link xxl.core.functions.Function function} expecting
 * objects of type <tt>Number</tt> as well as a
 * {@link xxl.core.math.functions.RealFunction RealFunction} expecting
 * data of type <tt>double</tt>. Estimators for the cdf provide estimations of the
 * probability and in the database context this is applicable for selectivity estimation.
 * Concerning this regard, {@link xxl.core.math.queries.WindowQuery WindowQuery} is implemented.
 * <br>
 * Since this class provides a preimplementation, it is abstract and implementing classes
 * only need to implement the methods {@link #evalKDE(double x)} and {@link #windowQuery(Object left, Object right)}.
 * <b>Note</b>: A sample of any kind of data given by an iterator could
 * be easily obtained through the {@link xxl.core.cursors.mappers.ReservoirSampler reservoir sampler
 * cursor}.
 * <br>
 * Generally, a more detailed coverage of kernel based density estimation respectively cdf estimation
 * is given in [Sco92]: David W. Scott. Multivariate Density Estimation:
 * Theory, Practice, and Visualization. 1992.
 * 
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelCDF
 * @see xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelCDF
 *
 */

public abstract class AbstractKernelCDF extends AbstractRealFunctionFunction implements WindowQuery {

	/** used kernel function to estimate the density */
	protected KernelFunction kf;

	/** used bandwidth for estimation */
	protected double h;

	/** used sample */
	protected Object[] sample;

	/** indicates whether sample, variance or bounds have changed */
	protected boolean hasChanged = true;

	/**
	* Gives a skeleton implementation for a kernel based cdf estimator.
	*
	* @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function} 
	* with support restricted to [-1,1]
	* @param sample sample of a data set given as <tt>Object[]</tt> containing
	* objects of type <tt>Number</tt>.
	* @param h used bandwidth for computing the estimation
	* @throws IllegalArgumentException if a kernel function without restricted support to
	* <tt>[-1,1]</tt> is given or a kernel function is given that doesn't implement the
	* interface {@link xxl.core.math.functions.Integrable Integrable}
	*/
	public AbstractKernelCDF(KernelFunction kf, Object[] sample, double h) throws IllegalArgumentException {
	    if ((kf.eval(-1.1) != 0) || (kf.eval(1.1) != 0))
			throw new IllegalArgumentException("a kernel function with support restricted to [-1,1] must be given");
		if(!(kf instanceof Integrable)) throw new IllegalArgumentException("The given kernel provides no primitive function (i.e., implements NOT Integrable)");
	    this.kf = kf;
		this.sample = sample;
		this.h = h;
	}

	/** Evaluates the kernel based cdf estimator at a given point x.
	 * 
	 * @param x argument where to evaluate the cdf
	 * @return value of the estimated cdf at x
	 */
	public double eval(double x) {
		if (hasChanged())
			reinit();
		return evalKDE(x);
	}

	/** Evaluates the kernel based cdf estimator at given point x.
	 * 
	 * @param x argument where to evaluate the cdf
	 * @return value of the estimated cdf at x
	 */
	protected abstract double evalKDE(double x);

	/** Sets a new sample. If the sample has changed, i.e., if <TT>old_sample.equals (new_sample)</TT>
	 * returns false, <tt>changed</tt> will be set true.
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

	/** Indicates whether any parameter of the estimator has been changed and
	 * if, a recomputation may become necessary.
	 * 
	 * @return true, if something has changed
	 */
	public boolean hasChanged() {
		return hasChanged;
	}

	/** Reinitializes the estimator after changes.*/
	protected void reinit() {
		hasChanged = false;
	}

	/** Returns an estimation of the window (interval) given.
	 * 
	 * @param left left border of the window
	 * @param right right border of the window
	 * @return an estimation of the given window query
	 *
	 */
	public abstract double windowQuery(Object left, Object right);
}
