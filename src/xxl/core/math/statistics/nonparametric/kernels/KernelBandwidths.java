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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.queues.Heap;
import xxl.core.util.Distance;

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
 * <br>
 * Besides these applications estimators based on kernels may also be applicable in other scenarios.
 * Again, the choice of the bandwidth is crucial for the quality of these estimators. 
 * <br>
 * This class provides some static methods for dealing with bandwidths for
 * kernel estimators. Since there are many choices for bandwiths we refer for a
 * more detailed coverage
 * to <br>[Sco92]: David W. Scott. Multivariate Density Estimation:
 * Theory, Practice, and Visualization. 1992 
 * and <br>
 * [WJ95]: M.P. Wand and M.C. Jones. Kernel Smoothing.
 * 1995. pages 60 ff.
 * 
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelCDF
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 *
 */

public class KernelBandwidths {

	/** indicates the use of type {@link #oversmoothingRule(int,double,double,double) } as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int OVERSMOOTHING_RULE_1D = 0;

	/** indicates the use of type {@link #normalScaleRule(int,double,double,double)} as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int THUMB_RULE_1D = 1;

	/** indicates the use of type {@link #directNSPlugInRule2Stage(Object[],int,KernelFunction,double) } as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int DPI2_RULE_1D = 2;

	/** indicates the use of type {@link #maximumLikelihoodCV(Object[],KernelFunction,double) } as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int MLCV_RULE = 3;

	/** indicates the use of type {@link #adaBand(Object[],Iterator,Distance,int) } as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int ADABAND_RULE = 4;

	/** indicates the use of type {@link #scottsRule(double[],int) } as bandwidth strategy
	 * @see #computeBandWidth1D(int type,Object[] sample,KernelFunction kf,double var,double min,double max)
	 */
	public static final int SCOTTS_RULE = 5;

	/** Computes the bandwidth of a specified type for a given kernel estimator.
	 * The kernel estimator relies on a sample and a kernel function with a known variance.
	 * 
	 * @param type bandwidth strategy
	 * @param sample used sample to compute the estimation. Given as Objects of type <TT>Number</TT>.
	 * @param kf kernel function 
	 * @param var variance of the kernel function
	 * @param min minimum of the data
	 * @param max maximum of the data
	 * @return bandwidth according to the chosen strategy
	 */
	public static double computeBandWidth1D(
		int type,
		Object[] sample,
		KernelFunction kf,
		double var,
		double min,
		double max) {
		switch (type) {
			case KernelBandwidths.OVERSMOOTHING_RULE_1D :
				return KernelBandwidths.oversmoothingRule(sample.length, kf, var);
			case KernelBandwidths.DPI2_RULE_1D :
				return KernelBandwidths.directNSPlugInRule2Stage(sample, sample.length, kf, var);
			case KernelBandwidths.MLCV_RULE :
				return KernelBandwidths.maximumLikelihoodCV(
					sample,
					kf,
					KernelBandwidths.oversmoothingRule(sample.length, kf, var));
			case KernelBandwidths.SCOTTS_RULE :
				return KernelBandwidths.scottsRule(new double[] { var }, sample.length)[0];
			case KernelBandwidths.THUMB_RULE_1D :
				return KernelBandwidths.normalScaleRule(sample.length, kf, var);
		}
		throw new IllegalArgumentException("No bandwidth rule of this type is supported!");
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the oversmoothing or maximal principle rule.
	 * For further informations see
	 * [WJ95]: M.P. Wand and M.C. Jones. Kernel Smoothing.
	 * Chapman & Hall. 1995. pages 60 ff.
	 *
	 * @param n size of the used sample
	 * @param var variance of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}.
	 * @param r R value (roughness) of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}.
	 * ( R(f) = integral( f(x)^2 dx) ). See [Sco92] page 281 (notation).
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * standard deviation or the inter quartil range of the data
	 * @return bandwidth computed with the oversmoothing rule
	 * 
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction#r()
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction#variance()
	 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
	 */
	public static double oversmoothingRule(int n, double var, double r, double s) {
		double re = 0.0;
		re = 243.0 * r;
		re = re / (35.0 * var * n);
		re = Math.pow(re, 0.2);
		re = re * s;
		return re;
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the oversmoothing oder maximal principle rule.
	 * For further informations see
	 * [WJ95]: M.P. Wand and M.C. Jones. Kernel Smoothing.
	 * Chapman & Hall. 1995. pages 60 ff.
	 *
	 * @param n size of the used sample
	 * @param kf Used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * variance or the inter quartil range of the data
	 * @return a bandwidth computed with the oversmoothing rule
	 * 
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
	 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
	 */
	public static double oversmoothingRule(int n, KernelFunction kf, double s) {
		return oversmoothingRule(n, kf.variance(), kf.r(), s);
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the normal scale rule.
	 * For further informations see
	 * [Sco92]: David W. Scott. Multivariate Density Estimation:
	 * Theory, Practice, and Visualization. John Wiley & Sons, inc. 1992. pages 131 ff.
	 *
	 * The normal scale rule is also known as <tt>thumb rule</tt>.
	 * @param n size of the used sample
	 * @param var variance of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param r R value (roughness) of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}.
	 * ( R(f) = integral( f(x)^2 dx) ). See [Sco92] page 281 (notation)
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * standard deviation or the inter quartil range of the data
	 * @return a bandwidth computed with the normal scale rule
	 * 
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction#r()
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction#variance()
	 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
	 */
	public static double normalScaleRule(int n, double var, double r, double s) {
		double re = 0.0;
		re = 8.0 * Math.sqrt(Math.PI) * r;
		re = re / (3.0 * var * n);
		re = Math.pow(re, 0.2);
		re = re * s;
		return re;
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the normal scale rule.
	 * For further informations see <BR>
	 * [Sco92]: David W. Scott. Multivariate Density Estimation:
	 * Theory, Practice, and Visualization. John Wiley & Sons, inc. 1992. pages 131 ff.  <BR>
	 *
	 * The normal scale rule is also known as <tt>thumb rule</tt>.
	 * @param n size of the used sample
	 * @param kf Used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * variance or the inter quartil range of the data	 
	 * @return a bandwidth computed with the normal scale rule
	 * 
	 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
	 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
	 */
	public static double normalScaleRule(int n, KernelFunction kf, double s) {
		return normalScaleRule(n, kf.variance(), kf.r(), s);
	}

	/** Computes the bandwidths for each given sample value (equally for each dimension)
	 * as the distance of the k-nearest neighbour of the sample values wrt.&nbsp;the given entirety.
	 * <br>
	 * Based upon [DG01]: Domeniconi, Carlotta. Gunopulos, Dimitios. An Efficent Approach for Approximation Multi-dimensional Range
	 * Queries and Nearest Neighbor Classification in Large Datasets. 2001
	 * <br>
	 * @param sample <tt>Object []</tt> containing a sample with values of type <tt>double []</tt>
	 * @param data {@link java.util.Iterator iterator} delivering the entirety of the given sample
	 * @param distance distance function
	 * @param quantil use the distance of the quantil-nearest neighbuor to a sample value for bandwidth
	 * @return the bandwidths (equally for each dimension) of the corresponding sample values
	 */
	public static double[] adaBand(final Object[] sample, Iterator data, final Distance distance, int quantil) {
		double[] bandwidths = new double[sample.length];
		Heap[] heaps = new Heap[sample.length];
		double[] kmax = new double[sample.length];
		for (int i = 0; i < sample.length; i++) {
			final int j = i;
			(heaps[i] = new Heap(quantil, new Comparator() {
				Object reference = sample[j];
				public int compare(Object o1, Object o2) {
					double dist = (distance.distance(reference, o1) - distance.distance(reference, o2));
					return dist < 0 ? 1 : (dist > 0 ? -1 : 0);
				}
			})).open();
		}
		int count = -1;
		Object x = null;
		while (data.hasNext()) {
			x = data.next();
			count++;
			for (int i = 0; i < sample.length; i++) {
				if (count == 0) { //init kmax
					kmax[i] = distance.distance(sample[i], x);
				}
				if (count >= quantil) {
					if (distance.distance(sample[i], x) < kmax[i])
						heaps[i].replace(x);
					kmax[i] = distance.distance(sample[i], heaps[i].peek());
				} else {
					if (distance.distance(sample[i], x) > kmax[i])
						kmax[i] = distance.distance(sample[i], x);
					heaps[i].enqueue(x);
				}
			}
		}
		for (int i = 0; i < sample.length; i++) {
			bandwidths[i] = distance.distance(sample[i], heaps[i].dequeue());
			heaps[i].close();
		}
		return bandwidths;
	}

	/** Returns the bandwidths for a n-dimensional kernel estimator based upon
	 * Scott's rule.
	 * For further informations see <BR>
	 * [Sco92]: David W. Scott. Multivariate Density Estimation:
	 * Theory, Practice, and Visualization. John Wiley & Sons, inc. 1992. pages 131 ff.
	 *
	 * @param stdDev standard deviations of the data in the corresponding dimensions
	 * @param n sample size used for estimation
	 * @return array of type <tt>double[]</tt> containing the bandwidths for the corresponding dimensions
	 */
	public static double[] scottsRule(double[] stdDev, int n) {
		int d = stdDev.length;
		double[] r = new double[d];
		for (int i = 0; i < d; i++) {
			double p = (-1.0) / (d + 4);
			r[i] = stdDev[i] * Math.pow(n, p);
		}
		return r;
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the maximum likelihood cross validation.
	 * For further informations see
	 * [Har91]: W. Hardle. Smoothing techniques with implementation in S. 1991.
	 *
	 * @param sample used sample of the data given by Objects of type Number.
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param h used bandwidth for cross validation
	 * @return a bandwidth computed with maximum likelihood cross validation.
	 */
	public static double maximumLikelihoodCV(Object[] sample, KernelFunction kf, double h) {
		return maximumLikelihoodCV(sample, kf, new double[] { h })[0];
	}

	/** Computes a d-dimensional bandwidth for a kernel estimator
	 * based upon the maximum likelihood cross validation.
	 * For further informations see
	 * [Har91]: W. Hardle. Smoothing techniques with implementation in S. 1991.
	 *
	 * @param sample used sample of the data given by Objects of type <tt>Number</tt>.
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param hs used bandwidths for cross validation for the corresponding dimension
	 * @return bandwidths computed with maximum likelihood cross validation.
	 */
	public static double[] maximumLikelihoodCV(Object[] sample, KernelFunction kf, double[] hs) {
		double[] r = new double[hs.length];
		int n = sample.length;
		Arrays.fill(r, 0.0);
		for (int k = 0; k < r.length; k++) {
			for (int i = 0; i < n; i++) {
				double xi = ((Number) sample[i]).doubleValue();
				double score = 0.0;
				for (int j = 0; j < n; j++) {
					double xj = ((Number) sample[j]).doubleValue();
					if (i != j)
						score += kf.eval((xi - xj) / hs[k]);
				}
				// if a sample xi has no neigbours, so that K( xi-xj / h) == 0, treat it like an outlier (i.e. ignore it),
				// otherwise log( score ) = -infinity occurs
				if (score > 0.0)
					r[k] += Math.log(score);
				else
					n--;
				// work around, if a sample x_i had been treated as an outlier (negative bandwiths!)
			}
			r[k] = r[k] / n - Math.log((n - 1) * hs[k]);
		}
		return r;
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the 2-stage direct plug in rule using a normal kernel function for
	 * estimating the roughness of the plug-in values.
	 * For further informations see
	 * [WJ95]: M.P. Wand and M.C. Jones. Kernel Smoothing.
	 * Chapman & Hall. 1995. pages 71 ff.
	 *
	 * @param sample used sample of the data given by Objects of type <tt>Number</tt>
	 * @param n size of the used sample
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * standard deviation or the inter quartil range of the data
	 * @return bandwidth computed with the 2-stage direct plug in rule
	 */
	public static double directNSPlugInRule2Stage(Object[] sample, int n, KernelFunction kf, double s) {
		return directNSPlugInRule2Stage(sample, n, kf.variance(), kf.r(), s);
	}

	/** Computes a one-dimensional bandwidth for a kernel estimator
	 * based upon the 2-stage direct plug in rule using a normal kernel function for
	 * estimating the roughness of the plug-in values.
	 * For further informations see
	 * [WJ95]: M.P. Wand and M.C. Jones. Kernel Smoothing.
	 * Chapman & Hall. 1995. pages 71 ff.
	 *
	 * @param sample used sample of the data given by Objects of type Number
	 * @param n size of the used sample
	 * @param var variance of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}.
	 * @param r R value (roughness) of the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}.
	 * ( R(f) = integral( f(x)^2 dx) ). See [Sco92] page 281 (notation).
	 * @param s estimator for the spread of the data, e.g., an estimator for the 
	 * standard deviation or the inter quartil range of the data.
	 * @return bandwidth computed with the 2-stage direct plug in rule
	 */
	public static double directNSPlugInRule2Stage(Object[] sample, int n, double var, double r, double s) {
		// compute phi_8_NS 
		double phi8 = 105.0 / (32.0 * Math.sqrt(Math.PI) * Math.pow(s, 9.0));
		// compute g1
		double k6 = Kernels.normalDerivativeAt0(6);
		double g1 = (-2.0 * k6) / (Math.sqrt(var) * phi8 * n);
		g1 = Math.pow(g1, (1.0 / 9.0));
		// use g1 with the roughness estimator
		double phi6 = Kernels.roughnessEstimator(g1, sample, 6);
		// compute g2
		double k4 = Kernels.normalDerivativeAt0(4);
		double g2 = (-2.0 * k4) / (Math.sqrt(var) * phi6 * n);
		g2 = Math.pow(g2, (1.0 / 7.0));
		// use g2 with the roughness estimator
		double phi4 = Kernels.roughnessEstimator(g2, sample, 4);
		// compute the bandwidth	
		double re = 0.0;
		re = r / (var * n * phi4);
		re = Math.pow(re, 0.2);
		return re;
	}

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private KernelBandwidths() {}
}
