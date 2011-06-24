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

package xxl.core.math.statistics.nonparametric;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.functions.AbstractRealFunctionFunction;
import xxl.core.math.queries.WindowQuery;

/**
 * This class realizes a real-valued function providing an empirical cdf
 * (cumulative distribution function)
 * based on a simple random sample (iid)
 * for data given by objects of type <tt>Number</tt>. <br>
 * The empirical distribution function <tt>F_n(y)</tt> of a sample
 * is the proportion of observations less than or equal to <tt>y</tt>.
 * <br>
 * Generally, a more detailed coverage of the empirical cdf
 * is given in [Sco92]: David W. Scott. Multivariate Density Estimation:
 * Theory, Practice, and Visualization. 1992.
 * <br>
 * The cdf is implemented as a one-dimensional
 * {@link xxl.core.functions.Function function} expecting
 * objects of type <tt>Number</tt> as well as a
 * {@link xxl.core.math.functions.RealFunction RealFunction} expecting
 * data of type <tt>double</tt>.
 * A sample of any kind of data given by an iterator could
 * be easily obtained through the {@link xxl.core.cursors.mappers.ReservoirSampler reservoir sampler
 * cursor}.
 *
 * @see xxl.core.functions.Function
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.queries.WindowQuery
 */

public class EmpiricalCDF extends AbstractRealFunctionFunction implements WindowQuery {

	/** Provides a factory for a
	 * {@link xxl.core.math.statistics.nonparametric.EmpiricalCDF sampling based} cdf estimator.
	 * The parameters needed for construction are passed to the factory by an
	 * <tt>Object[]</tt> representing an <TT>iid</TT> sample.
	 */
	public static Function FACTORY = new AbstractFunction<Object[],EmpiricalCDF>() {
		public EmpiricalCDF invoke(Object[] o) {
			return new EmpiricalCDF(o);
		}
	};

	/** used sample */
	protected Object[] sample;

	/** indicates whether sample, variance or bounds have changed */
	protected boolean hasChanged = true;

	/** confidence with P( | Fn(x) - F(x) | < epsilon ) = 1 - alpha = p */
	protected double alpha;

	/** last computed function value */
	protected double y;

	/** Constructs a new empirical cdf based upon the given sample
	 * and a confidence level.
	 *
	 * @param sample sample used to build the cdf
	 * @param alpha confidence with P( | Fn(x) - F(x) | < epsilon ) = 1 - alpha = p
	 */
	public EmpiricalCDF(Object[] sample, double alpha) {
		this.sample = sample;
		this.alpha = alpha;
	}

	/** Constructs a new empirical cdf based upon the given sample.
	 *
	 * @param sample sample used to build up the cdf
	 */
	public EmpiricalCDF(Object[] sample) {
		this(sample, -1);
	}

	/** Evaluates the empirical cdf at given point x.
	 * 
	 * @param x argument where to evaluate the empirical cdf
	 * @return value of the empirical cdf at point x
	 */
	public double eval(double x) {
		int r = 0;
		if (hasChanged())
			reinit();
		int size = sample.length;
		for (int i = 0; i < size; i++) {
			if (((Double) sample[i]).doubleValue() <= x)
				r++;
		}
		y = (double) r / (double) size;
		return y;
	}

	/** Sets a new sample. If the sample has changed, i.e., if old_sample.equals (new_sample)
	 * returns false, <tt>changed</tt> will be set true.
	 * 
	 * @param newSample new sample to set
	 */
	public void setSample(Object[] newSample) {
		if (!newSample.equals(sample)) {
			hasChanged = true;
			sample = newSample;
		}
	}

	/** Indicates whether something has changed. Thus, a recomputation of the 
	 * bandwidth may be necessary.
	 * 
	 * @return true, if something has changed
	 */
	public boolean hasChanged() {
		return hasChanged;
	}

	/** Reinitilizes the object after changes. */
	private void reinit() {
		hasChanged = false;
	}

	/**
	 * Returns the current confidence interval at the last computed function point with
	 * P( | Fn(x) - F(x) | < epsilon ) = 1 - alpha = p.
	 * <br>
	 * <b>Note:<\b> This functionality is not available yet!
	 *
	 * @throws UnsupportedOperationException if no confidence level alpha has been given and
	 * hence no confidence interval for the given value could be computed or 
	 * a sample of a size smaller than 40 is given
	 * @return value of the current confidence interval given by [ F(x)-epsilon, F(x)+epsilon]
	 */
	public double epsilon() throws UnsupportedOperationException {
		if (alpha < 0)
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		if (sample.length <= 40)
			throw new UnsupportedOperationException("computing of confidence intervals not supported for sample sizes smaller than 40");
		// ---
		throw new UnsupportedOperationException("computing of confidence intervals not yet supported by this estimator");
	}

	/**
	 * Returns the current confidence interval at the given value x with
	 * P( | Fn(x) - F(x) | < epsilon ) = 1 - alpha = p.
	 *
	 * @param x argument where to evaluate the confidence interval
	 * @throws UnsupportedOperationException if no confidence level alpha has been given and
	 * hence no confidence interval for the given value could be computed
	 * or a sample of a smaller size than 40 is given
	 * @return value of the current confidence interval given by [ F(x)-epsilon, F(x)+epsilon]
	 */
	public double epsilon(double x) throws UnsupportedOperationException {
		y = eval(x);
		return epsilon();
	}

	/** Returns the confidence level.
	 * 
	 * @return p = 1-alpha
	 * @throws UnsupportedOperationException if no confidence level has been given and
	 * hence no confidence interval could be computed
	 */
	public double confidence() throws UnsupportedOperationException {
		if (alpha < 0)
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		else
			return 1 - alpha;
	}

	/** Returns the difference F(b) - F(a).
	 * The implementation of this method is not based on the {@link #eval(double) eval-method}.
	 * The difference will be directly evaluated by<br>
	 * <pre>
	 * <code>
		int size = sample.length;
		for ( int i=0; i< size; i++){
			double xi = ((Number) sample[i]).doubleValue();
			if (( xi <= b) && ( xi > a)) r++;
		}
		return (double) r / (double) size;
	 * </code>
	 * </pre>
	 * 
	 * @param left left border of the window query
	 * @param right right border of the window query
	 * @return the difference F(b) - F(a) respectively F(right) - F(left)
	 */
	public double windowQuery(Object left, Object right) {
		double a = ((Number) left).doubleValue();
		double b = ((Number) right).doubleValue();
		int r = 0;
		if (hasChanged())
			reinit();
		int size = sample.length;
		for (int i = 0; i < size; i++) {
			double xi = ((Number) sample[i]).doubleValue();
			if ((xi <= b) && (xi > a))
				r++;
		}
		return (double) r / (double) size;
	}
}
