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

package xxl.core.math.statistics.parametric.aggregates;

import xxl.core.math.Statistics;
import xxl.core.math.functions.AggregationFunction;

/** In the context of online aggregation aggregates are iteratively computed. The data is successively
 * consumed and the current estimator respectively aggregate bases on the already processed elements. In order to provide
 * a quality statement for the current estimate, confidence intervals can be computed.
 * Let Y_n be for instance the current estimate, alpha the confidence level and epsilon the current
 * confidence. Then the final result (i.e. the estimate based on all data) is in [Y_n-epsilon,Y_n+epsilon] 
 * with probability p=1-alpha.   
 * <br>
 * This class provides a framework for aggregation functions providing a confidence interval for the
 * current aggregate.
 * Furthermore, this class contains a collection of factories for concrete aggregation functions. 
 * <br>
 * A more detailed coverage of the theory and the resulting formulas is given in <BR>
 * [Haa97]: Peter J. Haas: Large-Sample and Deterministic Confidence Intervals for Online Aggregation,
 * Ninth International Conference on Scientific and Statistical Database Management, Proceedings, 1997. 
 * <BR>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public abstract class ConfidenceAggregationFunction extends AggregationFunction<Number,Number> {

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the variance estimator. The bounds of the confidence interval base on the central limit
	 * theorem (CLT). See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulVarianceEstimator
	 * @see xxl.core.math.statistics.parametric.aggregates.FourthCentralMomentEstimator
	 */
	public static ConfidenceAggregationFunction largeSampleConfidenceVarianceEstimator(final double alpha) {

		/** internal variance estimator */
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** internal estimator of the fourth central moment */
		final FourthCentralMomentEstimator mom = new FourthCentralMomentEstimator();

		/** p = 1- alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the variance */
			Number v = null;

			/** internally stored value of the fourth central moment */
			Number m = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				m = mom.invoke(m, next);
				return v;
			}

			/** Computes and returns as a <tt>Double</tt> the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double r =
						(m.doubleValue() - Math.pow(v.doubleValue(), 2.0)) / var.n;
					double z = Statistics.normalQuantil((p + 1.0) * 0.5);
					r = Math.sqrt(r);
					r = r * z;
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulVarianceEstimator (sample variance) with confidence support (large sample confidence/CLT-based)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the standard deviation estimator. The bounds of the confidence interval base on the central limit
	 * theorem. See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulVarianceEstimator
	 * @see xxl.core.math.statistics.parametric.aggregates.FourthCentralMomentEstimator
	 */
	public static ConfidenceAggregationFunction largeSampleConfidenceStandardDeviationEstimator(final double alpha) {

		/** internal variance estimator */
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** internal estimator of the fourth central moment */
		final FourthCentralMomentEstimator mom = new FourthCentralMomentEstimator();

		/** p = 1- alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the std-dev */
			Number v = null;

			/** internally stored value of the forth central moment */
			Number m = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				m = mom.invoke(m, next);
				return new Double(Math.sqrt(v.doubleValue()));
			}

			/** Computes and returns as a <tt>Double</tt> the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double r =
						(m.doubleValue() - Math.pow(v.doubleValue(), 2.0))
							/ (4.0 * var.n);
					r = r / Math.sqrt(v.doubleValue());
					double z = Statistics.normalQuantil((p + 1.0) * 0.5);
					r = Math.sqrt(r);
					r = r * z;
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "Std-Dev.-Estimator (sample std-dev) with confidence support (large sample confidence/CLT-based)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the average estimator. The bounds of the confidence interval base on the central limit
	 * theorem. See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulVarianceEstimator
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
	 */
	public static ConfidenceAggregationFunction largeSampleConfidenceAverage(final double alpha) {

		/** internal variance estimator */
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** internal average estimator */
		final StatefulAverage avg = new StatefulAverage();

		/** p=1-alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the variance */
			Number v = null;

			/** internally stored value of the average */
			Number a = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(v, next);
				a = avg.invoke(old, next);
				return a;
			}

			/** Computes and returns as a <tt>Double</tt> the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (avg.count < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ avg.count	+ ")!");
				else {
					double r = Math.sqrt(v.doubleValue() / avg.count);
					r = r * Statistics.normalQuantil((p + 1.0) * 0.5);
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulAverage (Estimator) with confidence support (large sample confidence/CLT-based)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the standard deviation estimator. The bounds of the confidence interval are deterministic and 
	 * base on Hoeffding's Inequality. See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulVarianceEstimator
	 * @see xxl.core.math.statistics.parametric.aggregates.FourthCentralMomentEstimator
	 */
	public static ConfidenceAggregationFunction conservativeConfidenceAverage(
		final double alpha,
		final double a,
		final double b) {

		/** internal variance estimator */
		final StatefulAverage avg = new StatefulAverage();

		/** p=1-alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the average */
			Number av = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				av = avg.invoke(old, next);
				return av;
			}

			/** Computes and returns as a <tt>Double</tt> the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (avg.count < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ avg.count	+ ")!");
				else {
					double r = Math.log(2.0 / alpha);
					r = r / (2.0 * avg.count);
					r = Math.sqrt(r);
					r = r * (b - a);
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulAverage (Estimator) with confidence support (conservative confidence/based upon Hoeffding's inequality)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the standard deviation estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param N number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
	 */
	public static ConfidenceAggregationFunction deterministicConfidenceAverage(
		final double a,
		final double b,
		final long N) {

		/** internal average estimator */
		final StatefulAverage avg = new StatefulAverage();

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the average */
			Number av = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				av = avg.invoke(old, next);
				return av;
			}

			/** Computes and returns the current confidence interval. */
			public Object epsilon() throws IllegalStateException {
				if (avg.count < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ avg.count	+ ")!");
				else {
					double[] r = new double[] {
							(N - avg.count),
							(N - avg.count)
						};
					r[0] *= (av.doubleValue() - a) / N;
					r[1] *= (av.doubleValue() - b) / N;
					return new Double[] { new Double(r[0]), new Double(r[1])};
				}
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulAverage (Estimator) with confidence support (deterministic confidence)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the sum estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param N number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
	 */
	public static ConfidenceAggregationFunction deterministicConfidenceSumEstimator(
		final double a,
		final double b,
		final long N) {

		/** internal sum estimator */
		final SumEstimator se = new SumEstimator(N);

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the sum */
			Number sev = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				sev = se.invoke(old, next);
				return sev;
			}

			/** Computes and returns the current confidence interval. */
			public Object epsilon() throws IllegalStateException {
				if (se.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ se.n + ")!");
				else {
					double[] r = new double[] {
							(N - se.n) / a,
							(N - se.n) / b 
						};
					return new Double[] { new Double(r[0]), new Double(r[1])};
				}
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "Estimated sum with confidence support (deterministic confidence)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the variance estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param N number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
	 */
	public static ConfidenceAggregationFunction deterministicConfidenceVarianceEstimator(
		final double a,
		final double b,
		final long N) {

		/** internal variance estimator */
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** internal average estimator */
		final StatefulAverage avg = new StatefulAverage();

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the variance */
			Number v = null;

			/** internally stored value of the variance */
			Number av = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				av = avg.invoke(av, next);
				return v;
			}

			/** Computes and returns the current confidence interval. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double e_down =
						((double) (N - var.n) / (double) (N - 1))
							* v.doubleValue();
					double e_up = ConfidenceAggregationFunction.lambda(N,var.n,a,b,av.doubleValue()) - e_down;
					return new Double[] { new Double(e_down), new Double(e_up)};
				}
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulVarianceEstimator (sample variance) with confidence support (deterministic confidence)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the standard deviation estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param N number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 *
	 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
	 */
	public static ConfidenceAggregationFunction deterministicConfidencestandardDeviationEstimator(
		final double a,
		final double b,
		final long N) {

		/** internal variance estimator */
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** internal average estimator */
		final StatefulAverage avg = new StatefulAverage();

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the variance */
			Number v = null;

			/** internally stored value of the average */
			Number av = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				av = avg.invoke(av, next);
				return new Double(Math.sqrt(v.doubleValue()));
			}

			/** Computes and returns the current confidence interval. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double q =
						((double) (var.n - 1) / (double) (N - 1))
							* v.doubleValue();
					double e_down = Math.sqrt(v.doubleValue()) - Math.sqrt(q);
					double e_up =
						Math.sqrt(q	- ConfidenceAggregationFunction.lambda(	N, var.n, a, b,	av.doubleValue()))
							- Math.sqrt(v.doubleValue());
					return new Double[] { new Double(e_down), new Double(e_up)};
				}
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "Standard Deviation Estimator (sample std-dev) with confidence support (deterministic confidence)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the variance estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param sN number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 */
	public static ConfidenceAggregationFunction conservativeConfidenceVarianceEstimator(
		final double alpha,
		final double a,
		final double b,
		final long sN) {

		/** internal variance estimator*/
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** p=1-alpha*/
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value of the variance */
			Number v = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				return v;
			}

			/** Computes and returns the current confidence interval. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double r =
						sN
							/ (sN - 1.0)
							* Math.pow(ConfidenceAggregationFunction.Bn(var.n, a, b, p), 0.5);
					r += (sN / Math.pow((sN - 1.0), 2.0))
						* (Math.pow(b - a, 2.0) / 4.0);
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "StatefulVarianceEstimator (sample variance) with confidence support (conservative confidence/based upon Hoeffding's inequality)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the standard deviation estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @param a minimum of the data domain
	 * @param b maximum of the data domain
	 * @param sN number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 */
	public static ConfidenceAggregationFunction conservativeConfidenceStandardDeviationEstimator(
		final double alpha,
		final double a,
		final double b,
		final long sN) {

		/** internal variance estimator*/
		final StatefulVarianceEstimator var = new StatefulVarianceEstimator();

		/** p=1-alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value for the variance */
			Number v = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				v = var.invoke(old, next);
				return new Double(Math.sqrt(v.doubleValue()));
			}

			/** Computes and returns the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (var.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ var.n	+ ")!");
				else {
					double r =
						sN
							/ (sN - 1.0)
							* Math.pow(
								ConfidenceAggregationFunction.Bn( var.n, a, b, p), 0.25);
					r += (sN / Math.pow((sN - 1.0), 2.0))
						* (Math.pow(b - a, 2.0) / 4.0);
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "Std-Dev.-Estimator (sample std-dev) with confidence support (conservative confidence/based upon Hoeffding's inequality)";
			}
		};
	}

	/** Provides a factory that computes confidence intervals for the current estimate of
	 * the sum estimator. The bounds of the confidence interval are deterministic. 
	 * See [Haa97] for further details.
	 * 
	 * @param alpha confidence level
	 * @param N number of elements of the entirety
	 * @return a function that computes epsilon for the current estimate
	 */
	public static ConfidenceAggregationFunction largeSampleConfidenceSumEstimator(
		final double alpha,
		final long N) {

		/** internal sum estimator*/
		final SumEstimator se = new SumEstimator(N);

		/** internal variance estimator */
		final StatefulVarianceEstimator svar = new StatefulVarianceEstimator();

		/** p=1-alpha */
		final double p = 1 - alpha;

		return new ConfidenceAggregationFunction() {

			/** internally stored value for the sum */
			Number s = null;

			/** internally stored value for the variance */
			Number sv = null;

			/** Computes iteratively the current estimate. */
			public Number invoke(Number old, Number next) {
				s = se.invoke(s, next);
				sv = svar.invoke(
						sv,
						next == null ?
							next :
							new Double(N * next.doubleValue())
				);
				return s;
			}

			/** Computes and returns the current epsilon. */
			public Object epsilon() throws IllegalStateException {
				if (svar.n < 1)
					throw new IllegalStateException(
						"Computation a confidence interval not possible yet (n="
						+ svar.n + ")!");
				else {
					double r = Math.sqrt(sv.doubleValue() / svar.n);
					r = r * Statistics.normalQuantil((p + 1.0) * 0.5);
					return new Double(r);
				}
			}

			/** Returns 1-alpha. */
			public double confidence() {
				return p;
			}

			/** Returns a description of the estimator. */
			public String toString() {
				return "SumEstimator with confidence support (large sample confidence/CLT-based)";
			}
		};
	}

	/** Computes lambda_n = ( 1- n/N) * ( (b-a)^4/4 + (n/N) * max( avg_n-a, b-avg_n)^2).
	 * 
	 * @param N long value
	 * @param n long value
	 * @param a double value
	 * @param b double value
	 * @param avg double value
	 * @return lambda_n
	 */
	protected static double lambda(
		long N,
		long n,
		double a,
		double b,
		double avg) {
		double q = ((double) n / (double) N);
		double r = q * (Math.pow(Math.max(avg - a, b - avg), 2.0));
		r += Math.pow(b - a, 4.0) / 4.0;
		r *= 1.0 - q;
		return r;
	}

	/** Computes a temporal variable.
	 *  
	 * @param n long value
	 * @param a double value
	 * @param b double value
	 * @param p double value
	 * @return Bn
	 */
	protected static double Bn(long n, double a, double b, double p) {
		double r = 	1.0	/ Math.floor(n / 2.0)
				* Math.log(2.0 / (1.0 - p));
		double r2 = Math.pow(b - a, 4.0) / 8.0;
		if (a * b < 0.0) {
			r2 =
				Math.max( r2, 0.5 * Math.max(Math.abs(a), b) * (Math.abs(a) + b) 
				- 0.25 * Math.pow(Math.max(Math.abs(a), b), 2.0));
		}
		return r * r2;
	}

	/**
	 * If an inheriting class doesn't overwrite this method and thus provides the computation of confidence intervals,
	 * this operation is not supported.
	 * 
	 * @return current epsilon
	 * @throws IllegalStateException no valid state
	 * @throws UnsupportedOperationException if computation of confidence intervals not supported
	 */
	public Object epsilon()
		throws IllegalStateException, UnsupportedOperationException {
		throw new UnsupportedOperationException("Computation of confidence intervals not supported by this estimator!");
	}

	/**
	 * If an inheriting class doesn't overwrite this method and thus provides the computation of confidence intervals,
	 * this operation is not supported.
	 * 
	 * @return p=1-alpha
	 * @throws IllegalStateException no valid state
	 * @throws UnsupportedOperationException if computation of confidence intervals is not supported
	 */
	public double confidence() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Computation confidence intervals not supported by this estimator!");

	}
}
