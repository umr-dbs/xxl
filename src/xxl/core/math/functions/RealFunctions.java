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

package xxl.core.math.functions;

/** This class provides some static methods useful for working with RealFunctions.
 * Additionally, this class provides some RealFunctions used for testing and
 * evaluating algorithms of XXL, especially for those provided by the package xxl.core.math.
 *
 * @see RealFunction
 */

public class RealFunctions {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private RealFunctions() {}

	/** Returns the identity function defined by f:R &rarr; R respectively,
	 * f:double &rarr; double.
	 *
	 * @see RealFunction
	 */
	public static final RealFunction Identity = new RealFunction() {
		public double eval(double x) {
			return x;
		}
	};

	/** Returns a {@link RealFunction} providing f(x) = x^4 + I[0.3,1](x).
	 * This function is used for simulating studies among others in <BR>
	 * [Qiu94]: P. Qiu, Estimation of the number of jumps of the jump regression functions,
	 * 1994 <BR> and
	 * [WC92]: J.S. Wu and C.K.Chu, Kernel type estimators of jump points and values of a regression function, 1992 <BR>
	 *
	 * @return a {@link xxl.core.math.functions.RealFunction} providing f(x) = x^4 + I[0.3,1](x).
	 */
	public static RealFunction pdfCP00() {
		return new RealFunction() {
			public double eval(double x) {
				return Math.pow(x, 4.0) + xxl.core.math.Maths.characteristicalFunction(x, 0.3, 1.0);
			}
		};
	}

	/** 
	 * Returns a {@link RealFunction} modeling the distribution function G(x) = x^2 * I(x)[0,1]. 
	 * 
	 * @return a {@link RealFunction} modeling the distribution function G(x) = x^2 * I(x)[0,1]
	 */
	public static RealFunction distCont01() {
		return new RealFunction() {
			public double eval(double y) {
				return (y <= 0) ? 0.0 : ((y > 1) ? 1.0 : y * y);
			}
		};
	}

	/**
	 * * Returns a function g(x) that is required for generating samples of
	 * a random variable with {@link #pdfCP01() the CP01 pdf}.
	 * For f(x) defined as {@link #pdfCP01()} one is able to use this pdf
	 * for generating pseudo random numbers with
	 * {@link xxl.core.util.random.InversionDistributionBasedPRNG} and
	 * {@link xxl.core.util.random.RejectionDistributionBasedPRNG}. We refer to the documentation
	 * of this classes for a detailed description how to generate random numbers
	 * following a user defined distribution.
	 *  
	 * Returns a {@link RealFunction} modeling the probability density function to G(x): g(x) = 2x * I(x)[0,1].
	 * 
	 * @return a {@link RealFunction} modeling the probability density function to G(x): g(x) = 2x * I(x)[0,1]
	 */
	public static RealFunction pdfCont01() {
		return new RealFunction() {
			public double eval(double x) {
				return ((x >= 0) & (x <= 1.0)) ? 2 * x : 0.0;
			}
		};
	}

	/** 
	 * * Returns a function g(x) that represents the inverse of the CP01 distribution
	 * with G^{-1}(y) = \sqrt(y) * I(y)[0,1].
	 * This permits in combination with the inversion principle the generation of pseudo random numbers
	 * of CP01 with
	 * {@link xxl.core.util.random.InversionDistributionBasedPRNG} and
	 * {@link xxl.core.util.random.RejectionDistributionBasedPRNG}. Refer to the documentation
	 * of this classes for a detailed description how to generate random numbers
	 * following a user defined distribution.
	 * 
	 * @return a {@link RealFunction} modeling the inversal function of G:  G^{-1}(y) = \sqrt(y) * I(y)[0,1]
	 */
	public static RealFunction invDistCont01() {
		return new RealFunction() {
			public double eval(double y) {
				return (y <= 0) ? 0.0 : ((y > 1) ? 1.0 : Math.sqrt(y));
			}
		};
	}

	/** Returns a pdf f:R &rarr; R defined by<BR>
	 * f(x) =  f1(x) * ( 0 <= x <= 1/3) + f2(x) * ( 1/3 < x <= 1)<BR>with<BR>
	 * f1(x) = 1 + 27x^2 - 54x^3<BR>
	 * and<BR>
	 * f2(x) = (169/8) - 135x + (2619 x^2)/8 - (693x^3)/2 + 135x^4<BR>.
	 *
	 * @return pdf
	 */
	public static RealFunction pdfCP01() {
		return new RealFunction() {
			public double eval(double x) {
				return ((x >= 0.0) & (x <= 1.0))
					? (1.0 + 27.0 * x * x - 54.0 * x * x * x)
						* xxl.core.math.Maths.characteristicalFunction(x, 0.0, 1.0 / 3.0)
						+ (169.0 / 8.0)
						- 135.0 * x
						+ 2619.0 * x * x / 8.0
						- 693.0 * x * x * x / 2.0
						+ 135.0 * Math.pow(x, 4.0) * xxl.core.math.Maths.characteristicalFunction(x, 1.0 / 3.0, 1.0)
					: 0.0;
			}
		};
	}

	/** Returns a pdf f:R &rarr; R defined by<BR>
	 * f(x) =  f1(x) * ( 0 <= x <= 0.3) + f2(x) * ( 0.3 < x < 0.6 + f3(x) * ( 0.6 <= x <= 1))<BR>with<BR>
	 * Thereby, f1, f2, f3 are local polynomials of degree 3 such that the overall function constitutes a pdf and
	 * contains two jumps. For the exact definition of the function we refer to the code.
	 *
	 * @return pdf
	 */
	public static RealFunction pdfCP02() {
		final double u = 0.5;
		final double v = 0.7;
		final RealFunction f1 = new RealFunction() {
			double l = 0.0;
			double r = 0.3;
			public double eval(double x) {
				if ((x >= l) & (x <= r))
					return 2.0 * u
						+ (10.0 / 3.0) * (8.0 - 23.0 * u) * x
						+ (50.0 / 9.0) * (93.0 * u - 32.0) * x * x
						+ (8000.0 / 27.0) * (1.0 - 3.0 * u) * x * x * x;
				else
					return 0.0;
			}
		};
		final RealFunction f2 = new RealFunction() {
			double l = 0.3;
			double r = 0.6;
			public double eval(double x) {
				if ((x > l) & (x < r))
					return
						- (302320.0 * u - 115403.0 * v - 15680.0) / 4039915.0
						+ (1568.0 * (2.0 * (87453.0 * v - 20.0) - 411465.0 * u) / 2423949.0) * x
						+ (200.0 * (68640765.0 * u - 2.0 * (13980993.0 * v - 980.0)) / 7271847.0) * x * x
						+ (400.0 * (2.0 * (44684347.0 * v - 980.0) - 222157535.0 * u) / 21815541.0) * x * x * x
						+ (5000.0 * (5.0 * u - 2.0 * v) / 9.0) * x * x * x * x;
				else
					return 0.0;
			}
		};
		final RealFunction f3 = new RealFunction() {
			double l = 0.6;
			double r = 1.0;
			public double eval(double x) {
				if ((x >= l) & (x <= r))
					return 27.0
						- 52.0 * v
						+ (225.0 * (2.0 * v - 1.0) / 2.0) * x
						+ (150.0 * (1.0 - 2.0 * v)) * x * x
						+ (125.0 * (2.0 * v - 1.0) / 2.0) * x * x * x;
				else
					return 0.0;
			}
		};
		return new RealFunction() {
			public double eval(double x) {
				return ((0.0 <= x) & (x <= 1.0)) ? f1.eval(x) + f2.eval(x) + f3.eval(x) : 0.0;
			}
		};
	}

	/**
	 * Returns a function g(x) that is required for generating samples of
	 * a random variable with {@link #pdfCP02() the CP02 pdf}.
	 * For f(x) defined as {@link #pdfCP02()} one is able to use this pdf
	 * for generating pseudo random numbers with
	 * {@link xxl.core.util.random.InversionDistributionBasedPRNG} and
	 * {@link xxl.core.util.random.RejectionDistributionBasedPRNG}. We refer to the documentation
	 * of this classes for a detailed description how to generate random numbers
	 * following a user defined distribution.
	 *
	 * @return a pdf
	 */
	public static RealFunction pdfCont02() {
		return new RealFunction() {
			public double eval(double x) {
				return ((x >= 0.0) & (x <= 1.0)) ? 0.5 + x : 0.0;
			}
		};
	}

	/**
	 * Returns a function g(x) that represents the inverse of the CP02 distribution.
	 * This permits in combination with the inversion principle the generation of pseudo random numbers
	 * of CP02 with
	 * {@link xxl.core.util.random.InversionDistributionBasedPRNG} and
	 * {@link xxl.core.util.random.RejectionDistributionBasedPRNG}. Refer to the documentation
	 * of this classes for a detailed description how to generate random numbers
	 * following a user defined distribution.
	 * 
	 * @return inverse of the CP02 distribution
	 */
	public static RealFunction invDistCont02() {
		return new RealFunction() {
			public double eval(double x) {
				return ((x >= 0.0) & (x <= 1.0)) ? 0.5 * Math.sqrt(8.0 * x + 1) - 0.5 : 0.0;

			}
		};
	}
}
