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

package xxl.core.math.numerics.integration;

import xxl.core.functions.Function;
import xxl.core.math.functions.RealFunction;
import xxl.core.math.functions.RealFunctionArea;
import xxl.core.math.functions.RealFunctionFunction;

/** This class provides the numerical integration of a
 * {@link xxl.core.math.functions.RealFunction real-valued function}
 * that is defined over an interval <tt>[a,b]</tt>. For the integration 
 * the <tt>Trapezoidal Rule</tt> is applied. The <tt>Trapezoidal Rule</tt> belongs to
 * the <tt>Newton-Cotes-Formulas</tt> and bases on two weights, providing
 * an approximation of order 2. By partitioning the interval, the <tt>Trapezoidal Rule</tt>
 * can be applied iteratively. 
 * 
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.functions.RealFunctionArea
 * @see xxl.core.math.numerics.integration.SimpsonsRuleRealFunctionArea
 */

public class TrapezoidalRuleRealFunctionArea extends RealFunctionArea {

	/** 
	 * error bound 
	 */
	protected double epsilon;

	/** Constructs a new Object of this type.
	 *
	 * @param realFunction {@link xxl.core.math.functions.RealFunction real-valued function} 
	 * to integrate numerically
	 * @param epsilon error bound 
	 */
	public TrapezoidalRuleRealFunctionArea(RealFunction realFunction, double epsilon) {
		super(realFunction);
		this.epsilon = epsilon;
	}

	/** Computes the area "under" the given real-valued function 
	 * for the interval <tt>[a,b]</tt>. The Trapezoidal Rule is applied iteratively until 
	 * the defined error threshold is reached.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval	 
	 * @return numerical approximation of the integral
	 * @throws IllegalArgumentException invalid parameters
	 */
	public double eval(double a, double b) throws IllegalArgumentException {
		return TrapezoidalRuleRealFunctionArea.trapezx(a, b, new RealFunctionFunction(realFunction), epsilon);
	}

	/** Computes the area under a real-valued function in a given interval <tt>[a,b]</tt> using
	 * the Trapezoidal Rule for numerical integration iteratively with a given number of 
	 * steps.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval
	 * @param n number of steps used for computation
	 * @param real1DFunction real-valued (1D) function to integrate numerically
	 * @return numerical approximation of the integral
	 */
	public static double trapez(double a, double b, Function real1DFunction, int n) {
		double r = 0.0;
		double h = (b - a) / n; // step-width
		double s = 0.0;
		double x = 0.0;
		for (int i = 1; i <= n - 1; i++) {
			x = a + i * h;
			s += ((Number) real1DFunction.invoke(new Double(x))).doubleValue();
		}
		r =
			((Number) real1DFunction.invoke(new Double(a))).doubleValue()
				+ 2 * s
				+ ((Number) real1DFunction.invoke(new Double(b))).doubleValue();
		r *= (h * 0.5);
		return r;
	}

	/** Computes the area under a real-valued function in a given interval <tt>[a,b]</tt> using
	 * the Trapezoidal Rule for numerical integration iteratively. The error is bounded
	 * through the absolute maximum of the second deviation of the function. The number of
	 * iterations in turn depends on this error estimation.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval
	 * @param real1DFunction real-valued (1D) function to integrate numerically
	 * @param maxF2Dev absolute maximum of the second derivative of the given function max |f''(x)|
	 * @param epsilon error threshold
	 * @return numerical approximation of the integral
	 */
	public static double trapez(double a, double b, Function real1DFunction, double maxF2Dev, double epsilon) {
		double n = Math.pow((b - a), 3.0) / (12.0 * epsilon);
		n *= maxF2Dev;
		n = Math.sqrt(n);
		return trapez(a, b, real1DFunction, (int) Math.ceil(n));
	}

	/** Computes the area under a real-valued function in a given interval <tt>[a,b]</tt> using
	 * the Trapezoidal Rule for numerical integration until an error threshold is reached.
	 * For estimating the error, <tt>|T(n) - T (n/2)| <= epsilon * |T(n)|</tt> is used.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval
	 * @param real1DFunction real-valued (1D) function to integrate numerically
	 * @param epsilon error threshold used as |T(n) - T ( n/2)| <= epsilon * |T(n)|
	 * @return numerical approximation of the integral
	 */
	public static double trapezx(double a, double b, Function real1DFunction, double epsilon) {
		int n = 2;
		double h = (b - a) / n; // step-width
		double sab =
			((Number) real1DFunction.invoke(new Double(a))).doubleValue()
				+ ((Number) real1DFunction.invoke(new Double(b))).doubleValue();
		double w = sab * h / 2.0;
		double w_old = 0.0;
		do {
			n = 2 * n;
			w_old = w;
			w = trapez(a, b, real1DFunction, n);
		} while (Math.abs(w - w_old) > epsilon * Math.abs(w));
		return w;
	}
}
