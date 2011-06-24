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
import xxl.core.math.Maths;
import xxl.core.math.functions.RealFunction;
import xxl.core.math.functions.RealFunctionArea;
import xxl.core.math.functions.RealFunctionFunction;

/** This class provides the numerical integration of a
 * {@link xxl.core.math.functions.RealFunction real-valued function}
 * that is defined over an interval <tt>[a,b]</tt>. For the integration 
 * the <tt>Simpson's Rule</tt> is applied. The <tt>Simpson's Rule</tt> belongs to
 * the <tt>Newton-Cotes-Formulas</tt> and bases on three weights, providing
 * an approximation of order 4. By partitioning the interval, the <tt>Simpson's Rule</tt>
 * can be applied iteratively. Thereby, <tt>n</tt>, the number of intervals,
 * has to be even.
 *
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.functions.RealFunctionArea
 * @see xxl.core.math.numerics.integration.TrapezoidalRuleRealFunctionArea
 */

public class SimpsonsRuleRealFunctionArea extends RealFunctionArea {

	/** 
	 * error bound 
	 */
	protected double epsilon;

	/** Constructs a new Object of this type.
	 *
	 * @param realFunction {@link xxl.core.math.functions.RealFunction real-valued function} to integrate
	 * @param epsilon error bound 
	 */
	public SimpsonsRuleRealFunctionArea(RealFunction realFunction, double epsilon) {
		super(realFunction);
		this.epsilon = epsilon;
	}

	/** Computes the area "under" the given real-valued function 
	 * for the interval <tt>[a,b]</tt>.
	 * 
	 * @param a left interval border of the area to compute
	 * @param b right interval border of the area to compute	 
	 * @return numerical approximation of the integral
	 * @throws IllegalArgumentException invalid arguments
	 */
	public double eval(double a, double b) throws IllegalArgumentException {
		return SimpsonsRuleRealFunctionArea.simpsonx(a, b, new RealFunctionFunction(realFunction), epsilon);
	}

	/** Computes the area under a real-valued function for a given interval <tt>[a,b]</tt> using
	 * the Simpson's rule for numerical integration iteratively. Thereby, the number of steps
	 * has to be even.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval
	 * @param n number of steps used for computation
	 * @param real1DFunction one-dimensional real-valued function to integrate numerically
	 * @throws IllegalArgumentException if n is not even
	 * @return numerical approximation of the integral
	 */
	public static double simpson(double a, double b, Function real1DFunction, int n) throws IllegalArgumentException {
		if (!Maths.isEven(n))
			throw new IllegalArgumentException("number of computations (n) needs to be even!");
		double h = (b - a) / n; // step-width
		double sg = 0.0; // even index of the function values
		double su = 0.0; // uneven index of the function values
		double x = 0.0;
		for (int i = 1; i <= n - 1; i++) {
			x = a + i * h;
			if (Maths.isEven(i)) {
				sg += ((Number) real1DFunction.invoke(new Double(x))).doubleValue();
			} else {
				su += ((Number) real1DFunction.invoke(new Double(x))).doubleValue();
			}
		}
		return (
			((Number) real1DFunction.invoke(new Double(a))).doubleValue()
				+ 4 * su
				+ 2 * sg
				+ ((Number) real1DFunction.invoke(new Double(b))).doubleValue())
			* h
			/ 3.0;
	}

	/** Computes the area under a real-valued function for a given interval <tt>[a,b]</tt> using
	 * the Simpson's rule for numerical integration iteratively until an error threshold is reached.
	 * For estimating the error, <tt>|S(n) - S (n/2)| <= epsilon * |S(n)|</tt> is used.
	 * 
	 * @param a left border of the integration interval
	 * @param b right border of the integration interval
	 * @param epsilon error threshold used as |S(n) - S (n/2)| <= epsilon * |S(n)|
	 * @param real1DFunction real-valued (1D) function to integrate numerical
	 * @return numerical approximation of the integral for the given error bound
	 */
	public static double simpsonx(double a, double b, Function real1DFunction, double epsilon) {
		int n = 2;
		double sab =
			((Number) real1DFunction.invoke(new Double(a))).doubleValue()
				+ ((Number) real1DFunction.invoke(new Double(b))).doubleValue();
		double sm = ((Number) real1DFunction.invoke(new Double(a + (b - a) * .5))).doubleValue();
		double s = (sab + 4 * sm) * (b - a) / 6.0;
		double s_old = 0.0;
		do {
			n = 2 * n;
			s_old = s;
			s = simpson(a, b, real1DFunction, n);
		} while (Math.abs(s - s_old) > epsilon * Math.abs(s));
		return s;
	}
}
