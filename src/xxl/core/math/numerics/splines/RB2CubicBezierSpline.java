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

package xxl.core.math.numerics.splines;

import xxl.core.util.DoubleArrays;

/**
 * This class implements a <tt>cubic Bezier-Spline</tt> with the second boundary condition.
 * Thus, the abstract class {@link CubicBezierSpline} is implemented. Since there are most 
 * of the variables precomputed, this class only implements the method <code>solveLGS</code>.
 * This method solves a linear equation system whose solution uniquely determines the Bezier-coefficients. 
 * For solving the linear equation system, a modified version of the
 * {@link xxl.core.math.Maths#triDiagonalGaussianLGS(double[],double[],double[],double[]) Gauss algorithm}
 * is used because the system contains a tridiagonal matrix and this permits an efficient computation.
 *
 * @see xxl.core.math.numerics.splines.CubicBezierSpline
 * @see xxl.core.math.numerics.splines.RB1CubicBezierSpline
 * @see xxl.core.math.Maths#triDiagonalGaussianLGS(double[],double[],double[],double[])
 */

public class RB2CubicBezierSpline extends CubicBezierSpline {

	/**
	 * Constructs a cubic Bezier-Spline with the second boundary condition
	 * based upon a grid and the corresponding function
	 * values. Also the values of the second derivative at the borders are given. 
	 * A flag cdfMode indicates whether the spline is in cdf mode or not, 
	 * i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0. 
	 * 
	 * @param grid given grid points
	 * @param fvalues function values at the grid points
	 * @param deviation_0 value of the first derivative of f at the left border of the grid
	 * @param deviation_dim value of the first derivative of f at the right border of the grid
	 * @param cdfMode indicates spline is in cdf mode
	 */
	public RB2CubicBezierSpline(
		double[] grid,
		double[] fvalues,
		double deviation_0,
		double deviation_dim,
		boolean cdfMode) {
		super(grid, fvalues, deviation_0, deviation_dim, cdfMode);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the second boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. Also the values of the second derivative at the borders are given. 
	 * A flag cdfMode indicates whether the spline is in cdf Mode or not, 
	 * i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0. 
	 * 
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 * @param deviation_0 value of the first derivative of f at the left border of the grid
	 * @param deviation_dim value of the first derivative of f at the right border of the grid
	 * @param cdfMode indicates spline is in cdf mode
	 */
	public RB2CubicBezierSpline(
		double a,
		double b,
		int n,
		double[] fvalues,
		double deviation_0,
		double deviation_dim,
		boolean cdfMode) {
		super(DoubleArrays.equiGrid(a, b, n), fvalues, deviation_0, deviation_dim, cdfMode);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the second boundary condition
	 * based upon a grid and the corresponding function
	 * values. Also the values of the second derivative at the borders are given.
	 * Initially, the cdf mode is set to false.
	 * 
	 * @param grid given grid points
	 * @param fvalues function values at the grid points
	 * @param deviation_0 value of the first derivative of f at the left border of the grid
	 * @param deviation_dim value of the first derivative of f at the right border of the grid
	 */
	public RB2CubicBezierSpline(double[] grid, double[] fvalues, double deviation_0, double deviation_dim) {
		super(grid, fvalues, deviation_0, deviation_dim, false);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the second boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. Also the values of the second derivative at the borders are given.
	 * Initially, the cdf mode is set to false. 
	 *  
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 * @param deviation_0 value of the first derivative of f at the left border of the grid
	 * @param deviation_dim value of the first derivative of f at the right border of the grid
	 */
	public RB2CubicBezierSpline(
		double a,
		double b,
		int n,
		double[] fvalues,
		double deviation_0,
		double deviation_dim) {
		super(DoubleArrays.equiGrid(a, b, n), fvalues, deviation_0, deviation_dim, false);
	}

	/**
	 * Solves the linear system of equations for the second boundary condition with a
	 * modified algorithm of Gauss.
	 * 
	 * @return solution of the linear equation system 
	 */
	protected double[] solveLGS() {
		double[] a1 = new double[a.length + 1];
		double[] b1 = new double[b.length + 1];
		for (int i = 0; i < a.length; i++) {
			a1[i] = a[i];
			b1[i + 1] = b[i];
		}
		b1[0] = a1[a1.length - 1] = 1; // 2nd border
		double[] twoTmp = new double[a1.length + 1];
		double[] right = new double[a1.length + 1]; // 2nd border
		for (int i = 0; i < twoTmp.length; i++) {
			twoTmp[i] = 2;
		}
		for (int i = 0; i < rightSide.length; i++) {
			right[i + 1] = rightSide[i];
		}
		right[0] =
			(fvalues[1] - fvalues[0]) * Math.pow(distance[0] * distance[0], -1)
				- deviation[0] * Math.pow(distance[0], -1);
		// 2nd border
		right[right.length - 1] =
			(fvalues[dim - 2] - fvalues[dim - 1]) * Math.pow(distance[dim - 2] * distance[dim - 2], -1)
				+ deviation[1] * Math.pow(distance[dim - 2], -1);
		// 2nd border

		double[] res = xxl.core.math.Maths.triDiagonalGaussianLGS(a1, twoTmp, b1, right);
		return res;
	}
}
