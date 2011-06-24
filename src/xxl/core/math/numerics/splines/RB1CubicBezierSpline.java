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
 * This class implements a <tt>cubic Bezier-Spline</tt> with the first boundary condition.
 * Thus, the abstract class {@link CubicBezierSpline} is implemented. Since there are most 
 * of the variables precomputed, this class only implements the method <code>solveLGS</code>.
 * This method solves a linear equation system whose solution uniquely determines the Bezier-coefficients. 
 * For solving the linear equation system a modified version of the
 * {@link xxl.core.math.Maths#triDiagonalGaussianLGS(double[],double[],double[],double[]) Gauss algorithm}
 * is used because the system contains a tridiagonal matrix and this permits an efficient computation.
 *
 * @see xxl.core.math.numerics.splines.CubicBezierSpline
 * @see xxl.core.math.numerics.splines.RB2CubicBezierSpline
 * @see xxl.core.math.Maths#triDiagonalGaussianLGS(double[],double[],double[],double[])
 */

public class RB1CubicBezierSpline extends CubicBezierSpline {

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon a grid and the corresponding function
	 * values. A flag cdfMode indicates whether the spline is in cdf mode or not, 
	 * i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0. 
	 * 
	 * @param grid grid points, i.e., corresponding x values for the given function values
	 * @param fvalues function values at the grid points
	 * @param cdfMode indicates if the spline is in cdf mode
	 */
	public RB1CubicBezierSpline(double[] grid, double[] fvalues, boolean cdfMode) {
		super(grid, fvalues, cdfMode);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. A flag cdfMode indicates whether the spline is in cdf mode or not, 
	 * i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0. 
	 *  
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 * @param cdfMode indicates spline is in cdf mode
	 */
	public RB1CubicBezierSpline(double a, double b, int n, double[] fvalues, boolean cdfMode) {
		super(DoubleArrays.equiGrid(a, b, n), fvalues, cdfMode);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon a grid and the corresponding function
	 * values. Initially, the cdf mode is set to false.
	 * 
	 * @param grid grid points, i.e., corresponding x values for the given function values
	 * @param fvalues function values at the grid points
	 */
	public RB1CubicBezierSpline(double[] grid, double[] fvalues) {
		super(grid, fvalues, false);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. Initially, the cdf mode is set to false.
	 * 
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 */
	public RB1CubicBezierSpline(double a, double b, int n, double[] fvalues) {
		super(DoubleArrays.equiGrid(a, b, n), fvalues, false);
	}

	/**
	 * Solves the linear system of equations for the first boundary condition with a modified
	 * algorithm of Gauss.
	 * 
	 * @return solution of the linear equation system 
	 */
	protected double[] solveLGS() {
		double[] a1 = new double[a.length - 1];
		double[] b1 = new double[b.length - 1];
		for (int i = 0; i < a1.length; i++) {
			a1[i] = a[i + 1];
			b1[i] = b[i];
		}
		double[] twoTmp = new double[a1.length + 1];
		for (int i = 0; i < twoTmp.length; i++) {
			twoTmp[i] = 2;
		}
		double[] res = xxl.core.math.Maths.triDiagonalGaussianLGS(a1, twoTmp, b1, rightSide);
		double[] result = new double[res.length + 2];
		result[0] = result[result.length - 1] = 0;
		for (int i = 0; i < res.length; i++) {
			result[i + 1] = res[i];
		}
		return result;
	}
}
