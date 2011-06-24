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

import xxl.core.math.functions.AbstractRealFunctionFunction;
import xxl.core.util.DoubleArrays;

/**
 * This class implements a <tt>cubic Bezier-Spline</tt> interpolation with one of the two common
 * boundary conditions for a one-dimensional real-valued function
 * by determining the necessary parameters. 
 * <br>
 * The function, that is defined over <tt>[a,b]</tt>, is 
 * evaluated on a grid [a=x_0,x_1,...,b=x_n-1].
 * Then for each of the grid intervals a polynomial of degree 3 is chosen that interpolates
 * the function in the borders of the interval. These polynomials are parameterized such
 * that the partial polynomials fit together and globally build a smooth curve. That means, the
 * overall function is globally two times continuously differentiable. The <tt>cubic Bezier-Spline</tt> 
 * interpolate is especially qualified for approximating a function since it is in the class of
 * interpolating functions the one with the least fraction.  
 * <br>
 * For determining the unique <tt>cubic Bezier-Spline interpolate</tt>, one of two boundary conditions has to be 
 * chosen. The first one, called natural, postulates the function to fulfill a second derivative of zero in a and b.
 * For the second condition the values of the functions first derivative in a and b has to be known.  
 * Given one of the conditions the determination of the local polynomials respectively their parameters results 
 * in the solution of a linear equation system. This equation system has the special form of a tridiagonal matrix
 * and thus permits an efficient computation of the solution.
 * <br>
 * With the solution of the linear equation system the Bezier-coefficients can be computed. The evaluation of the spline
 * relies on these coefficients. Generally, the spline is evaluated locally by determining the interval where the point to 
 * evaluate is located. For this interval and with the corresponding Bezier coefficients, the spline is evaluated in
 * the Bezier-Bernstein form. 
 * <br>   
 * Since some of the computations do not depend on the boundary conditions, this class is abstract.
 * In the abstract method <code>solveLGS<\code> the boundary conditions are included. Otherwise,
 * this class precomputes all necessary variables.   
 * The boundary conditions in turn are modelled in
 * {@link xxl.core.math.numerics.splines.RB1CubicBezierSpline} and
 * {@link xxl.core.math.numerics.splines.RB2CubicBezierSpline}.
 * <br>
 * The <tt>cubic Bezier-Spline interpolate</tt> is a {@link xxl.core.functions.Function Function} supporting
 * objects of type {@link java.lang.Number Number} within the given grid. In regard to the underlying function
 * this class extends {@link xxl.core.math.functions.AbstractRealFunctionFunction}. 
 * Thus, the spline can also be evaluated with double values.
 * <br>
 * 
 * @see xxl.core.math.numerics.splines.RB1CubicBezierSpline
 * @see xxl.core.math.numerics.splines.RB2CubicBezierSpline
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.functions.Function
 * @see xxl.core.math.functions.AbstractRealFunctionFunction
 */

public abstract class CubicBezierSpline extends AbstractRealFunctionFunction {

	/** If a cumulative distribution function is 
	 * interpolated respectively approximated, the resulting interpolate needs to be one for all values
	 * right "beside" the interval. This flag indicates that the spline is in cdf mode, i.e., evaluating
	 * the spline interpolate at x with x > maximum causes the spline
	 * to return 1.0 instead of 0.0. 
	 */
	protected boolean cdfMode = false;

	/** grid constituting the approximation interval */
	public double[] grid;

	/** grid values of the function to interpolate */
	protected double[] fvalues;

	/**
	 * This array contains values, which are necessary for determining the 
	 * Bezier-coefficients of the spline. They constitute the lower subdiagonal
	 * of the matrix A, which has to be solved to determine the polynomials coefficients.
	 */
	protected double[] a;

	/**
	 * This array contains values, which are necessary for determining the 
	 * Bezier-coefficients of the spline. They constitute the upper subdiagonal
	 * of the matrix A, which has to be solved to determine the polynomials coefficients.
	 */
	protected double[] b;

	/**
	 * This array contains the divided differences of the function values. 
	 * It constitutes the right side of the LGS, whose solution is
	 * necessary for determining the Bezier-coefficients.
	 */
	protected double[] rightSide;

	/**
	 * The solution of the LGS A * mu = rightSide. With those values the 
	 * Bezier-coefficients are determined.
	 */
	protected double[] mu;

	/** The Bezier-coefficients are necessary for invoking the spline. */
	public double[] bezier;

	/** This array contains the distances between the nodes of the grid. */
	protected double[] distance;

	/**
	 * This array contains, if known, the two values of the first 
	 * derivative of f at the borders.
	 */
	protected double[] deviation;

	/** The number of grid nodes. */
	protected int dim;

	/** Indicates whether the spline has been initialized or not. */
	protected boolean init = false;

	/**
	 * Constructs a cubic Bezier-Spline with the second boundary condition
	 * based upon a grid and the corresponding function
	 * values. Also the values of the second derivative at the borders are given. 
	 * The cdf mode has to be set manually.
	 * 
	 * @param grid grid points to the corresponding function values
	 * @param fvalues function values at the grid points
	 * @param deviation_0 value of the first derivative of f at the left border of the grid
	 * @param deviation_dim value of the first derivative of f at the right border of the grid
	 * @param cdfMode indicates if the function is in cdf mode
	 * @throws IllegalArgumentException if the dimensions of the given arrays do not match
	 */
	protected CubicBezierSpline(
		double[] grid,
		double[] fvalues,
		double deviation_0,
		double deviation_dim,
		boolean cdfMode)
		throws IllegalArgumentException {
		if (grid.length != fvalues.length)
			throw new IllegalArgumentException("Number of grid points differs from the number of function values !!");
		deviation = new double[2];
		deviation[0] = deviation_0;
		deviation[1] = deviation_dim;
		this.grid = grid;
		this.fvalues = fvalues;
		this.cdfMode = cdfMode;
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
	 * @param fvalues function values on the grid points
	 * @param deviation_0 the first derivative of f at the left border of the grid
	 * @param deviation_dim the first derivative of f at the right border of the grid
	 */
	protected CubicBezierSpline(
		double a,
		double b,
		int n,
		double[] fvalues,
		double deviation_0,
		double deviation_dim) {
		this(DoubleArrays.equiGrid(a, b, n), fvalues, deviation_0, deviation_dim, false);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon a grid and the corresponding function
	 * values. The cdf mode has to be set manually.
	 * 
	 * @param grid grid points to the corresponding function values
	 * @param fvalues function values at the grid points
	 * @param cdfMode indicates if the function is in cdf mode
	 * @throws IllegalArgumentException if the dimensions of the given arrays doesn't match
	 */
	protected CubicBezierSpline(double[] grid, double[] fvalues, boolean cdfMode) throws IllegalArgumentException {
		if (grid.length != fvalues.length)
			throw new IllegalArgumentException("Number of grid points differs from the number of function values !!");
		this.grid = grid;
		this.fvalues = fvalues;
		this.cdfMode = cdfMode;
	}

	/**
	* Constructs a cubic Bezier-Spline with the first boundary condition
	* based upon a grid and the corresponding function
	* values. The cdf mode is set to false.
	* 
	* @param grid grid points to the corresponding function values
	* @param fvalues function values at the grid points
	*/
	protected CubicBezierSpline(double[] grid, double[] fvalues) {
		this(grid, fvalues, false);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. The cdf mode has to be set manually. 
	 * 
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 * @param cdfMode indicates if the function is in cdf mode
	 */
	protected CubicBezierSpline(double a, double b, int n, double[] fvalues, boolean cdfMode) {
		this(DoubleArrays.equiGrid(a, b, n), fvalues, cdfMode);
	}

	/**
	 * Constructs a cubic Bezier-Spline with the first boundary condition
	 * based upon an equidistant grid and the corresponding function
	 * values. The cdf mode is set to false. 
	 * 
	 * @param a left border of the grid
	 * @param b right border of the grid
	 * @param n number of grid points
	 * @param fvalues function values at the grid points
	 */
	protected CubicBezierSpline(double a, double b, int n, double[] fvalues) {
		this(DoubleArrays.equiGrid(a, b, n), fvalues, false);
	}

	/**
	 * This method solves the linear equation system depending on the chosen boundary condition.
	 * Extending classes need to implement this method to support the different choices
	 * of boundary conditions.
	 * 
	 * @return solution of the linear equation system
	 */
	protected abstract double[] solveLGS();

	/**
	 * This method determines the parameters of the spline that are independent from
	 * the chosen boundary condition.
	 */
	protected void initSettings() {
		dim = grid.length;
		distance = new double[dim - 1];
		for (int i = 0; i < dim - 1; i++)
			distance[i] = grid[i + 1] - grid[i];
		bezier = new double[3 * dim - 2];
		a = new double[dim - 2];
		b = new double[dim - 2];
		rightSide = new double[dim - 2];
		mu = new double[dim];
		for (int j = 0; j < a.length; j++) {
			a[j] = distance[j] / (distance[j] + distance[j + 1]);
			b[j] = distance[j + 1] / (distance[j] + distance[j + 1]);
			rightSide[j] = (fvalues[j + 2] - fvalues[j + 1]) / distance[j + 1];
			rightSide[j] -= (fvalues[j + 1] - fvalues[j]) / distance[j];
			rightSide[j] /= (distance[j] + distance[j + 1]);
		}
	}

	/**
	 * This method determines the Bezier-coefficients based on the solution of the linear equation system.
	 */
	protected void bezierCoeff() {
		for (int i = 0; i < dim - 1; i++) {
			bezier[3 * i] = fvalues[i];
			bezier[3 * i + 1] = (2 * fvalues[i] + fvalues[i + 1]) / 3.0;
			bezier[3 * i + 1] -= distance[i] * distance[i] * (2.0 * mu[i] + mu[i + 1]) / 3.0;
			bezier[3 * i + 2] = (fvalues[i] + 2.0 * fvalues[i + 1]) / 3.0;
			bezier[3 * i + 2] -= distance[i] * distance[i] * (mu[i] + 2.0 * mu[i + 1]) / 3.0;
		}
		bezier[3 * dim - 3] = fvalues[dim - 1];
	}

	/** 
	 * Evaluates the spline at a given point. If the spline is not initialized yet, the 
	 * Bezier-coefficients are computed. For values outside the grid, zero will be returned or,
	 * if the cdf mode is true and x > right border, one will be returned.
	 * 
	 * @param x double value where the spline will be evaluated
	 * @return value of the spline at point x
	 */
	public double eval(double x) {
		// init if not yet used
		if (!init) {
			initSettings();
			mu = solveLGS();
			bezierCoeff();
			init=true;
		}
		// if argument is out of the supported range, return 0.0,
		// but, if spline is in cdf mode (i.e., is approximating a cdf function)
		// and x > maximum, return 1.0;
		if (x < grid[0])
			return 0.0;
		if (x > grid[dim - 1])
			return cdfMode ? 1.0 : 0.0;

		// if argument equals the last grid point, return the corresponding function value given
		if (x == grid[dim - 1])
			return fvalues[dim - 1];

		int j = 0;
		// finding interval respectively the needed polynomial, E ( complexity) = log log n, for equi-grid O(1)
		j = interpolationSearch(grid, x);
		// interval found, compute polynomial with parameters in Bezier/Bernstein-Form
		double value = 0.0;
		for (int i = 0; i < 4; i++) {
			double help = 0;
			help = xxl.core.math.Maths.binomialCoeff2(3, i);
			help *= Math.pow(x - grid[j], i);
			help *= Math.pow(grid[j + 1] - x, 3 - i);
			help *= bezier[3 * j + i];
			//System.out.println("Bezier orig "+i+" "+bezier[3 * j + i]);
			value += help;
		}
		value *= Math.pow(distance[j], -3);
		return value;
	}

	/** Searches the corresponding double array for the specified value using the interpolation search algorithm.
	 * The array must be sorted (as by the sort method, above) prior to making this call.
	 * If it is not sorted, the results are undefined.
	 * If the array contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
	 *
	 * @param a array to be searched
	 * @param x searched value
	 * @return index <TT>i</TT> with <TT>x \in [g[i], g(i+1) )</TT> if <TT>x \in [g[0], g[n-1] ), otherwise -1
	 */
	protected static int interpolationSearch(double[] a, double x) {
		if ((x < a[0]) || (x >= a[a.length - 1]))
			return -1;
		int pos = (int) Math.floor((x - a[0]) / (a[a.length - 1] - a[0]));
		if ((x < a[pos + 1]) & (x >= a[pos]))
			return pos;
		else {
			boolean found = false;
			while (!found) {
				if (x < a[pos]) {
					pos = (int) Math.floor((x - a[0]) / (a[pos] - a[0]));
				} else { // x >= g[pos+1]
					pos += 1 + (int) Math.floor((x - a[pos + 1]) / (a[a.length - 1] - a[pos + 1]));
				}
				if ((x < a[pos + 1]) & (x >= a[pos]))
					found = true;
			}
			return pos;
		}
	}
}
