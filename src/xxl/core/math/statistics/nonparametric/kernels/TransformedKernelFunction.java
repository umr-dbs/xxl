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

import xxl.core.math.functions.Differentiable;
import xxl.core.math.functions.Integrable;
import xxl.core.math.functions.RealFunction;

/** This class provides a transformation of a one-dimensional kernel function with
 * restricted support to <tt>[-1,1]</tt> like the
 * {@link xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel epanechnikow kernel function} or the 
 * {@link xxl.core.math.statistics.nonparametric.kernels.BiweightKernel biweight kernel function} to an interval
 * given by <tt>[l,l+c]</tt>.
 * The transformed kernel function keeps its enclosed area of size <tt>1</tt> so one could use it
 * to provide a {@link xxl.core.math.statistics.nonparametric.kernels.DifferenceKernelEstimator difference kernel estimator}.
 * The used transformation is:<br>
   <table border="0"><tr>
   <td>
   	K_t (x)_I(x)[l,l+c]
   </td>
   <td>
	= (2/c) * K ( (2/c) * ( x - ( l + (c/2)) )_I(x)[-1,1]
   </td></tr>
   <tr>
   <td>
   </td>
   <td>
	= (2/c) K ( (2/c) (x-l) - 1 )_I(x)[-1,1]
   </td></tr></table>
 * <br> This transformation preserves the area enclosed by the transformed kernel function, i.e.
 * the integral \int_{l}^{l+c} K^t (t) dt == 1 for all l \in R and c >0 , c \in R.
 * 
 * @see xxl.core.math.statistics.nonparametric.kernels.DifferenceKernelEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
 */

public class TransformedKernelFunction extends KernelFunction implements Integrable, Differentiable {

	/** one-dimensional kernel function to transform to the given interval respectively support */
	protected KernelFunction kernelFunction;

	/** left border of the new support respectively the left interval border of the transformed kernel function */
	protected double l;

	/** width of the new support respectively width of the interval of the transformed kernel function */
	protected double c;

	/** Constructs a new transformed kernel function based upon a given kernel function.
	 * 
	 * @param kernelFunction one-dimensional kernel function with appropriate support to transform
	 * @param l left border of the target interval
	 * @param c width of the target interval
	 * @throws IllegalArgumentException if c <= 0
	 */
	public TransformedKernelFunction(KernelFunction kernelFunction, double l, double c)
		throws IllegalArgumentException {
		if (c <= 0)
			throw new IllegalArgumentException("the width of the interval the kernel function is transformed to needs to be greater than 0");
		this.kernelFunction = kernelFunction;
		this.l = l;
		this.c = c;
	}

	/** Evaluates the transformed kernel function at given point x.
	 * The transformation is performed by <br>
	 * K^t (t) = (2 /c) * K (x) with x = (2/c) * (t-l) - 1<br>
	 * or<br>
	 * K^t (t) = (2 /c) * K ( (2/c) * (t-l) - 1)<br> for l <= t <= l-c.<br>
	 * For t < l and t > l+c is K^t (t) == 0.<br>
	 * 
	 * @param x function argument where to evaluate the transformed kernel function
	 * @return function value of the transformed kernel function using the wrapped kernel function
	 */
	public double eval(double x) {
		return (2.0 / c) * kernelFunction.eval((2.0 / c) * (x - l) - 1.0);
		// 2/c * k ( (2/c)(x-l) -1)
	}

	/** Returns the primitive of the transformed kernel function
	 * if and only if the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * for transformation itself implements the interface {@link xxl.core.math.functions.Integrable Integrable}.
	 * Otherwise an {@link java.lang.UnsupportedOperationException UnsupportedOperationException} occurs.<br>
	 * The primitive of the transformed kernel rests upon the primitive of the kernel used for transformation:<br>
	 * int_{l}^{a} K^t (t) dt = int{-1}^{(2/c)*( a - l) +1} K(x) dx
	 * <br> with l <= a <= l+c, a \in R.
	 *
	 * @throws UnsupportedOperationException if the transformed kernel function is not
	 * {@link xxl.core.math.functions.Integrable integrable}
	 * @return primitive as {@link xxl.core.math.functions.RealFunction RealFunction}
	 */
	public RealFunction primitive() throws UnsupportedOperationException {
		final RealFunction f;
	    try {
			f = ((Integrable) kernelFunction).primitive();
		} catch (ClassCastException e) {
			throw new UnsupportedOperationException("The interface Integrable is not implemented by the used kernel function");
		}
		return new RealFunction() {
			public double eval(double a) {
				if ((a < l) || (a > l + c))
					return 0.0;
				double trans = (2.0 / c) * (a - l) - 1.0;
				double inner = f.eval(trans) - f.eval(-1.0);
				return inner;
			}
		};
	}

	/** Returns the first derivative of the transformed kernel function
	 * if and only if the used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * for transformation itself implements the interface {@link xxl.core.math.functions.Differentiable Differentiable}.
	 * Otherwise an {@link java.lang.UnsupportedOperationException UnsupportedOperationException} occurs.<br>
	 * The derivative of the transformed kernel rests upon the derivative of the kernel used for transformation:<br>
	 * K'^t (t) dt = (2 / c)^2 * K' ( (2/c)*(x-l) -1 ) for 
	 * <br> l <= t <= l+c, t \in R 
	 * <br> otherwise K'^t (t) = 0
	 *
	 * @throws UnsupportedOperationException if the transformed kernel function is not
	 * {@link xxl.core.math.functions.Differentiable differentiable}
	 * @return first derivative as {@link xxl.core.math.functions.RealFunction RealFunction}
	 */
	public RealFunction derivative() throws UnsupportedOperationException {
		final RealFunction f;
	    try {
			f = ((Differentiable) kernelFunction).derivative();
		} catch (ClassCastException e) {
			throw new UnsupportedOperationException("The interface Differentiable is not implemented by the used kernel function");
		}
		return new RealFunction() {
			public double eval(double t) {
				double trans = (2.0 / c) * (t - l) - 1.0;
				double inner = f.eval(trans);
				return (2.0 / c) * (2.0 / c) * inner;
			}
		};
	}
}
