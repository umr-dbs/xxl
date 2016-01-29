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


/** This class provides the product of a given one-dimensional 
 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
 * for using it as a n-dimensional real-valued kernel function as modelled in
 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND KernelFunctionND}. 
 * For concrete applications
 * the n-dimensional Epanechnikow kernel as well as the n-dimensional Biweight kernel
 * are provided as static classes.
 *
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunctionND
 * @see xxl.core.math.statistics.nonparametric.kernels.AdaBandKernelDensityEstimatorND
 * @see xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
 * @see xxl.core.math.statistics.nonparametric.kernels.GaussianKernel
 */

public class ProductKernelFunction implements KernelFunctionND {

	/** This class provides a n-dimensional product kernel function based upon the one-dimensional
	 * {@link xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel epanechnikow kernel}.
	 */
	public static class Epanechnikow extends ProductKernelFunction {

		/** Constructs a new instance of an epanechnikow product kernel with given dimension.
		 * 
		 * @param dim dimension of the product kernel function based upon the one-dimensional
		 * {@link xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel epanechnikow kernel}
		 */
		public Epanechnikow(int dim) {
			super(new EpanechnikowKernel(), dim);
		}
	}

	/** This class provides a n-dimensional product kernel function based upon the one-dimensional
	 * {@link xxl.core.math.statistics.nonparametric.kernels.BiweightKernel biweight kernel}.
	 */
	public static class Biweight extends ProductKernelFunction {

		/** Constructs a new instance of a biweight product kernel with given dimension.
		 * 
		 * @param dim dimension of the product kernel function based upon the one-dimensional
		 * {@link xxl.core.math.statistics.nonparametric.kernels.BiweightKernel biweight kernel}
		 */
		public Biweight(int dim) {
			super(new BiweightKernel(), dim);
		}
	}

	/** used one-dimensional kernel function to build up a n-dimensional 
	 * product kernel function 
	 * */
	protected KernelFunction kernel;

	/** dimension of the product kernel function */
	protected int dim;

	/** Constructs a new object of this class.
	 * 
	 * @param kernel used one dimensional {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function}
	 * to build up a n-dimensional product kernel function
	 * @param dim dimension of the product kernel function
	 */
	public ProductKernelFunction(KernelFunction kernel, int dim) {
		this.kernel = kernel;
		this.dim = dim;
	}

	/** Evaluates the product kernel function at given real-valued n-dimensional point x and returns
	 * f(x) with f : R^n &rarr; R.
	 * 
	 * @param x real-valued function argument given as <tt>double []</tt>
	 * @return f(x)
	 * @throws IllegalArgumentException if the dimension of the given argument doesn't match
	 * the dimension of the kernel function
	 */
	public double eval(double[] x) throws IllegalArgumentException {
		if (x.length != dim)
			throw new IllegalArgumentException("wrong dimension in argument!");
		double r = 1.0;
		for (int i = 0; i < dim; i++)
			r *= kernel.eval(x[i]);
		return r;
	}
}
