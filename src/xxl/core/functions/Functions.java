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

package xxl.core.functions;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.NullaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.math.functions.AggregationFunction;


/**
 * This class contains some useful static methods for manipulating objects of
 * type {@link xxl.core.functions.Function Function}.
 */
public class Functions {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Functions() {
		// private access in order to ensure non-instantiability
	}

	/**
	 * This method declares a new function <tt>h</tt> by composing a given
	 * function <tt>g</tt> with a function <tt>f</tt>.
	 * 
	 * <p><b>Note</b> that the method does not execute the code of the new
	 * function <tt>h</tt>. The invocation of the composed function <tt>h</tt>
	 * is just triggered by a call of its own <tt>invoke-method</tt>. Then, the
	 * input parameters are passed to each given function <tt>f</tt>,
	 * and the returned objects are used as the input parameters of the
	 * function <tt>g</tt>. The invoke method of <tt>h</tt> returns the final
	 * result.</p>
	 * 
	 * @param <P> the return type of the <it>inner</it> function and the type
	 *        of the <it>outer</it> function's parameters.
	 * @param <R> the return type of the <it>outer</it> function, i.e., the
	 *        return type of the composed function.
	 * @param <T> the type of the <it>inner</it> function's parameters, i.e.,
	 *        the type of the composed function's parameters.
	 * @param g the <it>outer</it> function to be composed.
	 * @param f the <it>inner</it> function to be composed.
	 * @return the result of the composition.
	 */
	@SuppressWarnings("serial")
	public static <P, R, T> Function<T, R> composeMulti(final Function<P, R> g, final Function<? super T, ? extends List<? extends P>> f) {
		return new AbstractFunction<T, R>() {
			@Override
			public R invoke(List<? extends T> objects) {
				return g.invoke(f.invoke(objects));
			}
		};
	}

	/**
	 * This method declares a new function <tt>h</tt> by composing a given
	 * function <tt>g</tt> with a number of functions <tt>f_1,...,f_n</tt>.
	 * 
	 * <p><b>Note</b> that the method does not execute the code of the new
	 * function <tt>h</tt>. The invocation of the composed function <tt>h</tt>
	 * is just triggered by a call of its own <tt>invoke-method</tt>. Then, the
	 * input parameters are passed to each given function <tt>f_1,...,f_n</tt>,
	 * and the returned objects are used as the input parameters of the new
	 * function <tt>g</tt>. The invoke method of <tt>h</tt> returns the final
	 * result.</p>
	 * 
	 * @param <P> the return type of the <it>inner</it> functions and the type
	 *        of the <it>outer</it> function's parameters.
	 * @param <R> the return type of the <it>outer</it> function, i.e., the
	 *        return type of the composed function.
	 * @param <T> the type of the <it>inner</it> functions' parameters, i.e.,
	 *        the type of the composed function's parameters.
	 * @param g the <it>outer</it> function to be composed.
	 * @param f the <it>inner</it> functions to be composed.
	 * @return the result of the composition.
	 */
	public static <P, R, T> Function<T, R> compose(Function<P, R> g, Function<? super T, ? extends P>... f) {
		return composeMulti(g, new DecoratorArrayFunction<T, P>(f));
	}

	/**
	 * Returns the first function value (function takes m arguments).
	 * 
	 * @param <P> the type of the given function's parameters.
	 * @param <R> the return type of the given function.
	 * @param iterators iterators holding the arguments to the function. The
	 *        number of iterators should correspond to the number of arguments
	 *        of the function.
	 * @param function the function to invoke.
	 * @throws NoSuchElementException if the given
	 *         {@link java.util.Iterator iterators} do not deliver the
	 *         necessary number of objects.
	 * @return the first function value.
	 */
	public static <P, R> R returnFirst(Function<? super P, ? extends R> function, Iterator<? extends P>... iterators) throws NoSuchElementException {
		return returnNth(0, function, iterators);
	}

	//////////////////////////////////////////////////////////////////////////////////

	/**
	 * Returns the last function value (function takes m arguments).
	 * 
	 * @param <P> the type of the given function's parameters.
	 * @param <R> the return type of the given function.
	 * @param iterators iterators holding the arguments to the function. The
	 *        number of iterators should correspond to the number of arguments
	 *        of the function.
	 * @param function the function to invoke.
	 * @throws NoSuchElementException if the given
	 *         {@link java.util.Iterator iterators} do not deliver the
	 *         necessary number of objects.
	 * @return the last function value.
	 */
	public static <P, R> R returnLast(Function<? super P, ? extends R> function, Iterator<? extends P>... iterators) throws NoSuchElementException {
		return Cursors.last(new Mapper<P, R>(function, iterators));
	}

	//////////////////////////////////////////////////////////////////////////////////

	/**
	 * Returns the n-th function value (function takes m arguments).
	 * 
	 * @param <P> the type of the given function's parameters.
	 * @param <R> the return type of the given function.
	 * @param n the number of the returned value.
	 * @param iterators iterators holding the arguments to the function. The
	 *        number of iterators should correspond to the number of arguments
	 *        of the function.
	 * @param function the function to invoke.
	 * @throws NoSuchElementException if the given
	 *         {@link java.util.Iterator iterators} do not deliver the
	 *         necessary number of objects.
	 * @return the n-th function value.
	 */
	public static <P, R> R returnNth(int n, Function<? super P, ? extends R> function, Iterator<? extends P>... iterators) throws NoSuchElementException {
		return Cursors.nth(new Mapper<P, R>(function, iterators), n);
	}

	//////////////////////////////////////////////////////////////////////////////////

	/**
	 * Wraps an aggregation function to a unary function by storing the status
	 * of the aggregation internally.
	 * 
	 * @param <P> the type of the function's parameters.
	 * @param <R> the return type of the function.
	 * @param aggregateFunction aggregation function to provide as an unary function
	 * @return an unary function wrapping an aggregation function
	 */
	@SuppressWarnings("serial")
	public static <P, R> Function<P, R> aggregateUnaryFunction(final AggregationFunction<P, R> aggregateFunction) {
		return new AbstractFunction<P, R>() {
			R agg = null;
			
			@Override
			public R invoke(P o) {
				agg = aggregateFunction.invoke(agg, o);
				return agg;
			}
		};
	}

	//////////////////////////////////////////////////////////////////////////////////

	/**
	 * Returns an one-dimensional real-valued function providing an absolute of
	 * a {@link java.lang.Number number}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing an
	 *         absolute of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> abs() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number number){
				return number.doubleValue() < 0 ? -number.doubleValue() : number.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing a negative of
	 * a {@link java.lang.Number number}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         negation of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> minus() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number number){
				return -number.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing a product of
	 * two {@link java.lang.Number numbers}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         multiplication of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> mult() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number multiplicand, Number multiplicator){
				return multiplicand.doubleValue() * multiplicator.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing a quotient of
	 * two {@link java.lang.Number numbers}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         division of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> div() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number dividend, Number divisor){
				return dividend.doubleValue() / divisor.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing a sum of two
	 * {@link java.lang.Number numbers}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         summation of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> add() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number augend, Number addend){
				return augend.doubleValue() + addend.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing a difference
	 * of two {@link java.lang.Number numbers}. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         subtraction of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> sub() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number minuend, Number subtrahend){
				return minuend.doubleValue() - subtrahend.doubleValue();
			}
		};
	}

	/**
	 * Returns an one-dimensional real-valued function providing an
	 * exponentiation of two {@link java.lang.Number numbers}. The result of
	 * the mathematical operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing an
	 *         exponentiation of numerical data.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> exp() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number base, Number exponent){
				return Math.pow(base.doubleValue(), exponent.doubleValue());
			}
		};
	}
	
	/**
	 * Returns an one-dimensional real-valued function providing the
	 * trigonometric sine of an angle. The result of the mathematical operation
	 * will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} providing the
	 *         trigonometric sine of an angle.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> sin() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number x) {
				return Math.sin(x.doubleValue());
			}
		};
	}
	
	/**
	 * Returns an one-dimensional real-valued function providing the
	 * trigonometric cosine of an angle. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} providing the
	 *         trigonometric cosine of an angle.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> cos() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number x) {
				return Math.cos(x.doubleValue());
			}
		};
	}
	
	/**
	 * Returns an one-dimensional real-valued function providing the
	 * trigonometric tangent of an angle. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} providing the
	 *         trigonometric tangent of an angle.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> tan() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(Number x) {
				return Math.tan(x.doubleValue());
			}
		};
	}
	
	/**
	 * Returns an one-dimensional real-valued function providing the
	 * trigonometric cosine of an angle. The result of the mathematical
	 * operation will be returned by a double number!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} providing the
	 *         trigonometric cosine of an angle.
	 */
	@SuppressWarnings("serial")
	public static Function<Number, Double> sum() {
		return new AbstractFunction<Number, Double>() {
			@Override
			public Double invoke(List<? extends Number> summands) {
				double sum = 0;
				for (Number summand : summands)
					sum += summand.doubleValue();
				return sum;
			}
		};
	}

	/**
	 * Returns a function providing a concatenation of two
	 * {@link java.lang.Object obejct's} string representations. The result of
	 * the operation will be returned by a string!
	 * 
	 * @return a {@link xxl.core.functions.Function Function} performing a
	 *         concatenation of two object's string representation.
	 */
	@SuppressWarnings("serial")
	public static Function<Object, String> concat() {
		return new AbstractFunction<Object, String>() {
			@Override
			public String invoke(Object firstObject, Object secondObject){
				return firstObject.toString() + secondObject.toString();
			}
		};
	}

	/**
	 * Returns the hash-value of the given object wrapped to an Integer
	 * instance. Note, this implementation delivers an unary function. Do not
	 * apply to none, two or more parameters.
	 * 
	 * @return the hash-value of the given object.
	 */
	@SuppressWarnings("serial")
	public static Function<Object, Integer> hash() {
		return new AbstractFunction<Object, Integer>() {
			@Override
			public Integer invoke(Object o) {
				return o.hashCode();
			}
		};
	}
	
	/**
	 * This method returns a function which is the identity function with the
	 * side effect of sending the object to a PrintStream.
	 * 
	 * @param <T> the type of the objects the returned function is able to
	 *        process.
	 * @param ps PrintStream to which the object is sent.
	 * @return the desired function.
	 */
	public static <T> Function<T, T> printlnMapFunction(final PrintStream ps) {
		return new Print<T>(ps, true);
	}

	/**
	 * This method returns a function which is the identity function with the
	 * side effect of sending the object to a PrintStream.
	 * 
	 * @param <P> the type of the given function's parameters.
	 * @param <R> the return type of the given function.
	 * @param f Function to be decorated.
	 * @param ps PrintStream to which the object is sent.
	 * @param showArgs showing the arguments which are sent to the Function?
	 *        (yes/no).
	 * @param beforeArgs String which is printed at first (before writing the
	 *        arguments).
	 * @param argDelimiter String which delimits the arguments from each other.
	 * @param beforeResultDelimiter String which is places between the last
	 *        argument (if this is printed) and the rest.
	 * @param afterResultDelimiter String which is printed after the result (at
	 *        the end).
	 * @return the desired function
	 */
	@SuppressWarnings("serial")
	public static <P, R> Function<P, R> printlnDecoratorFunction(final Function<? super P, ? extends R> f, final PrintStream ps, final boolean showArgs, final String beforeArgs, final String argDelimiter, final String beforeResultDelimiter, final String afterResultDelimiter) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(List<? extends P> o) {
				ps.print(beforeArgs);
				if (showArgs) {
					for (int i = 0; i < o.size()-1; i++) {
						ps.print(o.get(i));
						ps.print(argDelimiter);
					}
					ps.print(o.get(o.size()-1));
				}
				ps.print(beforeResultDelimiter);

				R ret = f.invoke(o);
				
				ps.print(ret);
				ps.print(afterResultDelimiter);
				return ret;
			}
		};
	}

	/**
	 * This method returns a function which is the identity function with an
	 * additional test. If the objects of subsequent invoke-calls do not adhere
	 * to the given comparator, a runtime exception is sent.
	 * 
	 * @param <T> the type of the objects the returned function is able to
	 *        process.
	 * @param c the comparator.
	 * @param ascending true iff the order is ascending, else false.
	 * @return the desired function.
	 */
	@SuppressWarnings("serial")
	public static <T> Function<T, T> comparatorTestMapFunction(final Comparator<? super T> c, final boolean ascending) {
		return new AbstractFunction<T, T>() {
			boolean first = true;
			T lastObject;
			int value = ascending?+1:-1;
			
			@Override
			public T invoke(T o) {
				if (first)
					first = false;
				else if (c.compare(lastObject,o)*value>0)
					throw new RuntimeException("Ordering is not correct");
				lastObject = o;
				return o;
			}
		};
	}
	
	/**
	 * Makes a given function able to handle null values. The defaultValue is
	 * the return value of the function if one of the parameters is a null
	 * value. If no null value is handed over the given function is called.
	 *
	 * @param <P> the type of the functions's parameters.
	 * @param <R> the return type of the given function.
	 * @param function the given function.
	 * @param defaultValue the default value.
	 * @return the function able to handle null values.
	 */
	@SuppressWarnings("serial")
	public static <P, R> Function<P, R> newNullSensitiveFunction(final Function<? super P, ? extends R> function, final R defaultValue) {
		return new AbstractFunction<P, R>() {
			@Override
			public R invoke(List<? extends P> arguments) {
				for (P argument : arguments)
					if (argument == null)
						return defaultValue;
				return function.invoke(arguments);
			}
		};
	}
	
	/**
	 * Returns a function the selects to <code>index</code>th argument of its
	 * input parameters and returns it.
	 * 
	 * @param <T> the type of the function's parameters and result.
	 * @param index the index of the argument to be returned.
	 * @return a function the selects to <code>index</code>th argument of its
	 *         input parameters and returns it.
	 */
	@SuppressWarnings("serial")
	public static <T> Function<T, T> select(final int index) {
		return new AbstractFunction<T, T>() {
			@Override
			public T invoke(List<? extends T> arguments) {
				if (arguments.size() <= index)
					throw new IllegalArgumentException("not enough arguments");
				return arguments.get(index);
			}
		};
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	// compatibility methods
	///////////////////////////////////////////////////////////////////////////////////
	/**
	 * Wrap to function object
	 * @param nullaryFunction
	 * @return
	 */
	public static <T> Function<Object,T> toFunction(final NullaryFunction<T> nullaryFunction){
		return new AbstractFunction<Object, T>() {
			public T invoke() {
				return nullaryFunction.invoke();
			};
		};
	}  
	/**
	 * Wraps old function object
	 * @param unaryFunction
	 * @return
	 */
	public static <I,O> Function<I,O> toFunction(final UnaryFunction<I, O> unaryFunction){
		return new AbstractFunction<I, O>() {
			public O invoke(I argument) {
				return unaryFunction.invoke(argument);
			};
		};
	}  
	/**
	 * 
	 * @param binaryFunction
	 * @return
	 */
	public static <I0, I1, O> Function<Object,O> toFunction(final BinaryFunction<I0, I1, O> binaryFunction){
		return new AbstractFunction<Object, O>() {
			@Override
			public O invoke(Object argument0, Object argument1) {
				return binaryFunction.invoke((I0)argument0, (I1)argument1);
			}
		};
	}
	
	
	
}
