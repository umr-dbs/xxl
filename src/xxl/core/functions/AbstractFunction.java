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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class provides an abstract implementation of the interface Function.
 * In order to declare a function as an object of class AbstractFunction, the
 * invoke method has to be overridden. This method accepts both separate input
 * parameters and a typed list holding them. The output of invoke is an object
 * of the given return type. Simplified versions of invoke exists suitable for
 * functions with none, one or two input parameters. It is important to mention
 * that in general only one of the invoke methods has to be overridden. The
 * invocation of a function is then triggered by simply calling invoke.
 *
 * <p>In combination with the powerful mechanism of anonymous classes, it is
 * also possible to build higher-order functions which is known from functional
 * programming (e.&nbsp;g.&nbsp;Haskel). The method
 * {@link Functions#compose(Function, Function...) compose} shows how to
 * declare a function <tt>h</tt> which consists of
 * <tt>g o (f<sub>1</sub>,...,f<sub>n</sub>)</tt> where <tt>g</tt> is a
 * function with <tt>n</tt> arguments and
 * <tt>f<sub>1</sub>,...,f<sub>n</sub></tt> are functions with an equal number
 * of arguments. Note that compose provides the declaration of the function and
 * does not execute the function.</p>
 *
 * <p>Consider for example (see also the main-method) that you are interested
 * in building the function <tt>tan</tt> which can be build by composing
 * <tt>division</tt> and <tt>(sin, cos)</tt>. Function
 * <tt>div</tt>, <tt>sin</tt>, <tt>cos</tt>;
 * <code><pre>
 *   // Initialization of your functional objects
 *   ...
 *   // Declaration of a new function
 *   Function&lt;Double, Double&gt; tan = Functions.compose(div, sin, cos);
 *   ...
 *   // Execution of the new function
 *   tan.invoke(x);
 * </pre></code>
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 * @deprecated  replaced by {@link Functional}
 */
@SuppressWarnings("serial")
@Deprecated
public abstract class AbstractFunction<P, R> implements Function<P, R>, Serializable {

	/**
	 * Returns the result of the function as an object of the return type. This
	 * method determines <code>arguments.size()</code> and calls the
	 * appropriate invoke method (see below). The other invoke methods call
	 * this method. This means, that the user either has to override this
	 * method or one (!) of the other invoke methods. If <code>null</code> is
	 * given as argument <code>invoke((P)null)</code> will be returned meaning
	 * <code>invoke(P)</code> is needed to be overridden.</p>
	 * 
	 * <p>Implementation:
	 * <code><pre>
	 *   if (arguments == null)
	 *       return invoke((P)null);
	 *   switch (arguments.size()) {
	 *       case 0:
	 *           return invoke();
	 *       case 1:
	 *           return invoke(arguments.get(0));
	 *       case 2:
	 *           return invoke(arguments.get(0), arguments.get(1));
	 *       default:
	 *           throw new IllegalStateException("R invoke(List&lt;? super P&gt;) has to be overridden! The number of arguments was " + arguments.size() + ".");
	 *   }
	 * </pre></code></p>
	 * 
	 * @param arguments a list of the arguments to the function.
	 * @throws IllegalStateException if a list of arguments is given that contains 3
	 *         or more arguments and the corresponding method
	 *         <code>R invoke(List&lt;? extends P&gt;)</code> has not been
	 *         overridden.
	 * @return the function value is returned.
	 */
	public R invoke(List<? extends P> arguments) throws IllegalStateException {
		if (arguments == null)
			return invoke((P)null);
		switch (arguments.size()) {
			case 0:
				return invoke();
			case 1:
				return invoke(arguments.get(0));
			case 2:
				return invoke(arguments.get(0), arguments.get(1));
			default:
				throw new IllegalStateException("R invoke(List<? super P>) has to be overridden! The number of arguments was " + arguments.size() + ".");
		}
	}

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * <p>Implementation:
	 * <code><pre>
	 *   return invoke(new ArrayList&lt;P&gt;(0));
	 * </pre></code></p>
	 * 
	 * @return the function value is returned.
	 */
	public R invoke() {
		return invoke(new ArrayList<P>(0));
	}

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * <p>Implementation:
	 * <code><pre>
	 *   return invoke(Arrays.asList(argument));
	 * </pre></code></p>
	 * 
	 * @param argument the argument to the function.
	 * @return the function value is returned.
	 */
	public R invoke(P argument) {
		return invoke(Arrays.asList(argument));
	}

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * <p>Implementation:
	 * <code><pre>
	 *   return invoke(Arrays.asList(argument0, argument1));
	 * </pre></code></p>
	 * 
	 * @param argument0 the first argument to the function
	 * @param argument1 the second argument to the function
	 * @return the function value is returned
	 */
	public R invoke(P argument0, P argument1) {
		return invoke(Arrays.asList(argument0, argument1));
	}
}
