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

package xxl.core.predicates;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class provides an abstract implementation of the interface Predicate.
 * Predicates are highly related to
 * {@link xxl.core.functions.Function functions}. Like functions, predicates
 * provide a set of <code>invoke</code> methods that can be used to evaluate
 * the predicate. For providing predicates with and without parameters, this
 * class contains invoke methods with zero, one and two arguments and with a
 * typed list of arguments. The <code>invoke</code> methods call themselves in
 * a recursive manner. That means when using the default implementation the
 * <code>invoke</code> method with zero, one or two arguments calls the
 * <code>invoke</code> method with a typed list containing the arguments
 * and the other way round (see appropriate invoke methods). For this reason,
 * an <code>invoke</code> method with the desired signature has to be overridden
 * in order to declare a predicate as an object of class Predicate.
 * 
 * <p>But in contrast with functions, predicates do not return the result of
 * their evaluation as an object (that must be cast to a <code>Boolean</code>
 * object for getting the correct result), but as a primitive
 * <code>boolean</code> value.</p>
 *
 * <p>A first example shows how to declare a tautology, i.e. a predicate that
 * always returns <code>true</code>:
 * <code><pre>
 *     Predicate&lt;Object&gt; tautology = new Predicate&lt;Object&gt;() {
 *         public boolean invoke(List&lt;? extends Object&gt; arguments) {
 *             return true;
 *         }
 *     };
 * </pre></code>
 * In this example, the <code>invoke</code> method with a list of objects as
 * argument has been overwritten. Thanks to the recursive default
 * implementation of the <code>invoke</code> methods, every <code>invoke</code>
 * method calls the overwritten method and the overwritten method itself breaks
 * the recursion.</p>
 * 
 * <p>A second example shows how to declare a predicate implementing the
 * logical statement '<code>n is even</code>':
 * <code><pre>
 *     Predicate&lt;Integer&gt; even = new Predicate&lt;Integer&gt;() {
 *         public boolean invoke(Integer n) {
 *             return n % 2 == 0;
 *         }
 *     };
 * </pre></code>
 * In this example, the <code>invoke</code> method with one argument has been
 * overwritten. This method guarantees the specification of the parameter
 * <code>n</code> that is needed for the evaluation of the predicate. Thanks to
 * the recursive default implementation it is all the same whether the user
 * calls <code>even.invoke(n)</code> or
 * <code>even.invoke(Arrays.asList(n))</code>. Note that default
 * implementations of this class are still available. Therefore a call to
 * <code>even.invoke()</code> will cause an infinite recursive loop.</p>
 *
 * @param <P> the type of the predicate's parameters.
 */
public abstract class AbstractPredicate<P> implements Predicate<P>, Serializable {

	/**
	 * Returns the result of the predicate as a primitive boolean value. This
	 * method determines <code>arguments.size()</code> and calls the
	 * appropriate <code>invoke</code> method (see below). The other
	 * <code>invoke</code> methods call this method. This means, that the user
	 * either has to override this method or at least one (!) of the other
	 * <code>invoke</code> methods. The following listing shows the exact
	 * implementation of this method:
	 * <code><pre>
	 *     if (arguments == null)
	 *         return invoke((P)null);
	 *     switch (arguments.size()) {
	 *         case 0 :
	 *             return invoke();
	 *         case 1 :
	 *             return invoke(arguments.get(0));
	 *         case 2 :
	 *             return invoke(arguments.get(0), arguments.get(1));
	 *         default :
	 *             throw new IllegalStateException("boolean invoke(List&lt;? extends P&gt; arguments) has to be overridden! The number of arguments was " + arguments.size() + ".");
	 *     }
	 * </pre></code>
	 *
	 * @param arguments the arguments to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 * @throws IllegalStateException if an object array with length&ge;3 is given
	 *         and the corresponding <code>invoke</code> method
	 *         (<code>boolean invoke (List&lt;? extends P&gt;)</code>) has not
	 *         been overridden.
	 */
	public boolean invoke(List<? extends P> arguments) throws IllegalStateException {
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
				throw new IllegalStateException("boolean invoke(List<? extends P> arguments) has to be overridden! The number of arguments was "+arguments.size()+".");
		}
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. This
	 * method calls the <code>invoke</code> method working on lists with an
	 * empty list (see below). This means, that the user has to override this
	 * method to create a predicate without any argument. The following listing
	 * shows the exact implementation of this method:
	 * <code><pre>
	 *     return invoke(new ArrayList&lt;P&gt;(0));
	 * </pre></code>
	 *
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public boolean invoke() {
		return invoke(new ArrayList<P>(0));
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. This
	 * method calls the <code>invoke</code> method working on lists with a list
	 * containing the given argument (see below). This means, that the user has
	 * to override this method to create a predicate with one argument. The
	 * following listing shows the exact implementation of this method:
	 * <code><pre>
	 *     return invoke(Arrays.asList(argument));
	 * </pre></code>
	 *
	 * @param argument the argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public boolean invoke(P argument) {
		return invoke(Arrays.asList(argument));
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. This
	 * method calls the <code>invoke</code> method working on lists with a list
	 * containing the given arguments (see below). This means, that the user
	 * has to override this method to create a predicate with two arguments.
	 * The following listing shows the exact implementation of this method:
	 * <code><pre>
	 *     return invoke(Arrays.asList(argument0, argument1));
	 * </pre></code>
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public boolean invoke(P argument0, P argument1) {
		return invoke(Arrays.asList(argument0, argument1));
	}
}
