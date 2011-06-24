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

import java.util.List;

import xxl.core.util.Decorator;

/**
 * The class provides a decorator for a predicate that follows the <i>Decorator
 * Design Pattern</i> (for further details see Structural Patterns, Decorator
 * in <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides). It provides a
 * more flexible way to add functionality to a predicate or one of its
 * subclasses as by inheritance.
 * 
 * <p>To provide this functionality the class contains a reference to the
 * predicate to be decorated. This reference is used to redirect method call to
 * the underlying predicate. This class is an abstract class although it
 * provides no abstract methods. That's so because it does not make any sense
 * to instantiate this class which redirects every method call to the
 * corresponding method of the decorated predicate without adding any
 * functionality.</p>
 *
 * <p>Usage example (1).
 * <code><pre>
 *     // create a new decorated predicate that adds functionality to the invoke method without
 *     // arguments and leaves the other methods untouched
 *
 *     DecoratorPredicate predicate = new DecoratorPredicate(Predicate.TRUE) {
 *         public boolean invoke () {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the invocation of the underlying predicate!");
 *             super.invoke();
 *             System.out.println("After the invocation of the underlying predicate!");
 *         }
 *     }
 * </pre></code>
 * 
 * @param <P> the type of the predicate's parameters.
 */
public abstract class DecoratorPredicate<P> implements Predicate<P>, Decorator<Predicate<? super P>> {

	/**
	 * A reference to the predicate to be decorated. This reference is used to
	 * perform method calls on the underlying predicate.
	 */
	protected Predicate<? super P> predicate;

	/**
	 * Constructs a new DecoratorPredicate that decorates the specified
	 * predicate.
	 *
	 * @param predicate the predicate to be decorated.
	 */
	public DecoratorPredicate(Predicate<? super P> predicate) {
		this.predicate = predicate;
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying predicate with the given arguments.</p>
	 *
	 * @param arguments the arguments to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		return predicate.invoke(arguments);
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying predicate.</p>
	 *
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke() {
		return predicate.invoke();
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying predicate with the given argument.</p>
	 *
	 * @param argument the argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument) {
		return predicate.invoke(argument);
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>This implementation simply calls the <code>invoke</code> method of
	 * the underlying predicate with the given arguments.</p>
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return predicate.invoke(argument0, argument1);
	}
	
	/**
	 * Returns the decorated predicate.
	 * 
	 * @return the decorated predicate.
	 */
	@Override
	public Predicate<? super P> getDecoree() {
		return predicate;
	}
}
