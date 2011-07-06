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

import java.util.List;

import xxl.core.predicates.Predicate;

/**
 * A (recursive) functional for-loop.
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 */
public class For<P, R> extends DecoratorFunction<P, R> {

	/** 
	 * This class provides a Function that expects an Integer-Object as its
	 * input and returns the increment. This means, if
	 * <code>new Integer(n)</code> is the input-argument
	 * <code>new Integer(n+1)</code> is returned.
	 */
	@SuppressWarnings("serial")
	public static class IntegerIncrement extends AbstractFunction<Integer, Integer> {
	
		/** 
		 * This instance can be used for getting a default instance of
		 * IntegerIncrement. It is similar to the <i>Singleton Design
		 * Pattern</i> (for further details see Creational Patterns, Prototype
		 * in <i>Design Patterns: Elements of Reusable Object-Oriented
		 * Software</i> by Erich Gamma, Richard Helm, Ralph Johnson, and John
		 * Vlissides) except that there are no mechanisms to avoid the
		 * creation of other instances of IntegerIncrement.
		 */
		public static final IntegerIncrement DEFAULT_INSTANCE = new IntegerIncrement();
	
		/** 
		 * Returns the increment of the given Integer-Object.
		 * 
		 * @param argument Integer-Object to increment.
		 * @return a new Integer-Object representing the increment of the given
		 *         argument.
		 */
		@Override
		public Integer invoke(Integer argument) {
			return argument + 1;
		}
	}

	/** 
	 * Constructs a new Object of this class.
	 * 
	 * @param <T> the return type of the function <code>f1</code> and the
	 *        parameter type of the function <code>newState</code>.
	 * @param predicate the Predicate that determines whether f1 is called
	 *        recursively.
	 * @param f1 code to be executed if predicate returns true, general
	 *        contract: has to return arguments for newState.
	 * @param f2 code to be executed if predicate returns false.
	 * @param newState computes new arguments for consequent call of f1.
	 */
	@SuppressWarnings("serial")
	public <T> For(Predicate<? super P> predicate, final Function<? super P, ? extends T> f1, final Function<? super P, ? extends R> f2, final Function<? super T, ? extends P> newState) {
		super();												//should be "super(new Iff..." (early bind of compiler prob)
		function = new Iff<P, R>(
			predicate,
			new AbstractFunction<P, R>() {
				@Override
				public R invoke(List<? extends P> arguments) {	//Function to be called if predicate returned true
					return function.invoke(						//recurse
						newState.invoke( 						//compute newState, i.e. increment, use return value as argument for iff-Function
							f1.invoke(arguments) 				//execute actual code and use return value as argument vor newState
						)
					);
				}
			},
			f2													//Function to be called if predicate returned false
		);
	}
}
