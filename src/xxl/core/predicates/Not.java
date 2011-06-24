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

/**
 * This class provides a logical NOT predicate. The NOT predicate represents
 * the negation of a predicates to a new predicate that returns
 * <code>true</code> if and only if the underlying predicate returns
 * <code>false</code>.
 * 
 * @param <P> the type of the predicate's parameters.
 */
public class Not<P> extends DecoratorPredicate<P> {

	/**
	 * Creates a new NOT predicate that represents the negation of the
	 * specified predicate.
	 *
	 * @param predicate the predicate to be negated.
	 */
	public Not(Predicate<? super P> predicate) {
		super(predicate);
	}

	/**
	 * Returns <code>true</code> if and only if the underlying predicate
	 * returns <code>false</code>, otherwise <code>false</code> is returned.
	 *
	 * @param arguments the arguments to the underlying predicate.
	 * @return <code>true</code> if and only if the underlying predicate
	 *         returns <code>false</code>, <code>false</code> otherwise.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments){
		return !predicate.invoke(arguments);
	}

	/**
	 * Returns <code>true</code> if and only if the underlying predicate
	 * returns <code>false</code>, otherwise <code>false</code> is returned.
	 *
	 * @return <code>true</code> if and only if the underlying predicate
	 *         returns <code>false</code>, <code>false</code> otherwise.
	 */
	@Override
	public boolean invoke() {
		return !predicate.invoke();
	}

	/**
	 * Returns <code>true</code> if and only if the underlying predicate
	 * returns <code>false</code>, otherwise <code>false</code> is returned.
	 *
	 * @param argument the argument to the underlying predicate.
	 * @return <code>true</code> if and only if the underlying predicate
	 *         returns <code>false</code>, <code>false</code> otherwise.
	 */
	@Override
	public boolean invoke(P argument) {
		return !predicate.invoke(argument);
	}

	/**
	 * Returns <code>true</code> if and only if the underlying predicate
	 * returns <code>false</code>, otherwise <code>false</code> is returned.
	 *
	 * @param argument0 the first argument to the underlying predicate.
	 * @param argument1 the second argument to the underlying
	 *        predicate.
	 * @return <code>true</code> if and only if the underlying predicate
	 *         returns <code>false</code>, <code>false</code> otherwise.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return !predicate.invoke(argument0, argument1);
	}
}
