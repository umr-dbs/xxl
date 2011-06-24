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
 * This class provides a logical AND predicate. The AND predicate represents
 * the conjunction of two predicates to a new predicate that returns
 * <code>true</code> if and only if both underlying predicates return
 * <code>true</code>.
 *
 * @param <P> the type of the predicate's parameters.
 */
public class And<P> extends BinaryPredicate<P> {

	/**
	 * Creates a new AND predicate that represents the conjunction of the
	 * specified predicates.
	 *
	 * @param predicate0 the first predicate of the conjunction.
	 * @param predicate1 the second predicate of the conjunction.
	 */
	public And(Predicate<? super P> predicate0, Predicate<? super P> predicate1) {
		super(predicate0, predicate1);
	}

	/**
	 * Returns <code>true</code> if and only if both underlying predicates
	 * return <code>true</code>, otherwise <code>false</code> is returned.
	 *
	 * @param arguments the arguments to the underlying predicates.
	 * @return <code>true</code> if and only if both underlying predicates
	 *         return <code>true</code>, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		return predicate0.invoke(arguments) && predicate1.invoke(arguments);
	}

	/**
	 * Returns <code>true</code> if and only if both underlying predicates
	 * return <code>true</code>, otherwise <code>false</code> is returned.
	 *
	 * @return <code>true</code> if and only if both underlying predicates
	 *         return <code>true</code>, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke() {
		return predicate0.invoke() && predicate1.invoke();
	}

	/**
	 * Returns <code>true</code> if and only if both underlying predicates
	 * return <code>true</code>, otherwise <code>false</code> is returned.
	 *
	 * @param argument the argument to the underlying predicates.
	 * @return <code>true</code> if and only if both underlying predicates
	 *         return <code>true</code>, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke(P argument) {
		return predicate0.invoke(argument) && predicate1.invoke(argument);
	}

	/**
	 * Returns <code>true</code> if and only if both underlying predicates
	 * return <code>true</code>, otherwise <code>false</code> is returned.
	 *
	 * @param argument0 the first arguments to the underlying
	 *        predicates.
	 * @param argument1 the second arguments to the underlying
	 *        predicates.
	 * @return <code>true</code> if and only if both underlying predicates
	 *         return <code>true</code>, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return predicate0.invoke(argument0, argument1) && predicate1.invoke(argument0, argument1);
	}
}
