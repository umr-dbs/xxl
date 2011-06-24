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

import java.util.Comparator;

/**
 * This class provides a binary predicate that returns <code>true</code> if the
 * first given argument is less than the second. In other words, when
 * <code>argument0</code> and <code>argument1</code> are the given arguments,
 * the predicate returns <code>true</code> if
 * (<code>argument0&lt;argument1</code>) holds.
 * 
 * @param <P> the type of the predicate's parameters.
 */
public class Less<P> extends ComparatorPredicate<P> {

	/**
	 * Creates a new binary predicate that determines whether the first given
	 * argument is less than the second.
	 *
	 * @param comparator the comparator that should be used for comparing
	 *        objects.
	 */
	public Less(Comparator<? super P> comparator) {
		super(comparator);
	}

	/**
	 * Returns <code>true</code> if the <code>argument0</code> is less than
	 * <code>argument1</code>. In other words, returns
	 * <code>comparator.compare(argument0,argument1)&lt;0</code>.
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return <code>true</code> if the <code>argument0</code> is less than
	 *         <code>argument1</code>, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return comparator.compare(argument0, argument1) < 0;
	}
}
