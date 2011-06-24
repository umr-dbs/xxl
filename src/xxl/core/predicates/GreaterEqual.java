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
 * first given argument is greater than or equal to the second. In other words,
 * when <code>argument0</code> and <code>argument1</code> are the given
 * arguments, the predicate returns <code>true</code> if
 * (<code>argument0&ge;argument1</code>) holds.
 *
 * @param <P> the type of the predicate's parameters.
 */
public class GreaterEqual<P> extends Or<P> {

	/**
	 * Creates a new binary predicate that determines whether the first given
	 * argument is greater than or equal to the second.
	 *
	 * @param comparator the comparator that should be used for comparing
	 *        objects.
	 */
	public GreaterEqual(Comparator<? super P> comparator) {
		super(new Greater<P>(comparator), new ComparatorBasedEqual<P>(comparator));
	}
}
