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
 * This abstract class provides a predicate that uses an underlying comparator
 * for determining whether it is fulfilled or not. The comparator predicate is
 * an abstract class because no <code>invoke</code> method has been overwritten
 * such that the predicate's recursion mechanism will cause an infinite loop
 * for every call.
 *
 * @param <P> the type of the predicate's parameters.
 * @see Comparator
 */
public abstract class ComparatorPredicate<P> extends AbstractPredicate<P> {

	/**
	 * Creates a ComparatorPredicate with the given comparator. If the result
	 * of the Comparator is > 0 the Predicate returns greaterZero, lowerZero
	 * if the Comparator return a value < 0 and zero if the Comparator is 0.
	 * 
	 * @param <P> the type of the predicate's parameters.
	 * @param comp the comparator that is used for comparisons on given
	 *        objects.
	 * @param greaterZero result for comparator values greater 0.
	 * @param zero result for comparator values equal 0.
	 * @param lowerZero result for comparator values lower 0.
	 * @return the comparator-predicate.
	 */
	public static <P> ComparatorPredicate<P> getComparatorPredicate(Comparator<? super P> comp, final boolean greaterZero, final boolean zero, final boolean lowerZero) {
		return new ComparatorPredicate<P>(comp){
			@Override
			public boolean invoke(P argument0, P argument1) {
				return comparator.compare(argument0, argument1) > 0 ?
					greaterZero :
					comparator.compare(argument0, argument1) > 0 ?
						lowerZero :
						zero;
			}
		};
	}
	
	/**
	 * A reference to the wrapped comparator. This reference is used to perform
	 * comparisons on given objects during method calls.
	 */
	protected Comparator<? super P> comparator;

	/**
	 * Creates a new comparator predicate that uses the given comparator for
	 * comparisons on specified objects.
	 *
	 * @param comparator the comparator that is used for comparisons on given
	 *        objects.
	 */
	public ComparatorPredicate(Comparator<? super P> comparator) {
		this.comparator = comparator;
	}

}
