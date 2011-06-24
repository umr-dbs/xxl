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

/**
 * This class provides a higher-order predicate (in other words, this predicate
 * decorates two input predicates). The binary predicate is the basis for every
 * predicate implementing a logical binary operator or the combination of two
 * predicates.
 *
 * @param <P> the type of the predicate's parameters.
 */
public abstract class BinaryPredicate<P> extends AbstractPredicate<P> {

	/**
	 * A reference to the first decorated predicate. This reference is used to
	 * perform method calls on this predicate.
	 */
	protected Predicate<? super P> predicate0;

	/**
	 * A reference to the second decorated predicate. This reference is used to
	 * perform method calls on this predicate.
	 */
	protected Predicate<? super P> predicate1;


	/**
	 * Creates a new binary predicate that decorates the specified predicates.
	 *
	 * @param predicate0 the first predicate to be decorated.
	 * @param predicate1 the second predicate to be decorated.
	 */
	public BinaryPredicate(Predicate<? super P> predicate0, Predicate<? super P> predicate1) {
		this.predicate0 = predicate0;
		this.predicate1 = predicate1;
	}
}
