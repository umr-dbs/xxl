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

package xxl.core.cursors.filters;

import java.util.Iterator;

import xxl.core.functions.Function;
import xxl.core.predicates.FunctionPredicate;
import xxl.core.predicates.Predicate;

/**
 * A while-remover removes the elements of an input cursor with the help of a
 * user defined predicate (or boolean function). It removes the elements as
 * long as the result of the user defined, unary predicate applied to the next
 * element of the input cursor is <code>true</code>. An instance of a
 * while-remover is created as follows:
 * <code><pre>
 *   super(new WhileTaker&lt;E&gt;(iterator, predicate));
 * </pre></code>
 * So simply a new {@link xxl.core.cursors.filters.WhileTaker while-taker} is
 * created that selects the elements with the help of the given predicate and
 * this instance of a while-taker is used as the input cursor of a
 * {@link xxl.core.cursors.filters.Remover remover}.
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor. If a boolean function is
 * specified in a constructor it is also internally wrapped to a predicate.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.filters.Remover
 * @see xxl.core.cursors.filters.WhileTaker
 */
public class WhileRemover<E> extends Remover<E> {

	/**
	 * Creates a new while-remover backed on an iteration and an unary
	 * predicate applied on the iteration's elements. The iteration's elements
	 * will be removed from the underlying data structure as long as the
	 * specified predicate returns <code>true</code>.
	 * 
	 * <p>The implementation of this constructor is as follows:
	 * <code><pre>
	 *   super(new WhileTaker&lt;E&gt;(iterator, predicate));
	 * </pre></code>
	 * So a {@link xxl.core.cursors.filters.WhileTaker while-taker} is created
	 * that selects the elements with the help of the given predicate, and this
	 * instance of a while-taker is used as the input for a
	 * {@link xxl.core.cursors.filters.Remover remover}.
	 *
	 * @param iterator the input iteration the elements will be removed from as
	 *        long as the specified predicate applied on the current element
	 *        returns <code>true</code>.
	 * @param predicate the unary predicate used to select the elements of the
	 *        input iteration.
	 */
	public WhileRemover(Iterator<E> iterator, Predicate<? super E> predicate) {
		super(new WhileTaker<E>(iterator, predicate));
	}
	
	/**
	 * Creates a new while-remover backed on an iteration and an unary boolean
	 * function applied on the iteration's elements. The iteration's elements
	 * will be removed from the underlying data structure as long as the
	 * specified function returns {@link java.lang.Boolean#TRUE}.
	 * 
	 * @param iterator the input iteration the elements will be removed from as
	 *        long as the specified predicate applied on the current element
	 *        returns <code>true</code>.
	 * @param function the unary boolean function used to select the elements
	 *        of the input iteration.
	 */
	public WhileRemover(Iterator<E> iterator, Function<? super E, Boolean> function) {
		this(iterator, new FunctionPredicate<E>(function));
	}
}
