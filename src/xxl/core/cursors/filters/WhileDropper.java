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

import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.FunctionPredicate;
import xxl.core.predicates.Predicate;

/**
 * A while-dropper swallows the elements of an input iteration as long as an
 * unary predicate (or boolean function) is equal to a user defined flag. The
 * remaining elements of the input iteration are returned. The while-dropper
 * drops the elements of an input iteration as long as the result of a user
 * defined predicate applied to the next element of the input iteration equals
 * to the boolean flag <code>asLongAs</code> specified in the constructor. The
 * following part of the implementation of the method <code>hasNext</code>
 * shows this dropping:
 * <code><pre>
 *   while (cursor.hasNext() && asLongAs ^ !predicate.invoke(cursor.peek()))
 *       cursor.next();
 * </pre></code>
 * The remaining elements of the input iteration are returned.
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor. If a boolean function is
 * specified in a constructor it is also internally wrapped to a
 * predicate.</p>
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *     WhileDropper whileDropper = new WhileDropper(
 *         new Enumerator(21),
 *         new AbstractPredicate() {
 *             public boolean invoke(Object object) {
 *                 return ((Integer)object).intValue() &lt; 10;
 *             }
 *         },
 *         true
 *     );
 * 
 *     whileDropper.open();
 * 
 *     while (whileDropper.hasNext())
 *         System.out.print(whileDropper.next() +"; ");
 *     System.out.flush();
 * 
 *     whileDropper.close();
 * </pre></code>
 * This instance of a while-dropper uses an
 * {@link xxl.core.cursors.sources.Enumerator enumerator} as input iteration
 * containing the elements 0, ..., 20. A predicate is defined which is applied
 * to each element of the enumerator and returns <code>true</code> if the
 * integer value of this element is lower than 10. So all elements of the
 * enumerator that are lower than 10 will be dropped, because the user
 * specified flag <code>asLongAs</code> has been set to <code>true</code>. The
 * remaining elements of the enumerator are printed to the output stream. So
 * the output is:
 * <pre>
 *   10; 11; 12; 13; 14; 15; 16; 17; 18; 19; 20;
 * </pre></p>
 * 
 * <p>Please compare this output and functionality with that of a
 * {@link xxl.core.cursors.filters.WhileTaker while-taker}. A while-taker
 * returns the input iteration's elements as long as the user defined predicate
 * evaluated for each of these elements equals to the user specified flag
 * <code>asLongAs</code>. A while-dropper drops the input iteration's elements
 * as long as the user defined predicate invoked on each of these elements
 * equals to the user specified flag <code>asLongAs</code> and returns the
 * remaining elements.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 */
public class WhileDropper<E> extends SecureDecoratorCursor<E> {

	/**
	 * A flag to signal if all elements selected by the given predicate have
	 * already been dropped.
	 */
	protected boolean skipped = false;

	/**
	 * The predicate used to select the input cursor's elements. As long as
	 * this predicate equals to the user specified flag <code>asLongAs</code>
	 * the elements of the input cursor will be dropped.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * A kind of escape sequence for the predicate. The elements of the input
	 * cursor are dropped as long as the predicate's evaluation result is equal
	 * to this flag.
	 */
	protected boolean asLongAs;

	/**
	 * Creates a new while-dropper. Every iterator given to this constructor is
	 * wrapped to a cursor.
	 *
	 * @param iterator the input iterator.
	 * @param predicate the unary predicate determining how long the input
	 *        elements will be dropped.
	 * @param asLongAs the elements of the input cursor are dropped as long as
	 *        the predicate's evaluation result is equal to this flag.
	 */
	public WhileDropper(Iterator<E> iterator, Predicate<? super E> predicate, boolean asLongAs) {
		super(iterator);
		this.predicate = predicate;
		this.asLongAs = asLongAs;
	}

	/**
	 * Creates a new while-dropper. Every iterator given to this constructor is
	 * wrapped to a cursor. The boolean function is internally wrapped to a
	 * predicate.
	 *
	 * @param iterator the input iterator.
	 * @param function the boolean function determining how long the input
	 *        elements will be dropped.
	 * @param asLongAs the elements of the input cursor are dropped as long as
	 *        the predicate's evaluation result is equal to this flag.
	 */
	public WhileDropper(Iterator<E> iterator, Function<? super E, Boolean> function, boolean asLongAs) {
		this(iterator, new FunctionPredicate<E>(function), asLongAs);
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>This operation is implemented idempotent, i.e., consequent calls to
	 * <code>hasNext</code> do not have any effect.</p>
	 * 
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		if (!skipped) {
			while (super.hasNext() && asLongAs ^ !predicate.invoke(peek()))
				next();
			skipped = true;
		}
		return super.hasNext();
	}

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		skipped = false;
	}
}
