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

import xxl.core.cursors.Cursors;
import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.FunctionPredicate;
import xxl.core.predicates.Predicate;

/**
 * A filter is a
 * {@link xxl.core.cursors.SecureDecoratorCursor decorator-cursor} that selects
 * the iteration's elements with the help of a user defined predicate (or
 * boolean function for backward compatibility). Only those elements, for which
 * the specified unary function applied on that element returns
 * {@link java.lang.Boolean#TRUE}, are returned by <code>next</code> or
 * <code>peek</code>. If a predicate is used instead, the elements of the
 * decorated input cursor are selected by evaluating this predicate for each
 * element. So only the elements are returned, where the predicate's evaluation
 * result is <code>true</code>.
 *
 * <p>The implementation of the <code>next</code> method is as follows:
 * <code><pre>
 *     if (computedHasNext)
 *         return hasNext;
 *     for (; super.hasNext(); super.next())
 *         if (predicate.invoke(super.peek()))
 *             return true;
 *     return false;
 * </pre></code></p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage :</b>
 * <code><pre>
 *     Filter&lt;Integer&gt; filter = new Filter&lt;Integer&gt;(
 *         new Enumerator(11),
 *         new Function&lt;Integer, Boolean&gt;() {
 *             public Boolean invoke(Integer next) {
 *                 return next % 2 == 0;
 *             }
 *         }
 *     );
 * 
 *     filter.open();
 * 
 *     while(filter.hasNext())
 *         System.out.println(filter.next());
 * 
 *     filter.close();
 * </pre></code>
 * This example demonstrates the filter functionality. This filter returns only
 * the even elements of the input enumerator, because the defined unary
 * function only returns Boolean.TRUE, if the integer value of the object
 * modulo 2 is equal to 0.</p>
 * 
 * <p><b>Example usage when defining a predicate instead:</b>
 * <code><pre>
 *     Filter&lt;Integer&gt; filter = new Filter&lt;Integer&gt;(
 *         new Enumerator(11),
 *         new Predicate&lt;Integer&gt;() {
 *             public boolean invoke(Integer next) {
 *                 return next % 2 == 0;
 *             }
 *         }
 *     );
 * 
 *     filter.open();
 * 
 *     while(filter.hasNext())
 *         System.out.println(filter.next());
 * 
 *     filter.close();
 * </pre></code>
 * This example demonstrates the filter functionality implemented by using an
 * unary predicate. The result is exactly the same as in the first example!
 * </p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 * @see xxl.core.functions.Function
 * @see xxl.core.predicates.Predicate
 */
public class Filter<E> extends SecureDecoratorCursor<E> {

	/**
	 * The unary predicate used to evaluate an object.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * Creates a new filter by decorating the input iterator. The input
	 * iterator is wrapped to a
	 * {@link xxl.core.cursors.wrappers.IteratorCursor cursor}, if the
	 * <code>peek</code> functionality is not supported.
	 *
	 * @param iterator the input iterator containing the elements to be
	 *        filtered.
	 * @param predicate the unary predicate used to select the elements.
	 */
	public Filter(Iterator<E> iterator, Predicate<? super E> predicate) {
		super(Cursors.wrap(iterator));
		this.predicate = predicate;
	}

	/**
	 * Creates a new filter by decorating the input iterator. The input
	 * iterator is wrapped to a
	 * {@link xxl.core.cursors.wrappers.IteratorCursor cursor}, if the
	 * <code>peek</code> functionality is not supported. The boolean function
	 * is internally wrapped to a predicate.
	 *
	 * @param iterator the input iterator containing the elements to be
	 *        filtered.
	 * @param function the boolean, unary function used to select the elements.
	 */
	public Filter(Iterator<E> iterator, Function<? super E, Boolean> function) {
		this(iterator, new FunctionPredicate<E>(function));
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>The implementation is as follows:
	 * <code><pre>
	 *     if (computedHasNext)
	 *         return hasNext;
	 *     for (; super.hasNext(); super.next())
	 *         if (predicate.invoke(super.peek()))
	 *             return true;
	 *     return false;
	 * </pre></code>
	 * Only those elements for which the specified predicate returns
	 * <code>true</code> are returned by <code>next</code> or
	 * <code>peek</code>.
	 *
	 * @return <code>true</code> if the filter has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		if (computedHasNext)
			return hasNext;
		for (; super.hasNext(); super.next()) 
			if (predicate.invoke(super.peek()))
				return true;
		return false;
	}
}
