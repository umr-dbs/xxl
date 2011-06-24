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

package xxl.core.cursors.differences;

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;

/**
 * A sort-based implementation of the difference operator
 * (<code>input1 - input2</code>). This operation can be performed in two
 * different ways, namely the first realization removes an element of
 * <code>input1</code> if the same element exists in <code>input2</code>. The
 * second way of processing removes all elements of <code>input1</code> that
 * match with an element of <code>input2</code>. This second approch implies
 * that no duplicates will be returned by the difference operator, whereas the
 * first solution may contain duplicates if the number of equal elements in
 * cursor <code>input1</code> is greater than that of <code>input2</code>.
 * 
 * <p>The boolean flag <code>all</code> signals if all elements of cursor
 * <code>input1</code> that are equal to an element of <code>input2</code> will
 * be removed. In contrast to setting the flag to <code>false</code>, only one
 * element will be removed. So depending on this flag the result of the
 * difference operation can be a <i>set</i>, i.e., if <code>all</code> is
 * <code>true</code> all duplicates will be removed in the output of this
 * cursor, otherwise only one element is removed and the result may be a
 * <i>multi-set</i>, i.e., duplicates may occur in the output.</p>
 * 
 * <p><b>Precondition:</b> The input cursors have to be sorted!</p>
 * 
 * <p><b>Note:</b> If an input iteration is given by an object of the class
 * {@link Iterator}, i.e., it does not support the <code>peek</code> operation,
 * it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   SortBasedDifference&lt;Integer&gt; difference = new SortBasedDifference&lt;Integer&gt;(
 *       new Enumerator(21),
 *       new Filter&lt;Integer&gt;(
 *           new Enumerator(21),
 *           new Predicate&lt;Integer&gt;() {
 *               public boolean invoke(Integer next) {
 *                   return next % 2 == 0;
 *               }
 *           }
 *       ),
 *       ComparableComparator.INTEGER_COMPARATOR,
 *       true,
 *       true
 *   );
 *   
 *   difference.open();
 *   
 *   while (difference.hasNext())
 *       System.out.println(difference.next());
 *   
 *   difference.close();
 * </pre></code>
 * This example shows how to remove all even numbers from a given enumerator
 * with range [0, 20]. A default instance of a default 
 * {@link xxl.core.comparators.ComparableComparator#INTEGER_COMPARATOR comparator}
 * for integers is used to compare the elements of the two inputs. This kind of
 * comparator is also be chosen, if no comparator has been specified. The flag
 * <code>all</code>, which is set to <code>true</code> does not have any effect
 * in this case due to unique input elements.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   difference = new SortBasedDifference&lt;Integer&gt;(
 *       new ArrayCursor&lt;Integer&gt;(1, 2, 2, 2, 3),
 *       new ArrayCursor&lt;Integer&gt;(1, 2, 2, 3),
 *       ComparableComparator.INTEGER_COMPARATOR,
 *       false,
 *       true
 *   );
 *   
 *   difference.open();
 *   
 *   while (difference.hasNext())
 *       System.out.println(difference.next());
 *       
 *   difference.close();
 * </pre></code>
 * The first input cursor contains the elements {1, 2, 2, 2, 3}. The second
 * cursor, that is to be subtracted, delivers the elements {1, 2, 2, 3}. So, in
 * this case the flag <code>all</code> plays an important role. If it is
 * <code>false</code>, as shown above, the sort-based difference operator
 * delivers the element {2} as the only result. If it has been set to
 * <code>true</code> the sort-based difference operator returns no
 * elements.</p>
 * 
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see java.util.Comparator
 * @see xxl.core.comparators.ComparableComparator
 * @see xxl.core.cursors.differences.NestedLoopsDifference
 * @see xxl.core.relational.cursors.SortBasedDifference
 */
public class SortBasedDifference<E> extends AbstractCursor<E> {

	/**
	 * The first (or left) input cursor of the difference operator.
	 */
	protected Cursor<E> input1;
	
	/**
	 * The second (or right) input cursor of the difference operator.
	 */
	protected Cursor<? extends E> input2;
	
	/**
	 * The comparator used to compare the elements of the two input cursors.
	 */
	protected Comparator<? super E> comparator;

	/**
	 * A flag signaling if all matches returned by the comparator should be
	 * removed or only one element will be removed. So depending on this flag
	 * the result of difference operation can be a set, i.e., if
	 * <code>all</code> is <code>true</code> all duplicates will be removed in
	 * the resulting cursor, otherwise only one is removed and the result may
	 * be a multi-set, i.e., duplicates may occur in the resulting cursor.
	 */
	protected boolean all;

	/**
	 * A flag showing if the input cursors have been sorted in ascending or
	 * descending order.
	 */
	protected boolean asc;

	/**
	 * Creates a new instance of the sort-based difference operator. Every
	 * iterator given to this constructor is wrapped to a cursor.
	 *
	 * @param sortedInput1 the first input iterator where the elements have to
	 *        be subtracted from.
	 * @param sortedInput2 the second input iterator containing the elements
	 *        that have to be subtracted.
	 * @param comparator a comparator comparing the elements of the two input
	 *        cursors.
	 * @param all a boolean flag signaling if all elements of cursor
	 *        <code>input1</code> that are equal to an element of
	 *        <code>input2</code> will be removed, otherwise only one element
	 *        is removed.
	 * @param asc a flag showing if the input cursors have been sorted
	 *        ascending or descending.
	 */
	public SortBasedDifference(Iterator<E> sortedInput1, Iterator<? extends E> sortedInput2, Comparator<? super E> comparator, boolean all, boolean asc) {
		this.input1 = Cursors.wrap(sortedInput1);
		this.input2 = Cursors.wrap(sortedInput2);
		this.comparator = comparator;
		this.all = all;
		this.asc = asc;
	}

	/**
	 * Opens the sort-based difference operator, i.e., signals the cursor to
	 * reserve resources, open input iterations, etc. Before a cursor has been
	 * opened calls to methods like <code>next</code> or <code>peek</code> are
	 * not guaranteed to yield proper results. Therefore <code>open</code> must
	 * be called before a cursor's data can be processed. Multiple calls to
	 * <code>open</code> do not have any effect, i.e., if <code>open</code> was
	 * called the cursor remains in the state <i>opened</i> until its
	 * <code>close</code> method is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened)
			return;
		super.open();
		input1.open();
		input2.open();
	}
	
	/**
	 * Closes the sort-based difference operator, i.e., signals the cursor to
	 * clean up resources and close its input cursors. When a cursor has been
	 * closed calls to methods like <code>next</code> or <code>peek</code> are
	 * not guaranteed to yield proper results. Multiple calls to
	 * <code>close</code> do not have any effect, i.e., if <code>close</code>
	 * was called the cursor remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		input1.close();
		input2.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>If <code>input2</code> has no further elements all remaining elements
	 * of <code>input1</code> can be returned. If the input cursors are sorted
	 * ascending, also the elements of <code>input1</code> are returned, if
	 * they are smaller than the next element of <code>input2</code>. If the
	 * next element of <code>input1</code> and <code>input2</code> are equal,
	 * i.e., the given comparator returns 0, when comparing them, this element
	 * will not be returned. Depending on the flag <code>all</code>, all
	 * elements that are equal to the skipped element are skipped, too. So, if
	 * <code>all</code> is <code>true</code> the resulting cursor contains no
	 * duplicates. If the next element of <code>input2</code> is larger than
	 * the next one of <code>input1</code>, <code>input2</code> is consumed as
	 * long as this condition is fulfilled. If the input cursors are sorted
	 * descending the conditions explained above are negated and the
	 * computation runs the same way.
	 * 
	 * @return <code>true</code> if the sort-based difference operator has more
	 *         elements.
	 */
	protected boolean hasNextObject() {
		boolean exit;
		do {
			exit = true;
			if (!input2.hasNext())
			  	if (input1.hasNext()) {
			  		next = input1.next();
			  		return true;
			  	}
			  	else
			  		return false;
			else
				if (input1.hasNext()) {
				  	int res = comparator.compare(input1.peek(), input2.peek());
					if ((asc && res < 0) || (!asc && res > 0)) {
						next = input1.next();
						return true;
					}
					else
						if (res == 0) {
							input1.next();
							if (all) // remove duplicates
								while(input1.hasNext() && comparator.compare(input1.peek(), input2.peek()) == 0)
									input1.next();
							else
								input2.next();
							exit = false;
						}
						else { // (asc && res > 0) || (!asc && res < 0)
							input2.next();
							while(input2.hasNext() && ((asc && comparator.compare(input1.peek(), input2.peek()) > 0) || (!asc && comparator.compare(input1.peek(), input2.peek()) < 0)))
								input2.next();
							exit = false;
						}
				}
				else
					return false;
		}
		while (!exit);
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be removed
	 * from the iteration, if <code>next</code> is called.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return next;
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the sort-based difference operator (optional operation). This method can
	 * be called only once per call to <code>next</code> or <code>peek</code>
	 * and removes the element returned by this method. Note, that between a
	 * call to <code>next</code> and <code>remove</code> the invocation of
	 * <code>peek</code> or <code>hasNext</code> is forbidden. The behaviour of
	 * a cursor is unspecified if the underlying data structure is modified
	 * while the iteration is in progress in any way other than by calling this
	 * method.
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the sort-based difference
	 *         operator.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		input1.remove();
	}

	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the sort-based difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return input1.supportsRemove();
	}

	/**
	 * Replaces the object that was returned by the last call to
	 * <code>next</code> or <code>peek</code> (optional operation). This
	 * operation must not be called after a call to <code>hasNext</code>. It
	 * should follow a call to <code>next</code> or <code>peek</code>. This
	 * method should be called only once per call to <code>next</code> or
	 * <code>peek</code>. The behaviour of a sort-based difference operator is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * @param object the object that replaces the object returned by the last
	 *        call to <code>next</code> or <code>peek</code>.
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>update</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the sort-based difference
	 *         operator.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		input1.update(object);
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the sort-based difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return input1.supportsUpdate();
	}

	/**
	 * Resets the sort-based difference operator to its initial state (optional
	 * operation). So the caller is able to traverse the underlying data
	 * structure again. The modifications, removes and updates concerning the
	 * underlying data structure, are still persistent. This method also resets
	 * the input iterations.
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the sort-based difference
	 *         operator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input1.reset();
		input2.reset();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the sort-based difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input1.supportsReset() && input2.supportsReset();
	}
}
