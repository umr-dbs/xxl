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

package xxl.core.cursors.groupers;

import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;
import xxl.core.predicates.FunctionPredicate;
import xxl.core.predicates.Predicate;

/**
 * A sort-based implementation of the group operator. A sort-based grouper
 * partitions input data into groups (lazy evaluation). A binary predicate (or
 * boolean function) is used to compare the current element with the last
 * element consumed from the underlying input iteration. If this predicate
 * returns <code>true</code> a new group starts, otherwise the next element
 * delivered from the underlying iteration belongs to the same group. A call to
 * <code>next</code> returns a cursor pointing to the next group!
 * 
 * <p><b>Precondition:</b> The input cursor has to be sorted!</p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *     SortBasedGrouper&lt;Integer&gt; sortBasedGrouper = new SortBasedGrouper&lt;Integer&gt;(
 *         new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50),
 *         new Predicate&lt;Integer&gt;() {
 *             public boolean invoke(Integer previous, Integer next) {
 *                 return previous &gt; next;
 *             }
 *         }
 *     );
 * 
 *     sortBasedGrouper.open();
 * 
 *     Cursor&lt;Integer&gt; sequence;
 *     while (sortBasedGrouper.hasNext()) {
 *         sequence = sortBasedGrouper.next();
 *         while (sequence.hasNext())
 *             System.out.print(sequence.next() + "; ");
 *         System.out.flush();
 *         System.out.println();
 *     }
 * 
 *     sortBasedGrouper.close();
 * </pre></code>
 * This example partitions 50 random integers with maximum value '99' into
 * groups. A new group starts if the predicate returns <code>true</code>. That
 * is the case when the next integer value delivered by the input cursor is
 * lower than the previously returned integer value. So this instance of a
 * sort-based grouper returns all monotonous ascending sequences contained in
 * the input iteration by lazy evaluation.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.groupers.HashGrouper
 * @see xxl.core.cursors.groupers.NestedLoopsGrouper
 */
public class SortBasedGrouper<E> extends AbstractCursor<Cursor<E>> {

	/**
	 * The input iteration to be grouped.
	 */
	protected Cursor<E> input;

	/**
	 * A cursor iterating over the currently traversed group.
	 */
	protected Cursor<E> currentGroup;

	/**
	 * The binary predicate deciding if a new group starts. The evaluated
	 * predicate returns <code>true</code> a new group starts, otherwise the
	 * next element of the underlying cursor belongs to the same group.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * The previous consumed object.
	 */
	protected E previous;
	
	/**
	 * The pre-previous consumed object.
	 */
	protected E prePrevious;

	/**
	 * The size of the current group.
	 */
	protected int groupSize = 0;

	/**
	 * A flag to signal if an <code>update</code> operation is possible.
	 */
	protected boolean updatePossible = false;

	/**
	 * Creates a new sort-based grouper. If an iterator is given to this
	 * constructor it is wrapped to a cursor.
	 *
	 * @param iterator the input iteration to be grouped.
	 * @param predicate the binary predicate deciding if a new group starts.
	 */
	public SortBasedGrouper(Iterator<E> iterator, Predicate<? super E> predicate) {
		this.input = Cursors.wrap(iterator);
		this.predicate = predicate;
	}

	/**
	 * Creates a new sort-based grouper. If an iterator is given to this
	 * constructor it is wrapped to a cursor and the given boolean function is
	 * internally wrapped to a predicate.
	 *
	 * @param iterator the input iteration to be grouped.
	 * @param function a boolean function deciding if a new group starts.
	 */
	public SortBasedGrouper(Iterator<E> iterator, Function<? super E, Boolean> function) {
		this(iterator, new FunctionPredicate<E>(function));
	}

	/**
	 * Opens the sort-based grouper, i.e., signals the cursor to reserve
	 * resources, open the input iteration, etc. Before a cursor has been
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
		input.open();
	}
	
	/**
	 * Closes the sort-based grouper. Signals it to clean up resources, close
	 * the input iteration, etc. After a call to <code>close</code> calls to
	 * methods like <code>next</code> or <code>peek</code> are not guarantied
	 * to yield proper results. Multiple calls to <code>close</code> do not
	 * have any effect, i.e., if <code>close</code> was called the sort-based
	 * grouper remains in the state "closed".
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		input.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.) This happens when the specified predicate evaluated for the
	 * previous element and the next element of the input iteration, determined
	 * by its <code>peek</code> method is <code>true</code>. Then a new group
	 * starts.
	 *
	 * @return <code>true</code> if the sort-based grouper has more elements,
	 *         otherwise <code>false</code>.
	 */
	protected boolean hasNextObject() {
		if (currentGroup != null) {
			while (currentGroup.hasNext())
				currentGroup.next();
			currentGroup = null;
		}
		return input.hasNext() && (previous == null || predicate.invoke(previous, input.peek()));
	}

	/**
	 * Returns the next element in the iteration (a cursor iterating over the
	 * next group!). In this case that means the next group of elements that
	 * are equal concerning the user defined predicate is returned as a new
	 * cursor.<br />
	 * <b>Important:</b> LAZY EVALUATION is used!<br />
	 * With the help of this cursor instance the caller is able to traverse and
	 * operate with the group of elements. The delivered cursor does definitely
	 * not support the methods <code>close</code> and <code>reset</code>.
	 *
	 * @return the next element in the iteration (a cursor iterating over the
	 *         next group!).
	 */
	protected Cursor<E> nextObject() {
		groupSize = 0;
		return currentGroup = new AbstractCursor<E>() {

			public boolean hasNextObject() {
				return groupSize == 0 || input.hasNext() && !predicate.invoke(previous, input.peek());
			}

			public E nextObject() {
				updatePossible = true;
				groupSize++;
				prePrevious = previous;
				return previous = input.next();
			}

			public void remove() throws IllegalStateException, UnsupportedOperationException {
				super.remove();
				if (!updatePossible)
					throw new IllegalStateException();
				input.remove();
				updatePossible = false;
				previous = prePrevious;
				groupSize--;
			}
			
			public boolean supportsRemove() {
				return input.supportsRemove();
			}
			
			public void update(E object) throws IllegalStateException, UnsupportedOperationException {
				super.update(object);
				if (!updatePossible)
					throw new IllegalStateException();
				input.update(object);
				previous = object;
			}
			
			public boolean supportsUpdate() {
				return input.supportsUpdate();
			}
			
			@Override
			public void close() {
				// Does nothing
			}
		};
	}

	/**
	 * Resets the sort-based grouper to its initial state (optional operation).
	 * So the caller is able to traverse the underlying data structure again.
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code> method
	 *         is not supported by the input cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input.reset();
		groupSize = 0;
	}

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the sort-based grouper. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the sort-based grouper, otherwise
	 *         <code>false</code>.
	 */
	public boolean supportsReset() {
		return input.supportsReset();
	}
}
