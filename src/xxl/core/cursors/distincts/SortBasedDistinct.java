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

package xxl.core.cursors.distincts;

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.queues.DistinctQueue;
import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/**
 * A sort-based implementation of a distinct operator, i.e., all duplicates
 * contained in the input cursor will be removed. The input cursor is traversed
 * and a predicate determines whether two successive input elements are equal.
 * If this is the case the duplicates will not be returned by calls to
 * <code>next</code> or <code>peek</code>.
 * 
 * <p><b>Note:</b> This operator can be used with a sorted or unsorted input
 * cursor! If the input cursor is not sorted a new
 * {@link xxl.core.cursors.sorters.MergeSorter merge sorter} will be created
 * using {@link xxl.core.collections.queues.DistinctQueue distinct} queues for
 * an early duplicate elimination. Therefore this operator only forwards the
 * method calls to the merge sorter. If an input iteration is given by an
 * object of the class {@link java.util.Iterator Iterator}, i.e., it does not
 * support the <code>peek</code> operation, it is internally wrapped to a
 * cursor.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   SortBasedDistinct&lt;Integer&gt; distinct = new SortBasedDistinct&lt;Integer&gt;(
 *       new ArrayCursor&lt;Integer&gt;(
 *           new Integer[] {1, 1, 2, 2, 3, 4, 4, 4, 5, 6}
 *       )
 *   );
 *   
 *   distinct.open();
 *   
 *   while (distinct.hasNext())
 *       System.out.println(distinct.next());
 *   
 *   distinct.close();
 * </pre></code>
 *
 * The input cursor contains the elements {1, 1, 2, 2, 3, 4, 4, 4, 5, 6}. If
 * the sort-based distinct operation is applied all duplicates will be removed,
 * i.e., each delivered element returned by call to <code>next</code> is
 * unique. This example uses the {@link xxl.core.predicates.Equal equal}
 * predicate by default, therefore two elements are compared according to their
 * integer values. If the sort-based distinct operator is completely consumed,
 * the output of this example looks as follows:
 * <pre>
 *   1
 *   2
 *   3
 *   4
 *   5
 *   6
 * </pre></p>
 *
 * <p><b>Example usage (2): early duplicate elimination</b>
 * <code><pre>
 *   distinct = new SortBasedDistinct&lt;Integer&gt;(
 *       new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 20),
 *       ComparableComparator.INTEGER_COMPARATOR,
 *       8,
 *       12*4096,
 *       4*4096
 *   );
 *   
 *   distinct.open();
 *   
 *   while (distinct.hasNext())
 *       System.out.println(distinct.next());
 *   
 *   distinct.close();
 * </pre></code>
 * This example sorts twenty randomly distributed integer values of the
 * interval [0, 10) and removes all duplicates using
 * {@link xxl.core.collections.queues.DistinctQueue distinct} queues to store
 * the elements during run-creation.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.DistinctQueue
 * @see xxl.core.cursors.distincts.NestedLoopsDistinct
 * @see xxl.core.relational.cursors.SortBasedDistinct
 */
public class SortBasedDistinct<E> extends AbstractCursor<E> {

	/**
	 * The input cursor delivering the elements.
	 */
	protected Cursor<E> cursor;

	/**
	 * The predicate determining if two successive input elements are equal.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * The last object returned by the input cursor.
	 */
	protected E last;

	/**
	 * A flag that shows if the input cursor is initialized, i.e., if the first
	 * element has been returned already.
	 */
	protected boolean initialized = false;

	/**
	 * A flag signaling if an early duplicate elimination has been performed.
	 */
	protected boolean sorted = false;

	/**
	 * Function returning a new
	 * {@link xxl.core.collections.queues.DistinctQueue distinct queue} only
	 * needed if an early duplicate elimination during the sort-operation is
	 * performed.
	 */
	protected Function<Function<?, Integer>, Queue<E>> newDistinctQueue = new AbstractFunction<Function<?, Integer>, Queue<E>>() {
		public DistinctQueue<E> invoke(Function<?, Integer> functionInputBufferSize, Function<?, Integer> functionOutputBufferSize) {
			return new DistinctQueue<E>(new ListQueue<E>());
		}
	};

	/**
	 * Creates a new sort-based distinct operator.
	 *
	 * @param sortedIterator a sorted input iterator that delivers the
	 *        elements.
	 * @param predicate a binary predicate that returns <code>true</code> if
	 *        two successive input elements are equal.
	 */
	public SortBasedDistinct(Iterator<E> sortedIterator, Predicate<? super E> predicate) {
		this.cursor = Cursors.wrap(sortedIterator);
		this.predicate = predicate;
	}

	/**
	 * Creates a new sort-based distinct operator, which uses an
	 * {@link xxl.core.predicates.Equal equal} predicate to compare two
	 * successive input elements.
	 *
	 * @param sortedIterator a sorted input iterator that delivers the
	 *        elements.
	 */
	public SortBasedDistinct(Iterator<E> sortedIterator) {
		this(sortedIterator, Equal.DEFAULT_INSTANCE);
	}

	/**
	 * Creates a new sort-based distinct operator and sorts the input at first
	 * using a {@link xxl.core.cursors.sorters.MergeSorter merge sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct} queues. The
	 * merge sorter performs an early duplicate elimination during the sort
	 * operation.
	 *
	 * @param input the input iterator to be sorted.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param blockSize the size of a block (page).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param firstOutputBufferRatio the ratio of memory available to the
	 *        output buffer during run-creation (0.0: use only one page for the
	 *        output buffer and what remains is used for the heap; 1.0: use as
	 *        much memory as possible for the output buffer).
	 * @param outputBufferRatio the amount of memory available to the output
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the output buffer, what remains is used for the
	 *        merger and the input buffer, inputBufferRatio determines how the
	 *        remaining memory is distributed between them; 1.0: use as much
	 *        memory as possible for the output buffer).
	 * @param inputBufferRatio the amount of memory available to the input
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the input buffer, what remains is used for the
	 *        merger (maximal FanIn); 1.0: use as much memory as possible for
	 *        the input buffer).
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 * @param finalInputBufferRatio the amount of memory available to the input
	 *        buffer of the final (online) merge (0.0: use the maximum number
	 *        of inputs (maximal fanIn), i.e., perform the online merge as
	 *        early as possible; 1.0: write the entire data into a final queue,
	 *        the online "merger" just reads the data from this queue).
	 * @param newQueuesQueue if this function is invoked, the queue, that
	 *        should contain the queues to be merged, is returned. The function
	 *        takes an iterator and the comparator
	 *        <code>queuesQueueComparator</code> as parameters, e.g.,
	 *        <code><pre>
	 *          new Function&lt;Object, Queue&lt;E&gt;&gt;() {
	 *              public Queue&lt;E&gt; invoke(Object iterator, Object comparator) {
	 *                  return new DynamicHeap&lt;E&gt;((Iterator&lt;E&gt;)iterator, (Comparator&lt;E&gt;)comparator);
	 *              }
	 *          };
	 *        </pre></code>
	 *        The queues contained in the iterator are inserted in the dynamic
	 *        heap using the given comparator for comparison.
	 * @param queuesQueueComparator this comparator determines the next queue
	 *        used for merging.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(
		Iterator<? extends E> input,
		Comparator<? super E> comparator,
		int blockSize,
		int objectSize,
		int memSize,
		double firstOutputBufferRatio,
		double outputBufferRatio,
		double inputBufferRatio,
		int finalMemSize,
		double finalInputBufferRatio,
		Function<Object, ? extends Queue<Cursor<E>>> newQueuesQueue,
		Comparator<? super Cursor<E>> queuesQueueComparator
	) {
		this.cursor = new MergeSorter<E>(
			input,
			comparator,
			blockSize,
			objectSize,
			memSize,
			firstOutputBufferRatio,
			outputBufferRatio,
			inputBufferRatio,
			finalMemSize,
			finalInputBufferRatio,
			newDistinctQueue,
			newQueuesQueue,
			queuesQueueComparator,
			false
		);
		sorted = true;
	}

	/**
	 * Creates a new sort-based distinct operator and sorts the input at first
	 * using a {@link xxl.core.cursors.sorters.MergeSorter merge sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct} queues. The
	 * merge sorter performs an early duplicate elimination during the sort
	 * operation.
	 *
	 * @param input the input iterator to be sorted.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(
		Iterator<? extends E> input,
		Comparator<? super E> comparator,
		int objectSize,
		int memSize,
		int finalMemSize
	) {
		this.cursor = new MergeSorter<E>(
			input,
			comparator,
			objectSize,
			memSize,
			finalMemSize,
			newDistinctQueue,
			false
		);
		sorted = true;
	}

	/**
	 * Opens the sort-based distinct operator, i.e., signals the cursor to
	 * reserve resources, open the input iteration, etc. Before a cursor has
	 * been opened calls to methods like <code>next</code> or <code>peek</code>
	 * are not guaranteed to yield proper results. Therefore <code>open</code>
	 * must be called before a cursor's data can be processed. Multiple calls
	 * to <code>open</code> do not have any effect, i.e., if <code>open</code>
	 * was called the cursor remains in the state <i>opened</i> until its
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
		cursor.open();
	}
	
	/**
	 * Closes the sort-based distinct operator. Signals the cursor to clean up
	 * resources, close the input cursor, etc. After a call to
	 * <code>close</code> calls to methods like <code>next</code> or
	 * <code>peek</code> are not guaranteed to yield proper results. Multiple
	 * calls to <code>close</code> do not have any effect, i.e., if
	 * <code>close</code> was called the sort-based distinct operator remains
	 * in the state "closed".
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		cursor.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>The first element can instantly be returned, after that the operator
	 * remains in the state <code>initialized</code>. If a further element
	 * exists it is checked by the given predicate invoked on the last and the
	 * next element of the input cursor. If the predicate returns
	 * <code>true</code> this next element is filtered out, otherwise it will
	 * be returned by a call to <code>next</code>.</p>
	 *
	 * @return <code>true</code> if the sort-based distinct operator has more
	 *         elements.
	 */
	protected boolean hasNextObject() {
		if (!initialized || sorted)
			return cursor.hasNext();
		else
			for (; cursor.hasNext(); cursor.next())
				if (!predicate.invoke(last, cursor.peek()))
					return true;
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be removed
	 * from the iteration, if <code>next</code> is called. This method returns
	 * the next element of the input cursor, which has not been filtered out by
	 * the given predicate.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		initialized = true;
		return last = cursor.next();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the sort-based distinct operator (optional operation). This method can
	 * be called only once per call to <code>next</code> or <code>peek</code>.
	 * The behaviour of a sort-based distinct operator is unspecified if the
	 * underlying data structure is modified while the iteration is in progress
	 * in any way other than by calling this method.
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the sort-based distinct operator,
	 *         i.e., the underlying cursor does not support the
	 *         <code>remove</code> operation.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		cursor.remove();
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the sort-based distinct operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return cursor.supportsRemove();
	}

	/**
	 * Replaces the object that was returned by the last call to
	 * <code>next</code> or <code>peek</code> (optional operation). This
	 * operation must not be called after a call to <code>hasNext</code>. It
	 * should follow a call to <code>next</code> or <code>peek</code>. This
	 * method should be called only once per call to <code>next</code> or
	 * <code>peek</code>. The behaviour of a sort-based distinct operator is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 *
	 * @param object the object that replaces the object returned by the last
	 *        call to <code>next</code> or <code>peek</code>.
	 * @throws IllegalStateException if there is no object which can be
	 *         updated.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the SortBasedDistinct operator,
	 *         i.e., the underlying cursor does not support the
	 *         <code>update</code> operation.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		cursor.update(object);
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the sort-based distinct operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return cursor.supportsUpdate();
	}

	/**
	 * Resets the sort-based distinct operator to its initial state (optional
	 * operation). So the caller is able to traverse the underlying data
	 * structure again. The modifications, removes and updates concerning the
	 * underlying data structure, are still persistent.
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code> method
	 *         is not supported by the sort-based distinct operator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		cursor.reset();
		initialized = false;
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the sort-based distinct operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return cursor.supportsReset();
	}
}
