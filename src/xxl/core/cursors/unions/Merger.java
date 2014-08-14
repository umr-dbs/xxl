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

package xxl.core.cursors.unions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.queues.Heap;
import xxl.core.collections.queues.Queue;
import xxl.core.comparators.FeatureComparator;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;

/**
 * A merger serializes input iterations with respect to a given
 * {@link java.util.Comparator comparator} or
 * {@link xxl.core.collections.queues.Queue queue}. The input iterations are
 * inserted into a queue, e.g., a
 * {@link xxl.core.collections.queues.Heap heap}, defining a strategy for
 * merging them. Some queues need a comparator to put their elements in order,
 * so the caller is able to specify his queue and his comparator with special
 * constructors. The serialization of the input iteration's elements is
 * depending on the queue's implementation, e.g., the queue can use a
 * FIFO-strategy with the intention to receive a cyclic order of the input
 * iteration's elements.
 * <code><pre>
 *     cursors[0].next(),
 *     ...,
 *     cursors[cursors-1].next(),
 *     cursors[0].next(),
 *     ...
 * </pre></code>
 * But be aware, that the merger works with lazy-evaluation, so calling the 
 * <code>next</code> or <code>peek</code> method accesses the next cursor,
 * named <code>minCursor</code> delivered by the queue and calls its
 * <code>next</code> or <code>peek</code> method. If this iteration contains
 * further elements it will be added to the queue for an other time. The
 * position this iteration is inserted in the queue is defined by the queue's
 * strategy.
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   HashGrouper&lt;Integer&gt; hashGrouper = new HashGrouper&lt;Integer&gt;(
 *       new Enumerator(21),
 *       new Function&lt;Integer, Integer&gt;() {
 *           public Integer invoke(Integer next) {
 *               return next % 5;
 *           }
 *       }
 *   );
 *   
 *   hashGrouper.open();
 *   
 *   Cursor[] cursors = new Cursor[5];
 *   for (int i = 0; hashGrouper.hasNext(); i++)
 *       cursors[i] = hashGrouper.next();
 *   
 *   Merger&lt;Integer&gt; merger = new Merger&lt;Integer&gt;(
 *       ComparableComparator.INTEGER_COMPARATOR,
 *       (Iterator&lt;Integer&gt;[])cursors
 *   );
 *   
 *   merger.open();
 *   
 *   while (merger.hasNext())
 *       System.out.print(merger.next() + "; ");
 *   System.out.flush();
 *   
 *   merger.close();
 * </pre></code>
 * This example uses a hash-grouper to partition the input data, i.e., the
 * function (object modulo 5) is invoked on each element of the enumerator with
 * range 0,...,20. Because the <code>next</code> method of the hash-grouper
 * returns a cursor pointing to the next group, i.e., the next bucket in the
 * hash-map, all returned cursors are stored in a cursor array named
 * <code>cursors</code>. For more detailed information see
 * {@link xxl.core.cursors.groupers.HashGrouper}. This cursor array is given to
 * the constructor of the merger internally using the defined comparator that
 * compares two integer objects. This instance of a merger uses a
 * {@link xxl.core.collections.queues.Heap heap} to arrange the cursors,
 * because no queue has been specified in the constructor. Because of the
 * specified comparator and the implementation of the heap realizing a
 * min-heap, the elements in the buckets of the hash-map are merged and
 * returned in ascending order. The generated output is as follows:
 * <pre>
 *   0; 1; 2; 3; 4; 5; 6; 7; 8; 9; 10; 11; 12; 13; 14; 15; 16; 17; 18; 19; 20;
 * </pre>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   merger = new Merger&lt;Integer&gt;(
 *       new StackQueue&lt;Cursor&lt;Integer&gt;&gt;(),
 *       new Enumerator(11),
 *       new Enumerator(11, 21)
 *   );
 *   
 *   merger.open();
 *   
 *   while (merger.hasNext())
 *       System.out.print(merger.next() + "; ");
 *   System.out.flush();
 *   
 *   merger.close();
 * </pre></code>
 * In this case, the used queue realizes a LIFO-queue (last in first out), so
 * the second enumerator is completely consumed at first, then the elements of
 * the first enumerator are returned. So the elements are printed to the output
 * stream in the following order:
 * <pre>
 *   11; 12; 13; 14; 15; 16; 17; 18; 19; 20; 0; 1; 2; 3; 4; 5; 6; 7; 8; 9; 10;
 * </pre></p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see java.util.Comparator
 */
public class Merger<E> extends AbstractCursor<E> {

	/**
	 * The array containing the input iterations to be merged.
	 */
	protected List<Cursor<E>> cursors;

	/**
	 * The queue used to define an order for the merge of the input iterations.
	 */
	protected Queue<Cursor<E>> queue;

	/**
	 * The iteration representing the next element of the queue. All method
	 * calls concerning the actual element of the merger are redirected to this
	 * iteration.
	 */
	protected Cursor<E> minCursor = null;

	/**
	 * Creates a new merger backed on an input iteration array and a queue
	 * delivering the strategy used for merging the input iterations. All input
	 * iterations are inserted into the given queue. Every iterator given to
	 * this constructor is wrapped to a cursor.
	 *
	 * @param queue the queue defining the strategy the input iterations are
	 *        accessed.
	 * @param iterators the input iterations to be merged.
	 */
	public Merger(Queue<Cursor<E>> queue, Iterator<E>... iterators) {
		cursors = new ArrayList<Cursor<E>>(iterators.length);
		for (Iterator<E> iterator : iterators)
			cursors.add(Cursors.wrap(iterator));
		this.queue = queue;
	}
	
	/**
	 * Creates a new merger backed on an input cursor list and a queue
	 * delivering the strategy used for merging the input cursors. All input
	 * cursors are inserted into the given queue.
	 *
	 * @param queue the queue defining the strategy the input iterations are
	 *        accessed.
	 * @param cursors the list of cursors to be merged.
	 */
	public Merger(Queue<Cursor<E>> queue, List<Cursor<E>> cursors) {
		this.cursors = cursors;
		this.queue = queue;
	}

	/**
	 * Creates a new merger backed on an input cursor list and a
	 * {@link xxl.core.collections.queues.Heap heap} for merging the input
	 * cursors. The order is defined by the specified comparator in that
	 * way, that a new
	 * {@link xxl.core.comparators.FeatureComparator feature-comparator} is
	 * used calling the <code>compare</code> method of the specified comparator
	 * in order to compare two elements delivered by the input cursors. So
	 * the heap manages the cursors' elements, but the order is defined by
	 * the <code>next</code> element that would be returned by these
	 * cursors.
	 *
	 * @param comparator the comparator used to compare two elements of the
	 *        input iteration.
	 * @param cursors the list of cursors to be merged.
	 */
	public Merger(Comparator<? super E> comparator, List<Cursor<E>> cursors) {
		this(
			new Heap<Cursor<E>>(
				cursors.size(),
				new FeatureComparator<E, Cursor<E>>(
					comparator,
					new AbstractFunction<Cursor<E>, E>() {
						public E invoke(Cursor<E> cursor) {
							return cursor.peek();
						}
					}
				)
			),
			cursors
		);
	}
	
	
	/**
	 * Creates a new merger backed on an input iteration array and a
	 * {@link xxl.core.collections.queues.Heap heap} for merging the input
	 * iterations. The order is defined by the specified comparator in that
	 * way, that a new
	 * {@link xxl.core.comparators.FeatureComparator feature-comparator} is
	 * used calling the <code>compare</code> method of the specified comparator
	 * in order to compare two elements delivered by the input iterations. So
	 * the heap manages the iterations' elements, but the order is defined by
	 * the <code>next</code> element that would be returned by these
	 * iterations. Every iterator given to this constructor is wrapped to a
	 * cursor.
	 *
	 * @param comparator the comparator used to compare two elements of the
	 *        input iteration.
	 * @param iterators the input iterations to be merged.
	 */
	public Merger(Comparator<? super E> comparator, Iterator<E>... iterators) {
		this(
			new Heap<Cursor<E>>(
				iterators.length,
				new FeatureComparator<E, Cursor<E>>(
					comparator,
					new AbstractFunction<Cursor<E>, E>() {
						public E invoke(Cursor<E> cursor) {
							return cursor.peek();
						}
					}
				)
			),
			iterators
		);
	}

	/**
	 * Opens the merger, i.e., signals the cursor to reserve resources, open
	 * input iterations, etc. Before a cursor has been opened calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Therefore <code>open</code> must be called before a
	 * cursor's data can be processed. Multiple calls to <code>open</code> do
	 * not have any effect, i.e., if <code>open</code> was called the cursor
	 * remains in the state <i>opened</i> until its <code>close</code> method
	 * is called.
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
		for (Cursor<E> cursor : cursors)
			cursor.open();
		queue.open();
		for (Cursor<E> cursor : cursors)
			if (cursor.hasNext())
				queue.enqueue(cursor);
	}

	/**
	 * Closes the merger, i.e., signals the cursor to clean up resources, close
	 * input iterations, etc. When a cursor has been closed calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Multiple calls to <code>close</code> do not have any
	 * effect, i.e., if <code>close</code> was called the cursor remains in the
	 * state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		for (Cursor<E> cursor : cursors)
			cursor.close();
		queue.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the merger has more elements.
	 */
	protected boolean hasNextObject() {
		return queue.size() > 0;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the merger's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * <p>A next element is available if the queue, which contains the input
	 * iterations, is not empty. The queue realizes a strategy, so delivers the
	 * the input iterations in a specific order. Therefore the next element is
	 * determined by accessing the next element of the queue's
	 * <code>peek</code> element, the cursor <code>minCursor</code>. If this
	 * cursor returned by the queue contains further elements,
	 * <code>queue.replace(minCursor)</code> is performed, otherwise the next
	 * cursor in the queue, returned by the queue's <code>next</code> method,
	 * will be consumed.</p>
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		minCursor = queue.dequeue();
		E minimum = minCursor.next();
		if (minCursor.hasNext())
			queue.enqueue(minCursor);
		return minimum;
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the merger (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and removes the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>remove</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behaviour of a cursor is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the merger.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		if (minCursor == null)
			throw new IllegalStateException();
		minCursor.remove();
		minCursor = null;
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the merger. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the merger, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return minCursor != null ?
			minCursor.supportsRemove() :
			false;
	}

	/**
	 * Replaces the last element returned by the merger in the underlying data
	 * structure (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and updates the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>update</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behaviour of a merger is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        merger.
	 * @throws IllegalStateException if the <code>nextcode/tt> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>update</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the merger.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		if (minCursor == null)
			throw new IllegalStateException();
		minCursor.update(object);
		minCursor = null;
	}

	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the merger. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the merger, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return minCursor != null ?
			minCursor.supportsUpdate() :
			false;
	}

	/**
	 * Resets the merger to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * merger (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the merger.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		for (Cursor<E> cursor : cursors)
			cursor.reset();
		queue.clear();
		for (Cursor<E> cursor : cursors)
			if (cursor.hasNext())
				queue.enqueue(cursor);
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the merger. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the merger, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		for (Cursor<E> cursor : cursors)
			if (!cursor.supportsReset())
				return false;
		return true;
	}
}
