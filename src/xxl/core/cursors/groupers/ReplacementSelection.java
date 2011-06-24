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

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.queues.Heap;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;

/**
 * The replacement-selection operator takes an iteration as its input and
 * creates sorted runs as its output. This technique is described in "Donald
 * Knuth.: <i>Sorting and Searching</i>. Addison-Wesley 1970." The
 * replacement-selection algorithm is especially useful for external sorting.
 * Recall, that an external merge-sort is performed by first producing
 * <code>n</code> sorted input-runs. These runs are then recursively merged to
 * a single output-run. The runs produced by replacement-selection operator
 * tend to be twice as big as the available memory or even bigger.
 * 
 * <p><b>Implementation details:</b> When initializing the
 * replacement-selection operator the input iteration's elements are inserted
 * in a array with length <code>size</code> until this array is filled up or
 * the input iteration has no more elements. With the help of this array and
 * the given comparator a new {@link xxl.core.collections.queues.Heap heap} is
 * created with the intention to order the elements. If a default
 * {@link xxl.core.comparators.ComparableComparator comparator} which assumes
 * that the elements implement the {@link java.lang.Comparable comparable}
 * interface is used, the elements will be returned in a natural ascending
 * order, because they will be organized in a min-heap. But if an
 * {@link xxl.core.comparators.InverseComparator inverse} comparator is used
 * instead, they will be organized in a max-heap and a descending order will
 * result. The method <code>check</code> verifies whether there are more
 * elements to process. If so and the heap is empty, it creates a new heap
 * using the elements that are reside in memory. The integer field
 * <code>n</code> displays the current position in the array. The array is used
 * for the creation of a new heap. It is initialized with <code>size</code>,
 * namely the number of elements that can be kept in memory. This array is
 * builds up a new heap during the run-creation. Consider the example a
 * comparable comparator defines the order in the heap, so a min-heap manages
 * the inserted elements of the input cursor. If such an element is lower than
 * the <code>peek</code> element of the heap a new run has to start, therefore
 * this element is written to the array. If the current heap has been emptied,
 * a new heap is instantly created by the method <code>check</code> called in
 * the method <code>next</code> of the replacement-selection operator. So a new
 * heap is built up during the other heap is consumed. The next element to be
 * returned is computed as follows:<br />
 * A next element only exists if <code>n &lt; array.length</code> or the heap
 * contains further elements. If this is the case the method <code>check</code>
 * is performed. If the input cursor contains more elements and the
 * <code>peek</code> method of the heap returns an element that is lower than
 * or equal to the next elemet of the input iteration concerning the used
 * comparator, the next element of the heap is returned after the next element
 * of the input iteration has been inserted into the heap. If the comparator
 * returned a value greater than 0, the next element of the heap is returned
 * and the next element of the input iteration is inserted in the array at
 * position <code>n</code>. The array builds up a new heap for the next run.
 * After that <code>n</code> is decremented. If the input iteration does not
 * contain further elements the next element of the heap is returned.</p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   ReplacementSelection&lt;Integer&gt; cursor = new ReplacementSelection&lt;Integer&gt;(
 *       new Enumerator(11),
 *       3,
 *       ComparableComparator.INTEGER_COMPARATOR
 *   );
 *   
 *   cursor.open();
 *   
 *   while (cursor.hasNext())
 *       System.out.print(cursor.next() + "; ");
 *   System.out.flush();
 *   System.out.println();
 *   
 *   cursor.close();
 * </pre></code>
 * This instance of a replacement-selection operator sorts the enumerator's
 * elements with range [0,11[ by using a memory size of 3, i.e., the heap
 * consists of a maximum of three elements. In this case a
 * {@link xxl.core.comparators.ComparableComparator comparator} which assumes
 * that the elements implement the {@link java.lang.Comparable comparable}
 * interface is used, so the elements are sorted in a natural order. If the
 * whole replacement-selection operator is consumed, only one single run
 * containing all of the underlying enumerator in ascending order is created.
 * That is the fact because using a comparable comparator causes that the heap
 * is organized as a min-heap and therefore the next element of the heap
 * (minimum) is returned every time. Due to the <code>peek</code> element of
 * the input iteration is greater than the <code>peek</code> element of the
 * heap. The next element of the input iteration is inserted in the heap and
 * then the heap is reorganized. Because the enumerator's elements are deliverd
 * in an ascending order a heap has to be build up only for one time. The
 * generated output looks as follows:
 * <pre>
 *   0; 1; 2; 3; 4; 5; 6; 7; 8; 9; 10;
 * </pre></p>
 *
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   cursor = new ReplacementSelection&lt;Integer&gt;(
 *       new Permutator(20),
 *       3,
 *       ComparableComparator.INTEGER_COMPARATOR
 *   );
 *   
 *   cursor.open();
 *   
 *   int last = 0;
 *   boolean first = true;
 *   while (cursor.hasNext())
 *       if (last > cursor.peek() || first) {
 *           System.out.println();
 *           System.out.print(" Run: ");
 *           last = cursor.next();
 *           System.out.print(last + "; ");
 *           first = false;
 *       }
 *       else {
 *           last = cursor.next();
 *           System.out.print(last + "; ");
 *       }
 *   System.out.flush();
 *   
 *   cursor.close();
 * </pre></code>
 * This instance of a replacement-selection uses a
 * {@link xxl.core.cursors.sources.Permutator permutator} with range [0,20[ and
 * memory size of 3, that is also the heap size. In this case a comparable
 * comparator is specified to compare two elements, i.e., a natural order will
 * result and the used heap is a min-heap, too. This example shows that more
 * than only one run can be created. A new run starts if the <code>peek</code>
 * element of the input iteration (permutator) is lower than the
 * <code>peek</code> element of the heap, i.e., the minimal element. The output
 * demonstrates the different created runs each having an ascending order
 * concerning the permutator's elements.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.cursors.sorters.MergeSorter
 */
public class ReplacementSelection<E> extends AbstractCursor<E> {

	/**
	 * The input iteration the runs should be created of.
	 */
	protected Cursor<? extends E> input;

	/**
	 * The array used to create a heap. The length of the array is specified by
	 * the parameter <code>size</code>.
	 */
	protected Object[] array;

	/**
	 * The number of elements that can be kept in memory.
	 */
	protected int size;

	/**
	 * Current position in the array.
	 */
	protected int n;

	/**
	 * The comparator used to compare two elements of the input iteration.
	 */
	protected Comparator<? super E> comparator;

	/**
	 * The heap used for the replacement-selection algorithm, ordering the
	 * elements.
	 */
	protected Heap<E> heap;

	/**
	 * Creates a new instance of the replacement-selection operator.
	 *
	 * @param iterator the input iteration the sorted runs should be created
	 *        of.
	 * @param size the number of elements that can be kept in memory.
	 * @param comparator the comparator used to compare two elements.
	 */
	public ReplacementSelection(Iterator<? extends E> iterator, int size, Comparator<? super E> comparator) {
		this.input = Cursors.wrap(iterator);
		this.comparator = comparator;
		this.size = size;
	}

	/**
	 * Opens the replacement-selection operator, i.e., signals the cursor to
	 * reserve resources, open the input iteration and initializing the
	 * internally used heap. Before a cursor has been opened calls to methods
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
		if (!isOpened)
			init();
		super.open();
	}
	
	/**
	 * Initializes the replacement-selection operator. The implementation of
	 * this method is as follows:
	 * <code><pre>
	 *     array = new Object[n = size];
	 *     input.open();
	 *     while (input.hasNext() && n &gt; 0)
	 *         array[--n] = input.next();
	 *     (heap = new Heap(array, 0, comparator)).open();
	 * </pre></code>
	 * The input iteration's elements are inserted in an array with length
	 * <code>size</code> until this array is filled up or the input iteration
	 * has no more elements. With the help of this array and the given
	 * comparator a new heap is created with the intention to order the
	 * elements.
	 */
	@SuppressWarnings("unchecked") // internally stored as Object array by Heap
	protected void init() {
		array = new Object[n = size];
		input.open();
		while (input.hasNext() && n > 0)
			array[--n] = input.next();
		(heap = new Heap<E>((E[])array, 0, comparator)).open();
	}

	/**
	 * Closes the replacement-selection operator, i.e., signals the cursor to
	 * clean up resources, close the input iteration and the internally used
	 * heap. When a cursor has been closed calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield
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
		input.close();
		heap.close();
	}

	/**
	 * Checks whether there are more elements to process. If so and the heap is
	 * empty, it creates a new heap using the elements that are reside in
	 * memory and sets <code>n</code> to <code>array.length</code>.
	 */
	@SuppressWarnings("unchecked") // internally stored as Object array by Heap
	protected void check() {
		if (heap.isEmpty()) {
			System.arraycopy(
				array,
				Math.max(n, array.length - n),
				array,
				0,
				Math.min(n, array.length - n)
			);
			(heap = new Heap<E>((E[])array, array.length - n, comparator)).open();
			n = array.length;
		}
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return n < array.length || !heap.isEmpty();
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * <p>Such an element exists if <code>n &lt; array.length</code> or the
	 * heap contains further elements. If this is the case the method
	 * <code>check</code> is performed. If the input iteration contains more
	 * elements and the next element of the  heap is lower than or equal to the
	 * next element of the input iteration concerning the used comparator, the
	 * next element of the heap is returned after the next element of the input
	 * iteration has been inserted into the heap. If the comparator returned a
	 * value greater than 0, the next element of the heap is returned and the
	 * next element of the input iteration is inserted in the array at position
	 * <code>n</code>. The array builds up a new heap for the next run. After
	 * that <code>n</code> is decremented. So a new heap is built up during the
	 * other heap is consumed. If the input cursor does not contain further
	 * elements the next element of the heap is returned.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		check();
		if (input.hasNext())
			if (comparator.compare(heap.peek(), input.peek()) <= 0)
				return heap.replace(input.next());
			else {
				E result = heap.dequeue();
				array[--n] = input.next();
				return result;
			}
		else
			return heap.dequeue();
	}

	/**
	 * Resets the replacement-selection to its initial state (optional
	 * operation). So the caller is able to traverse the underlying iteration
	 * again.
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code> method
	 *         is not supported by the replacement-selection operator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input.reset();
		heap.close();
		init();
	}
}
