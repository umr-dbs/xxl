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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;

/**
 * A hash grouper partitions input data into groups (strict evaluation). When
 * the hash grouper is initialized a new {@link java.util.HashMap hash-map} is
 * created. The input data is partitioned concerning this hash-map by invoking
 * the user defined, unary function on the given iteration's elements. The hash
 * grouper calls the <code>next</code> method on the input iteration until the
 * <code>hasNext</code> method returns <code>false</code>, i.e., the iteration
 * is completely consumed, so a strict evaluation is performed. Every entry in
 * the hash-map consists of a value and a linked list where the iteration's
 * elements are added to. This means that the hash grouper partitions the data
 * physically. A function is used to compute the hash value of an object. With
 * regard to this value a bucket in the hash-map is assigned, i.e., a new group
 * starts.
 * 
 * <p>A call to the <code>next</code> method returns a cursor pointing to the
 * next group (the next bucket in the hash-map).</p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *   HashGrouper&lt;Integer&gt; hashGrouper = new HashGrouper&lt;Integer&gt;(
 *       new xxl.core.cursors.sources.Enumerator(21),
 *       new Function&lt;Integer, Integer&gt;() {
 *           public Integer invoke(Integer next) {
 *               return next % 5;
 *           }
 *       }
 *   );
 *   
 *   hashGrouper.open();
 *   
 *   while (hashGrouper.hasNext()) {
 *       Cursor&lt;Integer&gt; bucket = hashGrouper.next();
 *       // a cursor pointing to next group
 *       while (bucket.hasNext())
 *           System.out.print(bucket.next() + "; ");
 *       System.out.flush();
 *       System.out.println();
 *   }
 *   
 *   hashGrouper.close();
 * </pre></code>
 * This example creates a new hash grouper by applying the function to the
 * objects of an enumerator. So a hash-map is created with the values 0,...,4
 * and their corresponding buckets. Every entry in this hash-map consists of a
 * value and a linked list (bucket) where the iteration's elements are added
 * to. The implementation of the <code>init</code> method is as follows:
 * <code><pre>
 *   HashMap&lt;Object, List&lt;E&gt;&gt; hashMap = new HashMap&lt;Object, List&lt;E&gt;&gt;();
 *   input.open();
 *   while (input.hasNext()) {
 *       E object = input.next();
 *       Object value = function.invoke(object);
 *       List&lt;E&gt; list = hashMap.get(value);
 *       if (list == null)
 *           hashMap.put(value, list = new LinkedList&lt;E&gt;());
 *       list.add(object);
 *   }
 *   groups = hashMap.values().iterator();
 * </pre></code>
 * A collection is returned by applying the method <code>values</code> on the
 * hash map. This collection contains references to all linked lists (buckets)
 * used in the hash-map. The method <code>iterator</code> invoked on this
 * collection returns the elements as an iterator, namely the result-iteration
 * <code>groups</code>.<br />
 * In the use case shown above, this iteration is printed to the output stream,
 * i.e., the elements contained in the buckets were printed out. The generated
 * output is as follows:
 * <pre>
 *     4; 9; 14; 19;
 *     3; 8; 13; 18;
 *     2; 7; 12; 17;
 *     1; 6; 11; 16;
 *     0; 5; 10; 15; 20;
 * </pre>
 * The partitions concerning the function (object modulo 5) are represented
 * correctly.
 *
 * @param <E> the type of the elements returned by the input iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.functions.Function
 * @see java.util.HashMap
 * @see xxl.core.cursors.groupers.NestedLoopsGrouper
 * @see xxl.core.cursors.groupers.SortBasedGrouper
 */
public class HashGrouper<E> extends AbstractCursor<Cursor<E>> {

	/**
	 * The given input cursor providing the data to be partitioned.
	 */
	protected Cursor<? extends E> input;

	/**
	 * The function used to partition the elements (hash function).
	 */
	protected Function<? super E, ? extends Object> function;

	/**
	 * An iterator pointing to the resulting groups.
	 */
	protected Iterator<List<E>> groups;

	/**
	 * Creates a new hash grouper backed on an iteration. If an iterator is
	 * given to this constructor it is wrapped to a cursor.
	 *
	 * @param iterator the input iteration delivering the elements to be
	 *        partitioned.
	 * @param function the unary function returning a (hash-)value for each
	 *        element.
	 */
	public HashGrouper(Iterator<? extends E> iterator, Function<? super E, ? extends Object> function) {
		this.input = Cursors.wrap(iterator);
		this.function = function;
	}

	/**
	 * Creates a new hash grouper using the identity function to partition the
	 * elements. If an iterator is given to this constructor it is wrapped to a
	 * cursor.
	 *
	 * @param iterator the input iteration delivering the elements to be
	 *        partitioned.
	 * @see xxl.core.functions.Identity
	 */
	public HashGrouper(Iterator<? extends E> iterator) {
		this(iterator, new Identity<E>());
	}

	/**
	 * Initializes the hash grouper by creating a new
	 * {@link java.util.HashMap hash-map}. Therefore this method partitions the
	 * input data concerning this hash-map by invoking the user defined, unary
	 * function on the iteration's elements. Every entry in this hash-map
	 * consists of a key (hash-value) and a linked list where the iteration's
	 * elements are added to.
	 * 
	 * <p>The implementation of this method is as follows:
	 * <code><pre>
	 *   HashMap&lt;Object, List&lt;E&gt;&gt; hashMap = new HashMap&lt;Object, List&lt;E&gt;&gt;();
	 *   input.open();
	 *   while (input.hasNext()) {
	 *       E object = input.next();
	 *       Object value = function.invoke(object);
	 *       List&lt;E&gt; list = hashMap.get(value);
	 *       if (list == null)
	 *           hashMap.put(value, list = new LinkedList&lt;E&gt;());
	 *       list.add(object);
	 *   }
	 *   groups = hashMap.values().iterator();
	 * </pre></code>
	 * A collection is returned by applying the method <tt>values</tt> on the
	 * hash-map. This collection contains references to all linked lists
	 * (buckets) used in the hash-map. The method <tt>iterator</tt> invoked on
	 * this collection returns the elements as an iterator, namely the
	 * result-iteration <tt>groups</tt>.
	 */
	protected void init() {
		HashMap<Object, List<E>> hashMap = new HashMap<Object, List<E>>();
		input.open();
		while (input.hasNext()) {
			E object = input.next();
			Object value = function.invoke(object);
			List<E> list = hashMap.get(value);
			if (list == null)
				hashMap.put(value, list = new LinkedList<E>());
			list.add(object);
		}
		groups = hashMap.values().iterator();
	}

	/**
	 * Opens the hash grouper, i.e., signals the cursor to reserve resources
	 * and consume the input iteration to compute its partitions. Before a
	 * cursor has been opened calls to methods like <code>next</code> or
	 * <code>peek</code> are not guaranteed to yield proper results. Therefore
	 * <code>open</code> must be called before a cursor's data can be
	 * processed. Multiple calls to <code>open</code> do not have any effect,
	 * i.e., if <code>open</code> was called the cursor remains in the state
	 * <i>opened</i> until its <code>close</code> method is called.
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
	 * Closes the hash grouper, i.e., signals the cursor to clean up resources,
	 * close the input iteration, etc. When a cursor has been closed calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Multiple calls to <code>close</code> do not
	 * have any effect, i.e., if <code>close</code> was called the cursor
	 * remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed) return;
		super.close();
		input.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.) This happens when the result-iteration <code>groups</code>
	 * has more elements.
	 *
	 * @return <code>true</code> if the hash grouper has more elements,
	 *         otherwise <code>false</code>.
	 */
	protected boolean hasNextObject() {
		return groups.hasNext();
	}

	/**
	 * Returns the next element in the iteration, in this case a cursor. A new
	 * {@link xxl.core.cursors.wrappers.IteratorCursor iterator} cursor is
	 * returned by calling the <code>next</code> method on the result-iteration
	 * <code>groups</code>.
	 * 
	 * <p>More detailed: The <code>next</code> method invoked on the
	 * result-iteration <code>groups</code> returns a reference to a list,
	 * because the result-iteration consists of references pointing to lists.
	 * The method <code>iterator</code> is applied on this list and the
	 * returned iterator instance is wrapped to a cursor.
	 *
	 * @return the next element in the iteration.
	 */
	protected Cursor<E> nextObject() {
		return new IteratorCursor<E>(groups.next().iterator());
	}

	/**
	 * Resets the hash grouper to its initial state (optional operation). That
	 * means the <code>reset</code> method of the input iteration and the
	 * <code>init</code> method of the hash grouper are called. So the caller
	 * is able to traverse the partitions again.
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code> method
	 *         is not supported by the input iteration.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input.reset();
		init();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the hash grouper. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the hash grouper, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input.supportsReset();
	}
}
