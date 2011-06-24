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

package xxl.core.collections.queues;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * The interface queue represents an user specified iteration over a collection
 * of elements (also known as a <i>sequence</i>) with a <code>peek</code>
 * method. This interface <b>does not</b> predefine any <i>strategy</i> for
 * addition and removal of elements so that the user has full control
 * concerning his <i>strategy</i> (e.g., FIFO (<i>first in, first out</i>),
 * LIFO (<i>last in, first out</i>) etc.).
 * 
 * <p>In contrast to sets, queues typically allow duplicate elements. More
 * formally, queues typically allow pairs of elements <code>e1</code> and
 * <code>e2</code> such that <code>e1.equals(e2)</code>, and they typically
 * allow multiple null elements if they allow null elements at all.</p>
 * 
 * <p>It is important to see that the <code>peek</code> method only shows the
 * element which currently is the next element. The next call of
 * <code>dequeue</code> does not have to return the peeked element. This is a
 * difference to the semantic of <code>peek</code> inside the
 * <code>Cursor</code> interface.</p>
 * 
 * @param <E> the type of the elements of this queue.
 */
public interface Queue<E> {

	/**
	 * A factory method to create a default queue. Each concrete implementation
	 * of a queue except for ArrayQueue should have a function FACTORY_METHOD
	 * that implements three variants of <code>invoke</code>
	 * <ul>
	 * <dl>
	 * <dt><li><code>Queue invoke()</code>:</dt>
	 * <dd>returns <code>new Queue()</code>.</dd>
	 * <dt><li><code>Queue invoke(Object iterator)</code>:</dt>
	 * <dd>returns <code>new Queue(iterator)</code>.</dd>
	 * <dt><li><code>Queue invoke(List<? extends Object> internalDataStructure)</code>:</dt>
	 * <dd>returns <code>new Queue((&lt;<i>InternalDataStructure&gt;</i>)internalDataStructure.get(0))</code>.</dd>
	 * </dl>
	 * </ul>
	 * This factory method creates a new ArrayQueue. It may be invoked with a
	 * <i>parameter list</i> (for further details see {@link Function}) of
	 * object arrays, an iterator or without any parameters. A <i>parameter
	 * list</i> of object arrays will be used to initialize the internally used
	 * array with the object array at index 0 and an iterator will be used to
	 * insert the contained elements into the new ArrayQueue.
	 * 
	 * @see Function
	 */
	public static final Function<Object, ArrayQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object, ArrayQueue<Object>>() {
		@Override
		public ArrayQueue<Object> invoke() {
			return new ArrayQueue<Object>();
		}
		
		@Override
		public ArrayQueue<Object> invoke(Object iterator) {
			return new ArrayQueue<Object>(
				Cursors.toArray((Iterator<?>)iterator)
			);
		}
		
		@Override
		public ArrayQueue<Object> invoke(List<? extends Object> list) {
			return new ArrayQueue<Object>((Object[])list.get(0));
		}
	};
	
	/**
	 * Opens the queue, i.e., signals the queue to reserve resources, open
	 * files, etc. Before a queue has been opened calls to methods like 
	 * <code>peek</code> are not guaranteed to yield proper results. Therefore
	 * <code>open</code> must be called before a queue's data can be processed.
	 * Multiple calls to <code>open</code> do not have any effect, i.e., if
	 * <code>open</code> was called the queue remains in the state
	 * <i>opened</i> until its <code>close</code> method is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed queue
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public abstract void open();

	/**
	 * Closes this queue and releases any system resources associated with it.
	 * This operation is idempotent, i.e., multiple calls of this method take
	 * the same effect as a single call.<br />
	 * <b>Note</b> that this method is very important for queues using external
	 * resources like files or JDBC resources.
	 */
	public abstract void close();

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.
	 * 
	 * @param object element to be appended at the <i>end</i> of this queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 */
	public abstract void enqueue(E object) throws IllegalStateException;
	
	/**
	 * Returns the element which currently is the <i>next</i> element in the
	 * queue. The element is <i>not</i> removed from the queue (in contrast to
	 * the method <code>dequeue</code>). The <i>next</i> element of the queue
	 * is given by its <i>strategy</i>. The next call to <code>dequeue</code>
	 * does not have to return the element which was returned by
	 * <code>peek</code>.
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public abstract E peek() throws IllegalStateException, NoSuchElementException;
	
	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it. The
	 * <i>next</i> element of the queue is given by its <i>strategy</i>.
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public abstract E dequeue() throws IllegalStateException, NoSuchElementException;
	
	/**
	 * Returns <code>false</code> if the queue has more elements (in other
	 * words, returns <code>false</code> if <code>next</code> would return an
	 * element rather than throwing an exception).
	 * 
	 * @return <code>false</code> if the queue has more elements.
	 */
	public abstract boolean isEmpty();
	
	/**
	 * Returns the number of elements in this queue. If this queue contains
	 * more than <code>Integer.MAX_VALUE</code> elements, returns
	 * <code>Integer.MAX_VALUE</code>.
	 * 
	 * @return the number of elements in this queue.
	 */
	public abstract int size();
	
	/**
	 * Removes all of the elements from this queue. The queue will be empty
	 * after this call returns so that <code>size() == 0</code>.<br />
	 * <b>Note</b> that the elements will only be removed from the queue but
	 * not from the underlying collection.
	 */
	public abstract void clear();

}
