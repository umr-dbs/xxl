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

import java.util.NoSuchElementException;

/**
 * This class provides a skeletal implementation of the Queue interface,
 * to minimize the effort required to implement this interface. <p>
 *
 * To implement a queue, the programmer only needs to extend this class
 * and provide implementations for the methods <tt>dequeueObject</tt>, 
 * <tt>enqueueObject</tt> and <tt>peekObject</tt>.<p>
 *
 * The documentation for each non-abstract method in this class describes
 * its implementation in detail. Each of these methods may be overridden
 * if the queue being implemented admits a more efficient implementation.
 * 
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 */
public abstract class AbstractQueue<E> implements Queue<E> {
	
	/**
	 * Internally used to store the state of this queue (if it is opened).
	 */
	protected boolean isOpened = false;
	/**
	 * Internally used to store the state of this queue (if it is closed).
	 */
	protected boolean isClosed = false;
	/**
	 * Internally used reference to the next element in this queue.
	 */
	protected E next = null;
	/**
	 * Internally used to remember whether the next element has already been 
	 * computed or not.
	 */
	protected boolean computedNext = false;
	/**
	 * Internally used to store the size (number of elements) of this queue. 
	 */
	protected int size = 0;
	
	/**
	 * Opens the queue, i.e., signals the queue to reserve resources, open
	 * files, etc. <br>
	 * Before a queue has been opened calls to methods like <tt>peek</tt> are 
	 * not guaranteed to yield proper results. Therefore <tt>open</tt> must be 
	 * called before a queue's data can be processed.<br>
	 * Multiple calls to <tt>open</tt> do not have any effect, i.e., if
	 * <tt>open</tt> was called the queue remains in the state <i>opened</i>
	 * until its <tt>close</tt> method is called. <br>
	 * Note that a call to the <tt>open</tt> method of a closed queue usually
	 * does not open it again because of the fact that its state generally
	 * cannot be restored when resources are released respectively files are
	 * closed.
	 */
	public void open() {
		if (!isOpened)
			isOpened = true;
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it (like closing files, cleaning up resources, etc.).<br>
	 * This operation is idempotent, i.e., multiple calls of this
	 * method take the same effect as a single call (which means that
	 * if <tt>close</tt> was called the queue remains in the state 
	 * <i>closed</i>).<br>
	 * <b>Note:</b> This method is very important for queues using
	 * external resources like files or JDBC resources.<br>
	 * This implementation is given by an empty body.
	 */
	public void close() {
		if (!isClosed) {
			clear();
			isClosed = true;
			computedNext = false;
		}
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.<br>
	 * This method calls <tt>enqueueObject(Object object)</tt> which
	 * has to implement the store procedure (depending on the
	 * <i>strategy</i> and the underlying datastructure).
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 */
	public final void enqueue(E object) throws IllegalStateException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		enqueueObject(object);
		computedNext = false;
		size++;
	}
	
	/**
	 * Abstract method which has to be overwritten by any class extending this 
	 * class.<br>
	 * This method is invoked by <tt>enque(Object object)</tt> and has to 
	 * implement the store procedure depending on the <i>strategy</i> and
	 * the used datastructure.
	 * @param object element to be appended to the <i>end</i> of this 
	 * 		  queue.
	 */
	protected abstract void enqueueObject(E object);

	/**
	 * Returns the <i>next</i> element in the queue without removing it.
	 * The <i>next</i> element of the queue is given by its
	 * <i>strategy</i>.<br>
	 * After calling <tt>peek</tt> the returned element is still the queue's
	 * next one such that a call to <tt>dequeue</tt> and then <tt>peek</tt> would 
	 * be the only way to access the next element.<br>
	 * This method internally calls <tt>peekObject</tt> which actually implements
	 * the operation.
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is closed.
	 * @throws NoSuchElementException if queue has no more elements.
	 */
	public final E peek() throws IllegalStateException, NoSuchElementException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		if (!computedNext) {
			if (size <= 0)
				throw new NoSuchElementException();
			next = peekObject();
			computedNext = true;
		}
		return next;
	}
	
	/**
	 * Returns the <i>next</i> element in the queue without removing it.
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.<br>
	 * This method is invoked by <tt>peek</tt> and has to implement the 
	 * peek procedure depending on the <i>strategy</i> and the used datastructure.<br>
	 * 
	 * @return the <i>next</i> element in the queue.
	 */
	protected abstract E peekObject();
	
	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it. 
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.
	 * This method internally calls <tt>dequeueObject</tt> which actually 
	 * implements the operation.
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public final E dequeue() throws IllegalStateException, NoSuchElementException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		if (size <= 0)
			throw new NoSuchElementException();
		next = dequeueObject();
		computedNext = false;
		size--;
		return next;
	}

	/**
	 * Returns the <i>next</i> element in the queue (and <i>removes</i> it).
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.<br>
	 * This method is invoked by <tt>dequeue</tt> and has to implement the 
	 * peek procedure depending on the <i>strategy</i> and the used datastructure.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	protected abstract E dequeueObject();

	/**
	 * Returns <tt>false</tt> if the queue has more elements. (In other
	 * words, returns <tt>false</tt> if <tt>peek</tt> would return an
	 * element rather than throwing an exception.)
	 * 
	 * @return false if the queue has more elements.
	 */
	public final boolean isEmpty() {
		return size == 0;
	} 
	
	/**
	 * Returns the number of elements in this queue. If this queue
	 * contains more than <tt>Integer.MAX_VALUE</tt> elements, 
	 * <tt>Integer.MAX_VALUE</tt> is returned.
	 * 
	 * @return the number of elements in this queue.
	 */
	public final int size() {
		return size;
	}
	
	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * Note that the elements will only be removed from the queue but not
	 * from the underlying collection.<br>
	 * This implementation iterates over this queue, calling the <tt>dequeue</tt>
	 * method for every element, in turn. 
	 */
	public void clear() {
		while (!isEmpty())
			dequeue();
	} 
}
