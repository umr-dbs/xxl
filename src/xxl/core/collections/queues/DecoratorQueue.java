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

import xxl.core.functions.Function;
import xxl.core.util.Decorator;

/**
 * This class provides a decorator for a queue that follows the
 * <i>Decorator Design Pattern</i> (for further details see Structural
 * Patterns, Decorator in <i>Design Patterns: Elements of Reusable
 * Object-Oriented Software</i> by Erich Gamma, Richard Helm, Ralph
 * Johnson, and John Vlissides). It provides a more flexible way to add
 * functionality to a queue or one of its subclasses as by inheritance.<p>
 *
 * To provide this functionality the class contains a reference to the
 * queue to be decorated. This reference is used to redirect method calls
 * to the <i>original</i> queue. This class is an abstract class although
 * it provides no abstract methods. That's so because it does not make any
 * sense to instanciate this class which redirects every method call to
 * the corresponding method of the decorated queue without adding any
 * functionality.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new decorated queue that adds functionality to the enqueue method 
 *     // and leaves the other methods untouched
 *
 *     DecoratorQueue queue = new DecoratorQueue(new ListQueue()) {
 *         public void enqueue (Object object) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified object!");
 *             super.enqueue(object);
 *             System.out.println("After the insertion of the specified object!");
 *         }
 *     }
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 */
public abstract class DecoratorQueue<E> implements Queue<E>, Decorator<Queue<E>> {

	/**
	 * A factory method to create a new DecoratorQueue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface Queue). In
	 * contradiction to the contract in Queue it may only be invoked with
	 * an array (<i>parameter list</i>) (for further details see Function)
	 * of queues. The array (<i>parameter list</i>) will be used to
	 * initialize the decorated queue. This field is set to
	 * <code>{@link UnmodifiableQueue#FACTORY_METHOD UnmodifiableQueue.FACTORY_METHOD}</code>.
	 *
	 * @see Function
	 */
	public static final Function<Object, ? extends Queue<Object>> FACTORY_METHOD = UnmodifiableQueue.FACTORY_METHOD;

	/**
	 * A reference to the queue to be decorated. This reference is used to
	 * perform method calls on the <i>original</i> queue.
	 */
	protected Queue<E> queue;
	/**
	 * Internally used to store the state of this queue (if it is opened).
	 */
	protected boolean isOpened = false;
	/**
	 * Internally used to store the state of this queue (if it is closed).
	 */
	protected boolean isClosed = false;
	
	/**
	 * Constructs a new DecoratorQueue that decorates the specified queue.
	 *
	 * @param queue the queue to be decorated.
	 */
	public DecoratorQueue(Queue<E> queue) {
		this.queue = queue;
	}

	/**
	 * Opens the queue, i.e., signals the queue to reserve resources, open
	 * files, etc. Before a queue has been opened calls to methods like 
	 * <tt>peek</tt> are not guaranteed to yield proper results.
	 * Therefore <tt>open</tt> must be called before a queue's data
	 * can be processed. Multiple calls to <tt>open</tt> do not have any effect,
	 * i.e., if <tt>open</tt> was called the queue remains in the state
	 * <i>opened</i> until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed queue
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (!isOpened) {
			isOpened = true;
			queue.open();
		}
	}
	
	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method take the same effect as a single call.<br>
	 * <b>Note:</b> This method is very important for queues using
	 * external resources like files or JDBC resources.
	 */
	public void close() {
		if (!isClosed) {
			isClosed = true;
			queue.close();
		}
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.
	 * 
	 * @param object element to be appended at the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 */
	public void enqueue(E object) throws IllegalStateException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		queue.enqueue(object);
	}

	/**
	 * Returns the <i>next</i> element in the queue <i>without</i> removing it.
	 * The <i>next</i> element of the queue is given by its
	 * <i>strategy</i>.
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public E peek() throws IllegalStateException, NoSuchElementException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		return queue.peek();
	}
	
	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it. 
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public E dequeue() throws IllegalStateException, NoSuchElementException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		return queue.dequeue();
	}

	/**
	 * Returns <tt>false</tt> if the queue has more elements (in other
	 * words, returns <tt>false</tt> if <tt>next</tt> would return an
	 * element rather than throwing an exception).
	 * 
	 * @return <tt>false</tt> if the queue has more elements.
	 */
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	/**
	 * Returns the number of elements in this queue. If this queue
	 * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
	 * <tt>Integer.MAX_VALUE</tt>.
	 * 
	 * @return the number of elements in this queue.
	 */
	public int size() {
		return queue.size();
	}

	/**
	 * Removes all of the elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * Note that the elements will only be removed from the queue but not
	 * from the underlying collection.
	 */
	public void clear() {
		queue.clear();
	}
	
	@Override
	public Queue<E> getDecoree() {
		return queue;
	}

}
