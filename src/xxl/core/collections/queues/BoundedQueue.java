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

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * Decorates an arbitrary queue by adding boundary conditions. <br>
 * These conditions are represented as a user-defined, unary predicate
 * which is applied to next element to be inserted into the underlying queue.
 * If an insert operation detects an overflow, i.e., the predicate returns
 * <tt>false</tt>, the method <code>handleOverflow()</code> is called.
 * The default implementation
 * throws an {@link java.lang.IndexOutOfBoundsException IndexOutOfBoundsException},
 * but overwriting this method offers the possibility to handle an overflow of
 * the underlying queue differently.
 * If the predicate returns <tt>false</tt>, the element will be inserted
 * into the underlying queue. <br>
 *
 * For the construction of a bounded buffer, this class has
 * proven to be useful.
 * <p>
 * This class is used in ThreadedIterator.
 * <p>
 * 
 * Example usage (1).
 * <pre>
 *     // create a new bounded queue using an ArrayQueue
 * 
 *     Queue&lt;Integer&gt; queue = new BoundedQueue&lt;Integer&gt;(new ArrayQueue&lt;Integer&gt;(), 100);
 *
 *     // open the queue
 *
 *     queue.open();
 *
 *     try {
 *         Iterator&lt;Integer&gt; iterator = new xxl.core.cursors.sources.Enumerator(200);
 *         for (; iterator.hasNext(); queue.enqueue(iterator.next()));
 *     }
 *     catch (IndexOutOfBoundsException e) {}
 *
 *     int count = queue.size();
 *     System.out.println("The queue contained "+count+
 *                        " elements (100 is correct!)");
 *
 *      // close the queue
 *
 *      queue.close();
 *</pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.DecoratorQueue
 */
public class BoundedQueue<E> extends DecoratorQueue<E> {

	/**
	 * A factory method to create a new bounded queue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface
	 * Queue). It may be invoked with a <i>parameter list</i>
	 * (for further details see Function) of object arrays, an iterator or
	 * without any parameters. A <i>parameter list</i> of object
	 * arrays will be used to initialize the internally used array with
	 * the object array at index 0 and an iterator will be used to insert
	 * the contained elements into the new ArrayQueue.
	 * 
	 * @see Function
	 */
	public static final Function<Object, BoundedQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object, BoundedQueue<Object>>() {
		@Override
		public BoundedQueue<Object> invoke() {
			return new BoundedQueue<Object>(
				Queue.FACTORY_METHOD.invoke(),
				1024
			);
		}
		
		@Override
		public BoundedQueue<Object> invoke(Object iterator) {
			BoundedQueue<Object> queue = new BoundedQueue<Object>(
				Queue.FACTORY_METHOD.invoke(),
				1024
			);
			for (Iterator<?> i = (Iterator<?>)iterator; i.hasNext(); queue.enqueue(i.next()));
			return queue;
		}
		
		@Override
		public BoundedQueue<Object> invoke(List<? extends Object> list) {
			return new BoundedQueue<Object>(
				(Queue<Object>)list.get(0),
				(Predicate<Object>)list.get(1)
			);
		}
	};
	
	/**
	 * Predicate that evaluates if a further element can be inserted.
	 * Unary predicate that gets the next element to be inserted
	 * as parameter. If <tt>false</tt> is returned, an overflow
	 * must be handled, so the method <code>handleOverflow()</code>
	 * is called. Otherwise (<tt>true</tt> is returned), the element
	 * specified as argument will be inserted into the queue.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * Constructs a BoundedQueue. The queue to become bounded must be passed
	 * as the first argument. The second argument is a predicate evaluating
	 * the next element and determining if an overflow must be handled.
	 *
	 * @param queue the queue that becomes bounded.
	 * @param predicate the predicate evaluating the next element and
	 * 		determining if an overflow must be handled, i.e. handleOverflow()
	 * 		will be called, if the predicate returns <tt>false</tt>.
	 */
	public BoundedQueue(Queue<E> queue, Predicate<? super E> predicate) {
		super(queue);
		this.predicate = predicate;
	}

	/**
	 * Constructs a BoundedQueue. The queue to become bounded must be passed as
	 * the first argument. The second argument defines the size of the queue. 
	 * 
	 * @param queue the queue that becomes bounded. 
	 * @param maxSize the size of the queue. If more elements become
	 *			inserted method handleOverflow() is called.
	 */
	public BoundedQueue(final Queue<E> queue, final int maxSize) {
		this(
			queue,
			new AbstractPredicate<E>() {
				@Override
				public boolean invoke(E object) {
					return queue.size() < maxSize;
				}
			}
		);
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws IndexOutOfBoundsException if the overflow cannot be handled.
	 */
	@Override
	public void enqueue(E object) throws IllegalStateException, IndexOutOfBoundsException {
		if (predicate.invoke(object))
			super.enqueue(object);
		else
			handleOverflow();
	}

	/**
	 * If an insert operation detects an overflow, i.e., the maximum number
	 * of objects has been inserted into the queue, and a further element
	 * is to be inserted this method is called. The default implementation
	 * throws an {@link java.lang.IndexOutOfBoundsException IndexOutOfBoundsException},
	 * but overwriting this method offers the possibility to handle an overflow in 
	 * the queue differently.
	 * 
	 * @throws IndexOutOfBoundsException if the overflow cannot be handled.
	 */
	public void handleOverflow() throws IndexOutOfBoundsException {
		throw new IndexOutOfBoundsException("The maximum number of elements has been reached. "+
											"No further elements can be inserted.");
	}
}
