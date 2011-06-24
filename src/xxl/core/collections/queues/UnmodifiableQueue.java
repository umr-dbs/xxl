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

import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides a decorator for a given queue that cannot be
 * modified. The methods of this class call the corresponding methods of
 * the internally stored queue except the methods that modify the queue.
 * These methods (<tt>enqueue</tt> and <tt>clear</tt>) throw
 * an <tt>UnsupportedOperationException</tt>.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new queue
 *
 *     ListQueue&lt;Integer&gt; inputQueue = new ListQueue&lt;Integer&gt;();
 *
 *     // open the queue
 *
 *     inputQueue.open();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 * 
 *     Cursor&lt;Integer&gt; cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     for (; cursor.hasNext(); inputQueue.enqueue(cursor.next()));
 * 
 *     // create a new unmodifiable queue with the given queue
 *
 *     UnmodifiableQueue&lt;Integer&gt; queue = new UnmodifiableQueue&lt;Integer&gt;(inputQueue);
 * 
 *     // open the queue
 *
 *     queue.open();
 * 
 *     // print all elements of the queue
 *
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *
 *     System.out.println();
 *
 *     // close the open queue and cursor after use
 * 
 *     inputQueue.close();
 *     queue.close();
 *     cursor.close();
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.DecoratorQueue
 */
public class UnmodifiableQueue<E> extends DecoratorQueue<E> {

	/**
	 * A factory method to create a new unmodifiable queue (see contract
	 * for {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface
	 * Queue). In contradiction to the contract in Queue it may only be
	 * invoked without parameter or with a <i>parameter list</i> (for further details
	 * see Function) of queues. The <i>parameter list</i> will be
	 * used to initialize the decorated queue by calling the constructor
	 * <code>UnmodifiableQueue((Queue) array[0])</code>.
	 *
	 * @see Function
	 */
	public static final Function<Object, UnmodifiableQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object, UnmodifiableQueue<Object>>() {
		@Override
		public UnmodifiableQueue<Object> invoke() {
			return new UnmodifiableQueue<Object>(
				Queue.FACTORY_METHOD.invoke()
			);
		}
		
		@Override
		public UnmodifiableQueue<Object> invoke(List<? extends Object> list) {
			return new UnmodifiableQueue<Object>(
				(Queue<Object>)list.get(0)
			);
		}
	};

	/**
	 * Constructs a new unmodifiable queue that decorates the specified
	 * queue.
	 *
	 * @param queue the queue to be decorated.
	 */
	public UnmodifiableQueue(Queue<E> queue) {
		super(queue);
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>. This
	 * implementation always throws an
	 * <tt>UnsupportedOperationException</tt>.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 * @throws UnsupportedOperationException when the method is not
	 *         supported.
	 */
	@Override
	public void enqueue(E object) throws IllegalStateException, UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Removes all of the elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>. This
	 * implementation always throws an
	 * <tt>UnsupportedOperationException</tt>.
	 *
	 * @throws UnsupportedOperationException if the <tt>remove</tt>
	 *         operation is not supported by this queue.
	 */
	@Override
	public void clear() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}
}
