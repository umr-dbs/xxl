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

/**
 * This class provides a wrapper for a queue that performes a simple
 * <i>duplicate elimination</i>. When inserting a row duplicate elements,
 * this class' <tt>enqueue</tt> method only inserts the first element and ignors
 * the rest. Every time the insertion of duplicate elements is separated
 * by another element's insertion, the duplicate elements are not detected
 * by the <tt>enqueue</tt> method. This duplicate elimination is very important for
 * some operations like sorting etc. For example it can be used to
 * integrate an early duplicate elimination into a physical sort operation
 * instead of proceeding it on the sorted data. One example is the
 * <tt>SortMerge</tt> cursor of this library. The destinct queue can be used to
 * eliminate duplicates while merging the runs.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new queue
 * 
 *     Queue&lt;Integer&gt; q1 = new ListQueue&lt;Integer&gt;();
 *
 *     // open the queue
 *
 *     q1.open();
 *
 *     // create an iteration over 10000 random Integers (between 0 and
 *     // 100)
 * 
 *     Cursor&lt;Integer&gt; cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 10000);
 * 
 *     // insert all elements of the given cursor
 * 
 *     for (; cursor.hasNext(); q1.enqueue(cursor.next()));
 *     
 *     // print the size of the queue after insertion
 *		
 *     System.out.println("Size q1:\t" + q1.size());
 *
 *     // reset the cursor, so that it can be used again
 *     
 *     cursor.reset();
 *
 *     // create a new distinct queue
 *		
 *     Queue&lt;Integer&gt; q2 = new DistinctQueue&lt;Integer&gt;(new ListQueue&lt;Integer&gt;());
 *
 *     // opent the queue
 *
 *     q2.open();
 *     
 *     // insert all elements of the given cursor
 *
 *     for (; cursor.hasNext(); q2.enqueue(cursor.next()));
 *
 *     // print the size of the distinct queue after insertion
 *
 *     System.out.println("Size q2:\t" + q2.size());
 *
 *     // close the open queues and the cursor after use
 *
 *     q1.close();
 *     q2.close();
 *     cursor.close();
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.DecoratorQueue
 */
public class DistinctQueue<E> extends DecoratorQueue<E> {

	/**
	 * A factory method to create a new DistinctQueue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface Queue).
	 * It may be invoked with a <i>parameter list</i> (for
	 * further details see Function) of queues, an iterator or without any
	 * parameters. A <i>parameter list</i> of queues will be used
	 * to initialize the wrapped queue with the queue at index 0 and an
	 * iterator will be used to insert the contained elements into the
	 * new DistinctQueue.
	 * 
	 * @see Function
	 */
	public static final Function<Object, DistinctQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object, DistinctQueue<Object>>() {
		@Override
		public DistinctQueue<Object> invoke() {
			return new DistinctQueue<Object>(
				Queue.FACTORY_METHOD.invoke()
			);
		}

		@Override
		public DistinctQueue<Object> invoke(Object iterator) {
			DistinctQueue<Object> queue = new DistinctQueue<Object>(
				Queue.FACTORY_METHOD.invoke()
			);
			for (Iterator<?> i = (Iterator<?>)iterator; i.hasNext(); queue.enqueue(i.next()));
			return queue;
		}

		@Override
		public DistinctQueue<Object> invoke(List<? extends Object> list) {
			return new DistinctQueue<Object>(
				(Queue<Object>)list.get(0)
			);
		}
	};

	/**
	 * The field <tt>last</tt> is used to store the object that is
	 * inserted into the queue at last. When another element should be
	 * inserted into the queue, it is compared with <tt>last</tt>. When
	 * the elements are not equal, it is inserted into the queue and
	 * <tt>last</tt> is set to it.
	 */
	protected E last = null;

	/**
	 * Constructs a new DistintQueue that decorates the specified queue.
	 *
	 * @param queue the queue to be decorated.
	 */
	public DistinctQueue(Queue<E> queue) {
		super(queue);
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.<br>
	 * This method performs a simple <i>duplicate elimination</i>. When
	 * inserting a row duplicate elements, it only inserts the first
	 * element and ignores the rest. Every time the insertion of duplicate
	 * elements is separated by another element's insertion, the duplicate
	 * elements are not detected by this method.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 */
	@Override
	public void enqueue(E object) throws IllegalStateException {
		if (queue.size() == 0 || !object.equals(last))
			super.enqueue(last = object);
	}
	
}
