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
import java.util.Stack;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides an implementation of the Queue interface with a
 * LIFO (<i>last in, first out</i>) strategy that internally uses a stack
 * to store its elements. It implements the <tt>peek</tt> method.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new stack queue
 *
 *     StackQueue&lt;Integer&gt; queue = new StackQueue&lt;Integer&gt;();
 *
 *     // open the queue
 *     
 *      queue.open();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Cursor&lt;Integer&gt; cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     for (; cursor.hasNext(); queue.enqueue(cursor.next()));
 *
 *     // print all elements of the queue
 * 
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *     
 *     System.out.println();
 *  
 *     // close open queue and cursor after use
 *
 *     queue.close();
 *     cursor.close();
 * </pre>
 *
 * Usage example (2).
 * <pre>
 *     // create an iteration over the Integer between 0 and 19
 *     
 *     cursor = new Enumerator(20);
 *
 *     // create a new stack queue that uses a new stack to store its
 *     // elements and that contains all elements of the given iterator
 *
 *     queue = new StackQueue&lt;Integer&gt;();
 *     Queues.enqueueAll(queue, cursor);
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
 *     // close open queue and cursor after use
 * 
 *     queue.close();
 *     cursor.close();
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.AbstractQueue
 * @see xxl.core.collections.queues.LIFOQueue
 * @see java.util.Stack
 */
public class StackQueue<E> extends AbstractQueue<E> implements LIFOQueue<E> {

	/**
	 * A factory method to create a new StackQueue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface Queue).
	 * It may be invoked with a <i>parameter list</i> (for
	 * further details see Function) of stacks, an iterator or without any
	 * parameters. A <i>parameter list</i> of stacks will be used
	 * to initialize the internally used stack with the stack at index 0
	 * and an iterator will be used to insert the contained elements into
	 * the new StackQueue.
	 *
	 * @see Function
	 */
	public static final Function<Object, StackQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object,StackQueue<Object>>() {
		@Override
		public StackQueue<Object> invoke() {
			return new StackQueue<Object>();
		}

		@Override
		public StackQueue<Object> invoke(Object iterator) {
			StackQueue<Object> queue = new StackQueue<Object>();
			for (Iterator i = (Iterator)iterator; i.hasNext(); queue.enqueue(i.next()));
			return queue;
		}

		@Override
		public StackQueue<Object> invoke(List<? extends Object> list) {
			return new StackQueue<Object>(
				(Stack<Object>)list.get(0)
			);
		}
	};


	/**
	 * The stack is internally used to store the elements of the queue.
	 */
	protected Stack<E> stack;

	/**
	 * Constructs a queue containing the elements of the stack. The
	 * specified stack is internally used to store the elements of the
	 * queue.
	 *
	 * @param stack the stack that is used to initialize the internally
	 *        used stack.
	 */
	public StackQueue(Stack<E> stack) {
		this.stack = stack;
	}

	/**
	 * Constructs an empty queue. This queue instantiates a new stack in
	 * order to store its elements.
	 */
	public StackQueue() {
		this(new Stack<E>());
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 */
	@Override
	protected void enqueueObject(E object) {
		stack.push(object);
	}

	/**
	 * Returns the <i>next</i> element in the queue <i>without</i> removing it.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	@Override
	protected E peekObject() {
		return stack.peek();
	}

	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	@Override
	protected E dequeueObject() {
		return stack.pop();
	}

	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.
	 */
	@Override
	public void clear () {
		stack.clear();
		size = 0;
	}
}
