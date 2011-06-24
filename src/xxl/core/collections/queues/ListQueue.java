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
import java.util.LinkedList;
import java.util.List;

import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides an implementation of the Queue interface with a
 * FIFO (<i>first in, first out</i>) strategy that internally uses a list
 * to store its elements. It implements the <tt>peek</tt> method.<p>
 * 
 * The performance of the queue depends on the performance of the
 * internally used list (e.g., an ArrayList guarantees for insertion in
 * amortized constant time, that is, adding n elements requires O(n)
 * time).<p>
 * 
 * Usage example (1).
 * <pre>
 *     // create a new list queue (that uses a linked list to store its elements per default)
 * 
 *     ListQueue&lt;Integer&gt; queue = new ListQueue&lt;Integer&gt;();
 *
 *     // open the queue
 *
 *     queue.open();
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
 *         System.out.println(queue.dequeue());
 *     System.out.println();
 *
 *     // close the open queue and cursor after use
 *     
 *     queue.close();
 *     cursor.close();
 * </pre>
 * 
 * Usage example (2).
 * <pre>
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 * 
 *     // create a new list queue that uses an array list to store its
 *     // elements and that contains all elements of the given iterator
 *
 *     queue = new ListQueue&lt;Integer&gt;(new java.util.ArrayList&lt;Integer&gt;());
 *     
 *     // open the queue
 *   
 *     queue.open();
 *     
 *     // insert all elements into the queue
 * 
 *     for (; cursor.hasNext(); queue.enqueue(cursor.next()));
 *
 *     // print all elements of the queue
 *     
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *     System.out.println();
 *
 *     // close the open queue and cursor after use
 *
 *     queue.close();
 *     cursor.close();
 * </pre>
 * 
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.AbstractQueue
 * @see xxl.core.collections.queues.FIFOQueue
 */
public class ListQueue<E> extends AbstractQueue<E> implements FIFOQueue<E> {
	
	/**
	 * A factory method to create a new ListQueue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface Queue).
	 * It may be invoked with a <i>parameter list</i> (for
	 * further details see Function) of lists, an iterator or without any
	 * parameters. A <i>parameter list</i> of lists will be used
	 * to initialize the internally used list with the list at index 0 and
	 * an iterator will be used to insert the contained elements into the
	 * new ListQueue.
	 * 
	 * @see Function
	 */
	public static final Function<Object, ListQueue<Object>> FACTORY_METHOD = new AbstractFunction<Object, ListQueue<Object>>() {
		@Override
		public ListQueue<Object> invoke() {
			return new ListQueue<Object>();
		}
		
		@Override
		public ListQueue<Object> invoke(Object iterator) {
			return new ListQueue<Object>(
				Cursors.toList((Iterator<Object>)iterator)
			);
		}
		
		@Override
		public ListQueue<Object> invoke(List<? extends Object> list) {
			return new ListQueue<Object>(
				(List<Object>)list.get(0)
			);
		}
	};
	
	/**
	 * The list is internally used to store the elements of the queue.
	 */
	protected List<E> list;
	
	/**
	 * Constructs a queue containing the elements of the list. The
	 * specified list is internally used to store the elements of the
	 * queue.
	 * 
	 * @param list the list that is used to initialize the internally used
	 *        list.
	 */
	public ListQueue(List<E> list) {
		this.list = list;
		this.size = list.size();
	}
	
	/**
	 * Constructs an empty queue. This queue instantiates a new LinkedList
	 * in order to store its elements.
	 */
	public ListQueue() {
		this(new LinkedList<E>());
	}
	
	/**
	 * Appends the specified element to the <i>end</i> of this queue.
	 * 
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 */
	@Override
	protected void enqueueObject(E object) {
		list.add(object);
	}
	
	/**
	 * Returns the <i>next</i> element in the queue <i>without</i> removing it.
	 * 
	 * @return the <i>next</i> element in the queue.
	 */
	@Override
	protected E peekObject() {
		return list.get(0);
	}
	
	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it.
	 * 
	 * @return the <i>next</i> element in the queue.
	 */
	@Override
	protected E dequeueObject() {
		return list.remove(0);
	}
	
	/**
	 * Removes all of the elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * Note that the elements will only be removed from the queue but not
	 * from the underlying collection.
	 */
	@Override
	public void clear() {
		list.clear();
		size = 0;
	}
}
