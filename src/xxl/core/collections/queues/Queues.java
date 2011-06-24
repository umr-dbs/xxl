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

/**
 * This class contains various <code>static</code> methods for manipulating
 * queues.
 */
public abstract class Queues {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Queues() {}

	/**
	 * Invokes <code>queue.enqueue(Object o)</code> on all objects
	 * <code>o</code> contained in the iterator.
	 * 
	 * @param <E> the type of the elements stored by the queue.
	 * @param queue the queue to insert the elements into.
	 * @param iterator iterator containing the elements to insert.
     */
	public static <E> void enqueueAll(Queue<? super E> queue, Iterator<? extends E> iterator) {
		for (; iterator.hasNext(); queue.enqueue(iterator.next()));
	}
	
	/**
	 * Invokes <code>queue.enqueue(Object o)</code> on all objects
	 * <code>o</code> from the array <code>objects</code>.
	 * 
	 * @param <E> the type of the elements stored by the queue.
	 * @param queue the queue to insert the elements into.
	 * @param objects array containing the elements to insert.
	 */
	public static <E> void enqueueAll(Queue<? super E> queue, E... objects) {
		for (E object : objects)
			queue.enqueue(object);
	}

	/**
	 * Converts a queue to an object array whose length is equal to the number
	 * of the queue's elements.
	 * 
	 * @param queue the input queue.
	 * @return an object array containing the queue's elements.
	 */
	public static Object[] toArray(Queue<Object> queue) {
		return toArray(queue, new Object[queue.size()]);
	}

	/**
	 * Converts a queue to an object array. If the queue contains more elements
	 * than the array is able to store, the remaining elements stay into the
	 * queue.
	 * 
	 * @param <E> the type of the elements stored by the queue.
	 * @param queue the input queue.
	 * @param array the array to be filled.
	 * @return the filled object array.
	*/
	public static <E> E[] toArray(Queue<? extends E> queue, E[] array) {
		for (int i = 0; !queue.isEmpty() && i < array.length; array[i++] = queue.dequeue());
		return array;
	}

}
