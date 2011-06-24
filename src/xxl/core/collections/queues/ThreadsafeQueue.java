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
 * A ThreadsafeQueue can be used by different threads that
 * use this queue for communicating with each other.
 * If the queue is empty, the queue has to wait until
 * a different thread writes something into the queue.
 * This class can also be used as a bounded buffer!
 * <p>
 * This class is used by ThreadedIterator.</p>
 * <p>
 * Usage example (1).
 * <pre>
 *     // create a threadsafe array queue
 *
 *     Queue&lt;Integer&gt; queue = new ThreadsafeQueue&lt;Integer&gt;(new BoundedQueue&lt;Integer&gt;(new ArrayQueue&lt;Integer&gt;(),100));
 *
 *     // open the queue
 * 
 *     queue.open();
 *     
 *     // create an enumeration with 100 elements
 * 
 *     Cursor&lt;Integer&gt; cursor = new Enumerator(100);
 *
 *     // insert all elements in the queue
 * 
 *     for (; cursor.hasNext(); queue.enqueue(cursor.next()));
 * 
 *     System.out.println("There were "+queue.size()+" elements in the queue");
 *
 *     // close the queue and the cursor
 * 
 *     queue.close();
 *     cursor.close();
 * </pre>
 * 
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.DecoratorQueue
 */
public class ThreadsafeQueue<E> extends DecoratorQueue<E> {

	/** Counts the number of failed calls to the underlying queue because the queue was full. */
	public int failedCallsFull = 0;
	/** Counts the number of failed calls to the underlying queue because the queue was empty. */
	public int failedCallsEmpty = 0;
	
	/** Counts the number of threads which want to insert into the queue. */
	protected int countWaitForInsert = 0;
	/** Counts the number of threads which want to get an object out of the queue. */
	protected int countWaitForNext   = 0;
	
	/** Internal state constant. */
	protected static final int EMPTY=0;
	/** Internal state constant. */
	protected static final int NORMAL=1;
	/** Internal state constant. */
	protected static final int FULL=2;

	/** Internal state of the queue. */
	protected int state = EMPTY;

	/**
	 * Constructs a threadsafe Queue.
	 *
	 * @param queue the queue that becomes threadsafe.
	 */
	public ThreadsafeQueue(Queue<E> queue) {
		super(queue);
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 */
	public synchronized void enqueue(E object) {
		while (true) {
			if (state == FULL) {
				countWaitForInsert++;
				try {
					wait();
				}
				catch (java.lang.InterruptedException e) {
					e.printStackTrace();
				}
				countWaitForInsert--;
			}
			else {
				try {
					super.enqueue(object);
					// if (state == EMPTY)
					state=NORMAL;
					break;
				}
				catch (IndexOutOfBoundsException e) {
					failedCallsFull++;
					state = FULL;
				}
			}
		}
		if ((countWaitForNext > 0) || (countWaitForInsert > 0))
			notify();
	}

	/**
	 * Returns the <i>next</i> element in the queue and <i>removes</i> it. 
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	public synchronized E dequeue() {
		E object;
		while (true) {
			if (state == EMPTY) {
				countWaitForNext++;
				try  {
					wait();
				}
				catch (java.lang.InterruptedException e) {}
				countWaitForNext--;
			}
			else {
				try {
					if (queue.size() > 0) { // equivalent to if (hasNext()) {
						object = super.dequeue();
						//if (state == FULL)
						state = NORMAL;
						break;
					}
				}
				catch (NoSuchElementException e) {}
				// no element availlable here!
				failedCallsEmpty++;
				state = EMPTY;
			}
		}
		if ((countWaitForNext > 0) || (countWaitForInsert > 0))
			notify();

		return object;
	}
}
