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

package xxl.core.cursors.wrappers;

import java.util.ConcurrentModificationException;

import xxl.core.collections.queues.Queue;
import xxl.core.cursors.AbstractCursor;

/**
 * A queue-cursor wraps a {@link xxl.core.collections.queues.Queue queue} to a
 * cursor, i.e., the wrapped queue can be accessed via the
 * {@link xxl.core.cursors.Cursor cursor} interface. All method calls are
 * passed to the underlying queue. Therefore the methods <code>remove</code>,
 * <code>update</code> and <code>reset</code> throw an
 * {@link java.lang.UnsupportedOperationException}.
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.collections.queues.Queue
 */
public class QueueCursor<E> extends AbstractCursor<E> {
	
	/**
	 * The internally used queue that is wrapped to a cursor.
	 */
	protected Queue<? extends E> queue;

	/**
	 * Creates a new queue-cursor.
	 *
	 * @param queue the queue to be wrapped to a cursor.
	 */
	public QueueCursor(Queue<? extends E> queue) {
		this.queue = queue;
	}

	/**
	 * Opens the queue-cursor, i.e., signals it to reserve resources, open the
	 * underlying queue, etc. Before a cursor has been opened calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Therefore <code>open</code> must be called before a
	 * cursor's data can be processed. Multiple calls to <code>open</code> do
	 * not have any effect, i.e., if <code>open</code> was called the cursor
	 * remains in the state <i>opened</i> until its <code>close</code> method
	 * is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened) return;
		super.open();
		queue.open();
	}

	/**
	 * Closes the queue-cursor, i.e., signals it to clean up resources, close
	 * the underlying queue, etc. When a cursor has been closed calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Multiple calls to <code>close</code> do not
	 * have any effect, i.e., if <code>close</code> was called the cursor
	 * remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed) return;
		super.close();
		queue.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the queue-cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return !queue.isEmpty();
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the queue-cursor's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 * @throws ConcurrentModificationException If the element returned by
	 *         <code>peek</code> would differ from that returned by a call to
	 *         <code>next</code>.
	 */
	protected E nextObject() {
		return queue.dequeue();
	}
	
	/**
	 * Returns the number of elements in this queue-cursor. If this
	 * queue-cursor contains more than <code>Integer.MAX_VALUE</code> elements,
	 * returns <code>Integer.MAX_VALUE</code>.
	 * 
	 * @return the number of elements in this queue-cursor.
	 */
	public int size() {
		return queue.size();
	}

	/**
	 * Returns the wrapped queue. Use with care: modifications to the wrapped
	 * queue can have undesired effects.
	 * 
	 * @return the queue wrapped by this QueueCursor.
	 * @since 1.1
	 */
	public Queue<? extends E> getQueue() {
		return queue;
	}
}
