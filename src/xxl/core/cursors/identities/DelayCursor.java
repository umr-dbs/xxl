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

package xxl.core.cursors.identities;

import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.cursors.sources.Repeater;
import xxl.core.cursors.unions.Sequentializer;

/**
 * This class holds the values of the underlying input iteration for a certain
 * amount of time before returning the object. Not all operations are delayed:
 * only next, peek, remove and update. This class is very useful for tests in a
 * multithreaded environment.
 * 
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see java.lang.Thread
 */
public class DelayCursor extends SecureDecoratorCursor {

	/**
	 * An iterator storing the time to wait for the single method calls.
	 */
	protected Iterator waitTime;
	
	/**
	 * A boolean flag indicating if the delay cursor waits at calls to the
	 * <tt>peek</tt> method.
	 */
	protected boolean waitAtPeek;

	/**
	 * Constructs a new delay cursor that delays the objects which flow between
	 * the underlying iteration and the calling code.
	 * 
	 * @param iterator the input iteration that should be delayed.
	 * @param waitTime an iteration containing integer objects which determine
	 *        (in milliseconds) how long the delay cursor waits before returning
	 *        the result. The iteration should have infinite size (if it is
	 *        consumed and the input iteration has more elements the time to wait
	 *        is set to 0 milliseconds). Therefore you can use the class
	 *        {@link xxl.core.cursors.sources.DiscreteRandomNumber} with size -1.
	 * @param waitAtPeek determines if the cursor does wait at calls to the
	 *        <tt>peek</tt> method or not.
	 */
	public DelayCursor(Iterator iterator, Iterator waitTime, boolean waitAtPeek) {
		super(iterator);
		this.waitTime = new Sequentializer(
			waitTime,
			new Repeater(new Integer(0))
		);
		this.waitAtPeek = waitAtPeek;
	}

	/**
	 * Constructs a new delay cursor that delays the objects which flow between
	 * the underlying iteration and the calling code.
	 * 
	 * @param iterator the input iteration that should be delayed.
	 * @param time the waiting time is always this value (in milliseconds).
	 * @param waitAtPeek determines if the cursor does wait at calls to the
	 *        <tt>peek</tt> method or not.
	 */
	public DelayCursor(Iterator iterator, int time, boolean waitAtPeek) {
		this(iterator, new Repeater(new Integer(time)), waitAtPeek);
	}

	/**
	 * Waits the amount of time specified by the next integer object returned by
	 * the iteration <tt>waitTime</tt>.
	 */
	protected void waiting() {
		try {
			Thread.sleep(((Integer)waitTime.next()).intValue());
		}
		catch (Exception e) {}
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public Object next() throws IllegalStateException, NoSuchElementException {
		waiting();
		return super.next();
	}

	/**
	 * Shows the next element in the iteration without proceeding the iteration
	 * (optional operation). After calling <tt>peek</tt> the returned element is
	 * still the cursor's next one such that a call to <tt>next</tt> would be
	 * the only way to proceed the iteration. But be aware that an
	 * implementation of this method uses a kind of buffer-strategy, therefore
	 * it is possible that the returned element will be removed from the
	 * <i>underlying</i> iteration, e.g., the caller can use an instance of a
	 * cursor depending on an iterator, so the next element returned by a call
	 * to <tt>peek</tt> will be removed from the underlying iterator which does
	 * not support the <tt>peek</tt> operation and therefore the iterator has to
	 * be wrapped and buffered.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors. After calling the <tt>peek</tt> method a call to <tt>next</tt>
	 * is strongly recommended.</p> 
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException iteration has no more elements.
	 * @throws UnsupportedOperationException if the <tt>peek</tt> operation is
	 *         not supported by the cursor.
	 */
	public Object peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		if (waitAtPeek)
			waiting();
		return super.peek();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and removes the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>remove</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>remove</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>remove</tt> operation is
	 *         not supported by the cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		waiting();
		super.remove();
	}
	
	/**
	 * Replaces the last element returned by the cursor in the underlying data
	 * structure (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and updates the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>update</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        cursor.
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>update</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>update</tt> operation is
	 *         not supported by the cursor.
	 */
	public void update(Object object) throws IllegalStateException, UnsupportedOperationException {
		waiting();
		super.update(object);
	}
}
