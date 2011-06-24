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

package xxl.core.cursors.visual;

import java.util.Iterator;

import xxl.core.util.concurrency.AsynchronousChannel;

/**
 * A controllable iterator wraps an iterator in order to enhance it by the
 * possibility to be controlled remotely. A thread is used to control the wrapped
 * iterator and command to the remotely controled iterator can be sent via an
 * asynchronous channel.
 * 
 * @see xxl.core.cursors.visual.Controllable
 * @see java.util.Iterator
 * @see java.lang.Thread
 * @see xxl.core.util.concurrency.AsynchronousChannel
 */
public class IteratorControllable implements Controllable {

	/**
	 * A constant identifying the state of a paused thread. It can be sent to the
	 * thread <tt>thread</tt> via the asynchronous channel <tt>channel</tt> in
	 * order to cause the thread to be paused.
	 */
	static final int PAUSE = 0;
	
	/**
	 * A constant identifying the state of a running thread. It can be sent to
	 * the thread <tt>thread</tt> via the asynchronous channel <tt>channel</tt>
	 * in order to cause the (paused) thread to be resumed.
	 */
	static final int RUN   = 1;
	
	/**
	 * A constant identifying the state of a thread that proceeds <tt>n</tt>
	 * steps of the wrapped iteration. It can be sent to the thread
	 * <tt>thread</tt> follow by the number of steps to be proceeded via the
	 * asynchronous channel <tt>channel</tt> in order to cause the (paused)
	 * thread to proceed the next <tt>n</tt> steps.
	 */
	static final int NEXT  = 2;

	/**
	 * The iterator to be controlled remotely.
	 */
	protected Iterator iterator;

	/**
	 * The thread controlling the wrapped iterator.
	 */
	protected Thread thread;

	/**
	 * An asynchronous channelused for the communication between the controll
	 * thread and this class.
	 */
	protected AsynchronousChannel channel;
	
	/**
	 * The element returned by the last call to the <tt>next</tt> method of the
	 * remotely controlled iterator.
	 */
	protected Object next;

	/**
	 * Creates a new controllable iterator that enhances the given iterator by
	 * the possibility to be controlled remotely.
	 * 
	 * @param iterator the iterator to be controlled remotely.
	 */
	public IteratorControllable(Iterator iterator) {
		this.iterator = iterator;
	}

	/**
	 * Initializes a controllable iterator and prepares for the start of its life
	 * cycle. In detail, a thread controlling the wrapped iterator and an
	 * asynchronous channel for communicating with this thread are created.
	 */
	public void init() {
		channel = new AsynchronousChannel();
		(
			thread = new Thread() {
				
				protected int status = PAUSE;
				
				protected void waitForCommand() {
					status = ((Integer)channel.take()).intValue();
				}
				
				protected boolean isCommandAvailable() {
					return !channel.isEmpty();
				}
	
				public void run() {
					while (iterator.hasNext())
						switch (status) {
							case PAUSE:
								waitForCommand();
								break;
							case RUN:
								if (isCommandAvailable())
									waitForCommand();
								else
									next = iterator.next();
								break;
							case NEXT:
								int number = ((Integer)channel.take()).intValue();
								do {
									next = iterator.next();
								}
								while (--number > 0 && iterator.hasNext());
								status = PAUSE;
								break;
						}
				}
			}
		).start();
	}

	/**
	 * Starts or resumes the life cycle of a controllable object. In detail, the
	 * status of the thread controlling the wrapped iterator is set to
	 * <tt>RUN</tt>.
	 */
	public void go() {
		(
			new Thread() {
				public void run (){
					channel.put(new Integer(RUN));
				}
			}
		).start();
	}

	/**
	 * Pauses the life cycle of a controllable object. In detail, the status of
	 * the thread controlling the wrapped iterator is set to <tt>PAUSE</tt>.
	 */
	public void pause() {
		(
			new Thread() {
				public void run() {
					channel.put(new Integer(PAUSE));
				}
			}
		).start();
	}

	/**
	 * Lets a controllable object perfom a given number of steps of its life
	 * cycle (optional operation). In detail, the status of the thread
	 * controlling the wrapped iterator is set to <tt>NEXT</tt> and the number
	 * of step the iterator should proceed is submitted.
	 * 
	 * @param steps the number of steps the controllable object will be allowed
	 *        to resume its life cycle.
	 * @throws UnsupportedOperationException if the <tt>go(int)</tt> operation is
	 *         not supported by the controllable object.
	 */
	public void go(final int steps) {
		(
			new Thread() {
				public void run() {
					channel.put(new Integer(NEXT));
					channel.put(new Integer(steps));
				}
			}
		).start();
	}

	/**
	 * Indicates whether the controllable object supports the <tt>go(int)</tt>
	 * operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         <tt>go(int)</tt> operation, otherwise <tt>false</tt>.
	 */
	public boolean supportsGoSteps() {
		return true;
	}

	/**
	 * Resets the controllable object to the beginning of its life cycle
	 * (optional operation). This operation is not supported
	 * 
	 * @throws UnsupportedOperationException if the <tt>reset</tt> operation is
	 *         not supported by the controllable object.
	 */
	public void reset() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("reset not supported!");
	}

	/**
	 * Indicates whether the controllable object supports the <tt>reset</tt>
	 * operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         <tt>reset</tt> operation, otherwise <tt>false</tt>.
	 */
	public boolean supportsReset() {
		return false;
	}
	
	/**
	 * Closes the controllable object and releases ressources collected during
	 * its life cycle (optional operation). This operation is not supported.
	 * 
	 * @throws UnsupportedOperationException if the <tt>close</tt> operation is
	 *         not supported by the controllable object.
	 */
	public void close() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("close not supported!");
	}

	/**
	 * Indicates whether the controllable object supports the <tt>close</tt>
	 * operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         <tt>close</tt> operation, otherwise <tt>false</tt>.
	 */
	public boolean supportsClose() {
		return false;
	}

}
