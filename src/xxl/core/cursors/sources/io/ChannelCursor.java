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

package xxl.core.cursors.sources.io;

import xxl.core.cursors.AbstractCursor;
import xxl.core.util.concurrency.Channel;

/**
 * This class provides a cursor backed on a
 * {@link xxl.core.util.concurrency.Channel channel}, i.e., 
 * the cursor listens to the given channel and waits for objects 
 * send through it. When a certain termination object is 
 * received (for example <tt>null</tt>), the cursor stops
 * listening and does not provide any more data.
 * <p>
 * After <tt>close</tt> is called, it should not happen, that the
 * channel becomes filled from the other side, because the filling
 * thread might become suspended.
 */
public class ChannelCursor extends AbstractCursor {

	/**
	 * The communication channel that is used for receiving the data of this
	 * iteration.
	 */
	protected Channel channel;

	/**
	 * The termination object that is used to signal the end of the iteration.
	 */
	protected Object termObject;

	/**
	 * Creates a new channel-cursor that returns the data received through the
	 * given channel.
	 * 
	 * @param channel the channel to which the channel-cursor is listening for
	 *        its data.
	 * @param termObject the object that is used to signal the end of the
	 *        iteration. When the channel is exhausted, i.e., no more data will
	 *        be sent through it, this object will be passed to the
	 *        channel-cursor via its channel.
	 */
	public ChannelCursor(Channel channel, Object termObject) {
		this.channel = channel;
		this.termObject = termObject;
	}

	/**
	 * Creates a new channel-cursor that returns the data received through the
	 * given channel. Until the channel-cursor receives a <tt>null</tt> object
	 * it listens to its channel.
	 * 
	 * @param channel the channel to which the channel-cursor is listening for
	 *        its data.
	 */
	public ChannelCursor(Channel channel) {
		this(channel, null);
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * @return <tt>true</tt> if the channel-cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return (next = channel.take()) != termObject;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the channel-cursor's methods, e.g., <tt>update</tt>
	 * or <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected Object nextObject() {
		return next;
	}

	/**
	 * Closes the cursor. This method must retrieve all other elements
	 * from the channel, so that no other thread is waiting.
	 */
	public void close() {
		if (isClosed) return;
		while (hasNext())
			next();
		super.close();
	}
}
