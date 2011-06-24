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

package xxl.core.util.concurrency;

/**
 * Implements a channel by using semaphores.
 * The concept of channels is well described in Doug Lea: "Concurrent
 * Programming in Java", Second Edition, Sun Microsystems. 
 * Online: http://java.sun.com/Series/docs/books/cp/
 */
public interface Channel {
	/** 
	 * Waits for an object that is sent from a different thread 
	 * via the put method.
	 *
	 * @return object from the channel
	 */
	public Object take();

	/** 
	 * Puts an object into the channel.
	 *
	 * @param object object that is put into the channel
	 */
	public void put(Object object);

	/**
	 * Tries to read an object from the channel. 
	 * If an object is not availlable, this method returns false.
	 * The object is returned inside the first element
	 * of the object array.
	 *
	 * @param o Object array. The first element is used for
	 *	returning the desired object
	 * @return true - iff there was an object in the channel
	 */
	public boolean attemptTake(Object o[]);

	/** 
	 * Determines if an object is currently in the channel.
	 *
	 * @return true if the channel currently does not contain any element
	 */
	public boolean isEmpty();

	/** 
	 * Determines if the channel currently is full.
	 *
	 * @return true if the channel is currently full
	 */
	public boolean isFull();
}
