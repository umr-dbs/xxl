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

import xxl.core.functions.Function;

/**
 * The interface LIFO queue represents a LIFO (<i>last in, first out</i>)
 * iteration over a collection of elements (also known as a
 * <i>sequence</i>) with a <tt>peek</tt> method. This interface predefines
 * a <i>LIFO strategy</i> for addition and removal of elements.
 * 
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 */
public interface LIFOQueue<E> extends Queue<E> {

	/**
	 * A factory method to create a default LIFO queue (see contract for
	 * {@link Queue#FACTORY_METHOD FACTORY_METHOD} in interface Queue).
	 * This field is set to
	 * <code>{@link StackQueue#FACTORY_METHOD StackQueue.FACTORY_METHOD}</code>.
	 */
	public static final Function FACTORY_METHOD = StackQueue.FACTORY_METHOD;
	
}
