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

package xxl.core.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.wrappers.IteratorCursor;

/**
 * This class provides an input stream that depends on a given iterator.
 * The objects of the iteration must extend the abstract class
 * <tt>Number</tt> in order to be converted to <tt>int</tt> and returned
 * by the read method.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new list
 *
 *     List list = new ArrayList();
 *
 *     // add some integers to that list
 *
 *     list.add(new Integer(1));
 *     list.add(new Integer(10));
 *     list.add(new Integer(100));
 *     list.add(new Integer(1000));
 *     list.add(new Integer(10000));
 *     list.add(new Integer(100000));
 *     list.add(new Integer(1000000));
 *     list.add(new Integer(10000000));
 *
 *     // create a new iterator input stream
 *
 *     IteratorInputStream input = new IteratorInputStream(list.iterator());
 *
 *     // print all data of the iterator input stream
 *
 *     try {
 *         int i = 0;
 *         while ((i = input.read()) != -1)
 *             System.out.println(i);
 *     }
 *     catch (IOException ioe) {
 *         System.out.println("An I/O error occured.");
 *     }
 * </pre>
 *
 * @see Cursor
 * @see InputStream
 * @see IOException
 * @see Iterator
 * @see IteratorCursor
 */
public class IteratorInputStream extends InputStream {

	/**
	 * The underlying cursor contains the <tt>Number</tt> objects for the
	 * input stream.
	 */
	protected Cursor cursor;

	/**
	 * Constructs a new IteratorInputStream that depends on the specified
	 * cursor.
	 *
	 * @param cursor the cursor that contains the <tt>Number</tt> objects
	 *        for the input stream.
	 */
	public IteratorInputStream (Cursor cursor) {
		this.cursor = cursor;
	}

	/**
	 * Constructs a new IteratorInputStream that depends on the specified
	 * iterator. In order to add Cursor functionality to the given
	 * iterator it is wrapped by an IteratorCursor.
	 *
	 * @param iterator the iterator that contains the <tt>Number</tt>
	 *        objects for the input stream.
	 */
	public IteratorInputStream (Iterator iterator) {
		this(new IteratorCursor(iterator));
	}

	/**
	 * Reads the next byte of data from the input stream. The value byte
	 * is returned as an <tt>int</tt> in the range <tt>0</tt> to
	 * <tt>255</tt>. If no byte is available because the end of the stream
	 * has been reached, the value <tt>-1</tt> is returned.<br>
	 * This implementation takes the next object of the iteration and
	 * casts it to <tt>Number</tt>. Thereafter the last 8 bit (an
	 * <tt>int</tt> in the range <tt>0</tt> to <tt>255</tt>) of the
	 * <tt>int</tt> value of the <tt>Number</tt> object are returned.
	 *
	 * @return the next byte of data, or <tt>-1</tt> if the end of the
	 *         stream is reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read () throws IOException {
		return !cursor.hasNext() ? -1 : ((Number)cursor.next()).intValue() & 255;
	}

	/**
	 * Closes this input stream and releases any system resources
	 * associated with the stream. <br>
	 * This implementation calls the close method of the underlying
	 * cursor.
	 */
	public void close () {
		cursor.close();
	}
}
