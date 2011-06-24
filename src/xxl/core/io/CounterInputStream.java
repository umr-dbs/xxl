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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class provides a decorator for an input stream that counts the
 * number of bytes that are read from the underlying stream.
 */
public class CounterInputStream extends FilterInputStream {

	/**
	 * The int value counter stores the number of bytes read from the underlying
	 * input stream.
	 */
	protected long counter = 0;

	/**
	 * Creates a new counter input stream that counts the number of bytes read from
	 * the specified input stream.
	 *
	 * @param in the input stream which read bytes should be counted.
	 */
	public CounterInputStream (InputStream in) {
		super(in);
	}

	/**
	 * Reads the next byte of data from the underlying input stream. The value byte
	 * is returned as an int in the range 0 to 255. If no byte is available because
	 * the end of the stream has been reached, the value <tt>-1</tt> is returned.
	 * This method blocks until input data is available, the end of the stream is
	 * detected, or an exception is thrown.<br>
	 * The counter is increased only when the returned value is not equal to
	 * <tt>-1</tt>.
	 *
	 * @return the next byte of data, or <tt>-1</tt> if the end of the stream is
	 *         reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read () throws IOException {
		int ret = super.read();
		if (ret > 0)
			counter++;
		return ret;
	}

	/**
	 * Reads up to <tt>b.length</tt> bytes of data from the underlying input stream
	 * into an array of bytes. This method blocks until some input is available.<br>
	 * The counter in increased by the number of read bytes only when the returned
	 * value is greater than <tt>0</tt>.
	 *
	 * @param b the buffer into which the data is read.
	 * @return the total number of bytes read into the buffer, or <tt>-1</tt> if
	 *         there is no more data because the end of the stream has been
	 *         reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read (byte[] b) throws IOException {
		int ret = super.read(b);
		if (ret > 0)
			counter += ret;
		return ret;
	}

	/**
	 * Reads up to <tt>len</tt> bytes of data from the underlying input stream into
	 * an array of bytes. This method blocks until some input is available.<br>
	 * The counter in increased by the number of read bytes only when the returned
	 * value is greater than <tt>0</tt>.
	 *
	 * @param b the buffer into which the data is read.
	 * @param off the start offset of the data.
	 * @param len the maximum number of bytes read.
	 * @return the total number of bytes read into the buffer, or <tt>-1</tt> if
	 *          there is no more data because the end of the stream has been
	 *          reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read (byte [] b, int off, int len) throws IOException {
		int ret = super.read(b, off, len);
		if (ret > 0)
			counter += ret;
		return ret;
	}

	/**
	 * Returns the number of bytes that are read from the underlying input stream.
	 *
	 * @return the number of bytes that are read from the underlying input stream.
	 */
	public long getCounter () {
		return counter;
	}

	/**
	 * Resets the counter to <tt>0</tt>.
	 */
	public void resetCounter () {
		counter = 0;
	}

	/**
	 * Returns a string representation of the counter inside.
	 * @return string representation
	 */
	public String toString() {
		return "number of bytes transfered (input stream): "+counter;
	}
}
