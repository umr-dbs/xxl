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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides a decorator for an output stream that counts the
 * number of bytes that are written to the underlying stream.
 */
public class CounterOutputStream extends FilterOutputStream{

	/**
	 * The int value counter stores the number of bytes written to the underlying
	 * output stream.
	 */
	protected long counter = 0;

	/**
	 * Creates a new counter output stream that counts the number of bytes written
	 * to the specified output stream.
	 *
	 * @param out the output stream which written bytes should be counted.
	 */
	public CounterOutputStream (OutputStream out) {
		super(out);
	}

	/**
	 * Writes the specified byte to the underlying output stream and increases the
	 * counter.
	 *
	 * @param b the byte to be written to the underlying output stream.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (int b) throws IOException {
		super.write(b);
		counter++;
	}

	/**
	 * Writes <tt>b.length</tt> bytes to the underlying output stream and increases
	 * the counter by this number of bytes.
	 *
	 * @param b the data to be written.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (byte [] b) throws IOException {
		super.write(b);
		counter += b.length;
	}

	/**
	 * Writes <tt>len</tt> bytes from the specified byte array starting at
	 * offset <tt>off</tt> to the underlying output stream and increases the
	 * counter by this number of bytes.
	 *
	 * @param b the data to be written.
	 * @param off the start offset in the data.
	 * @param len the number of bytes to write.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (byte [] b, int off, int len) throws IOException {
		super.write(b, off, len);
		counter += len;
	}

	/**
	 * Returns the number of bytes that are written to the underlying input stream.
	 *
	 * @return the number of bytes that are written to the underlying input stream.
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
		return "number of bytes transfered (output stream): "+counter;
	}
}
