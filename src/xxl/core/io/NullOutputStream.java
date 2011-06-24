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

import java.io.OutputStream;

/**
 * This class provides an output stream pointing to a NULL sink, i.e., all
 * output will be suppressed.
 * 
 * @see xxl.core.util.XXLSystem#NULL
 */
public class NullOutputStream extends OutputStream{

	/** Provides a ready-to-use {@link java.io.OutputStream OutputStream} representing
	 * a null device, i.e., a dummy sink.
	 *
	 * @see java.lang.System#out
	 * @see java.lang.System#err
	 */
	public static final OutputStream NULL = new NullOutputStream();

	/**
	 * Don't let anyone instantiate this class (singleton pattern). 
	 */
	private NullOutputStream(){}

	/**
	 * Closes the stream.
	 */
	public void close() {}

	/**
	 * Flushes the stream (not necessary to call here). 
	 */
	public void flush() {}

	/**
	 * Writes the byte array to the NullOutputStream.
	 * @param b
	 */
	public void write(byte[] b) {}

	/**
	 * Writes the byte array to the NullOutputStream.
	 * @param b the array
	 * @param off the offset
	 * @param len the number of bytes written.
	 */
	public void write(byte[] b, int off, int len) {}

	/**
	 * Writes the byte to the NullOutputStream.
	 * @param b the value of the byte.
	 */
	public void write(int b) {}
}
