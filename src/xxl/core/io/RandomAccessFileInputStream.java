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
import java.io.RandomAccessFile;

import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a mechanism to read data via an 
 * InputStream from a RandomAccessFile. The stream starts 
 * reading at the current position of the underlying file. 
 * A close of the stream does not close the file, so it
 * is still open for subsequent processing.
 */
public class RandomAccessFileInputStream extends InputStream {
	/**
	 * The internally used RandomAccessFile
	 */
	protected RandomAccessFile raf;
	
	/**
	 * Position set by the mark-operation (initially 0).
	 */
	protected long markPos;
	
	/**
	 * Constructs an InputStream that reads its data from a
	 * RandomAccessFile.
	 * @param raf The RandomAccessFile used for input.
	 */
	public RandomAccessFileInputStream(RandomAccessFile raf) {
		this.raf = raf;
		this.markPos = 0;
	}

	/**
	 * Reads a byte from the RandomAccessFile.
	 * @return the read byte. -1 means end of file.
	 * @throws IOException
	 */
	public int read() throws IOException {
		return raf.read();
	}

	/**
	 * Determines the number of bytes which can be read without
	 * blocking. Here, it is the number of bytes left after the
	 * current position in the file.
	 *
	 * @return available bytes
	 * @throws IOException
	 */
	public int available() throws IOException {
		long avail = raf.length()-raf.getFilePointer();
		return avail>Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) avail;
	}

	/**
	 * Writes a byte array into the RandomAccessFile.
	 * @param b byte array
	 * @param off position of the first byte in the array
	 *	that should be written.
	 * @param len length in bytes that should be written.
	 * @return number of bytes read. -1 means end of file.
	 * @throws IOException
	 */
	public int read(byte[] b,int off,int len) throws IOException {
		return raf.read(b,off,len);
	}

	/**
	 * This class supports the mark-operation.
	 * @return true
	 */
	public boolean markSupported() {
		return true;
	}

	/**
	 * Marks the position in the RandomAccessFile.
	 * @param readlimit has no effekt in this class
	 */
	public void mark(int readlimit) {
		try {
			markPos = raf.getFilePointer();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Resets the position inside the RandomAccessFile to a
	 * position that has been marked before (or to the beginning
	 * of the file if no position has been marked).
	 * @throws IOException
	 */
	public void reset() throws IOException {
		raf.seek(markPos);
	}

	/**
	 * Skips n bytes of the input stream (if possible).
	 * @param n number of bytes to be skipped.
	 * @return number of bytes which have really been skipped.
	 * @throws IOException
	 */
	public long skip(long n) throws IOException {
		return raf.skipBytes((int) n);
	}

	/**
	 * Disconnects the OutputStream. Further calls to the OutputStream 
	 * are not allowed.
	 */
	public void close() {
		raf = null;
	}
}
