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
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * This class provides a mechanism to write Data via an 
 * OutputStream into a RandomAccessFile. The stream starts 
 * writing at the current position of the underlying file. 
 * A close of the stream does not close the file, so it
 * is still open for subsequent processing.
 */
public class RandomAccessFileOutputStream extends OutputStream {
	/**
	 * The internally used RandomAccessFile
	 */
	protected RandomAccessFile raf;
	
	/**
	 * Constructs an OutputStream that writes its data into a
	 * RandomAccessFile.
	 * @param raf The RandomAccessFile used for outputing the stream.
	 */
	public RandomAccessFileOutputStream(RandomAccessFile raf) {
		this.raf = raf;
	}

	/**
	 * Writes a byte into the RandomAccessFile.
	 * @param b byte
	 * @throws IOException
	 */
	public void write(int b) throws IOException {
		write(b);
	}
	
	/**
	 * Writes a byte array into the RandomAccessFile.
	 * @param b byte array
	 * @param off position of the first byte in the array
	 *	that should be written.
	 * @param len length in bytes that should be written.
	 * @throws IOException
	 */
	public void write(byte[] b, int off, int len) throws IOException {
		raf.write(b,off,len);
	}
	
	/**
	 * Data is automatically written to the RandomAccessFile. There is
	 * no gerneral possibility to tell a RandomAccessFile to really write
	 * the data.
	 * @throws IOException
	 */
	public void flush() throws IOException {
	}
	
	/** 
	 * Disconnects the OutputStream. Further calls to the OutputStream 
	 * are not allowed.
	 */
	public void close() {
		raf = null;
	}
}
