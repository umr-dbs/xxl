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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps a DataInput and returns only the first bytes of the DataInput.
 * The methods that return a String are not supported.
 */
public class HeadOfDataInput implements DataInput {
	/**
	 * Number of bytes to read.
	 */
	protected int size;
	/**
	 * The DataInput which is wrapped.
 	 */
	protected DataInput di;
	
	/**
	 * Constructs a new DataInput which only reads the
	 * data up to a certain point. If this point is reached,
	 * an EOFException is thrown.
	 * @param di DataInput to be wrapped.
	 * @param numberOfBytes Number of bytes to read.
	 */
	public HeadOfDataInput(DataInput di, int numberOfBytes) {
		this.di = di;
		size = numberOfBytes;
	}
	/**
	 * @see java.io.DataInput#readFully(byte[])
	 */
	public void readFully(byte[] b) throws IOException {
		size -= b.length;
		if (size<0) throw new EOFException();
		di.readFully(b);
	}
	/**
	 * @see java.io.DataInput#readFully(byte[], int, int)
	 */
	public void readFully(byte[] b, int off, int len) throws IOException {
		size -= len;
		if (size<0) throw new EOFException();
		di.readFully(b, off, len);
	}
	/**
	 * @see java.io.DataInput#skipBytes(int)
	 */
	public int skipBytes(int n) throws IOException {
		size -= n;
		if (size<0) throw new EOFException();
		return di.skipBytes(n);
	}
	/**
	 * @see java.io.DataInput#readBoolean()
	 */
	public boolean readBoolean() throws IOException {
		size--;
		if (size<0) throw new EOFException();
		return di.readBoolean();
	}
	/**
	 * @see java.io.DataInput#readByte()
	 */
	public byte readByte() throws IOException {
		size--;
		if (size<0) throw new EOFException();
		return di.readByte();
	}
	/**
	 * @see java.io.DataInput#readUnsignedByte()
	 */
	public int readUnsignedByte() throws IOException {
		size--;
		if (size<0) throw new EOFException();
		return di.readUnsignedByte();
	}
	/**
	 * @see java.io.DataInput#readShort()
	 */
	public short readShort() throws IOException {
		size -= 2;
		if (size<0) throw new EOFException();
		return di.readShort();
	}
	/**
	 * @see java.io.DataInput#readUnsignedShort()
	 */
	public int readUnsignedShort() throws IOException {
		size -= 2;
		if (size<0) throw new EOFException();
		return di.readUnsignedShort();
	}
	/**
	 * @see java.io.DataInput#readChar()
	 */
	public char readChar() throws IOException {
		size -= 2;
		if (size<0) throw new EOFException();
		return di.readChar();
	}
	/**
	 * @see java.io.DataInput#readInt()
	 */
	public int readInt() throws IOException {
		size -= 4;
		if (size<0) throw new EOFException();
		return di.readInt();
	}
	/**
	 * @see java.io.DataInput#readLong()
	 */
	public long readLong() throws IOException {
		size -= 8;
		if (size<0) throw new EOFException();
		return di.readLong();
	}
	/**
	 * @see java.io.DataInput#readFloat()
	 */
	public float readFloat() throws IOException {
		size -= 4;
		if (size<0) throw new EOFException();
		return di.readFloat();
	}
	/**
	 * @see java.io.DataInput#readDouble()
	 */
	public double readDouble() throws IOException {
		size -= 8;
		if (size<0) throw new EOFException();
		return di.readDouble();
	}
	/**
	 * This method is not supported by the counter.
	 * @return throws an UnsupportedOperationException.
	 * @throws IOException
	 */
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}
	/**
	 * This method is not supported by the counter.
	 * @return throws an UnsupportedOperationException.
	 * @throws IOException
	 */
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}
}
