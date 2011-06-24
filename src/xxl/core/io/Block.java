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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.InputStreamEqualPredicate;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a block of serialized data. A block wraps a byte
 * array that stores the serialized data and adds methods for reading and
 * writing single bytes or the whole block. The block can additionally be
 * released when its serialized data is not longer needed.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new block of five bytes
 *
 *     Block block = new Block(5);
 *
 *     // catch IOExceptions
 *
 *     try {
 *
 *         // open a data output stream on the block
 *
 *         DataOutputStream output = block.dataOutputStream();
 *
 *         // write the bytes 0 to 4 to the data output stream
 *
 *         for (int i = 0; i < 5; i++)
 *             output.write(i);
 *
 *         // invert every bit of the block
 *
 *         for (int i = 0; i < block.size; i++)
 *             block.set(i, (byte)block.get(i));
 *
 *         // open a data input stream on the block
 *
 *         DataInputStream input = block.dataInputStream();
 *
 *         // print the five bytes contained by the block
 *
 *         while (input.available()>0)
 *             System.out.println(input.read());
 *
 *     }
 *     catch (IOException ioe) {
 *         System.out.println("An I/O error occured.");
 *     }
 *
 *     // release the block
 *
 *     block.release();
 * </pre>
 *
 * @see DataInputStream
 * @see DataOutputStream
 * @see InputStream
 * @see IOException
 * @see OutputStream
 */
public class Block implements Cloneable, SizeAware {

	/**
	 * Writes the length to the beginning of a Block using little endian byte ordering (4 bytes).
	 * The function returns the block.
	 */
	public static Function SET_REAL_LENGTH = new AbstractFunction() {
		public Object invoke (Object block, Object len) {
			((Block) block).writeInteger(0,((Integer)len).intValue());
			return null;
		}
	};

	/** 
	 * Gets the length from the beginning of a Block. Not every Block contains this information!
	 */
	public static Function GET_REAL_LENGTH = new AbstractFunction() {
		public Object invoke (Object block) {
			return new Integer(((Block) block).readInteger(0));
		}
	};

	/**
	 * The byte array stores the serialized data of the block. The block
	 * starts at index <tt>offset</tt> of the array and contains
	 * <tt>size</tt> bytes.
	 */
	public byte [] array;

	/**
	 * The <tt>int</tt> value <tt>offset</tt> determines the index of the
	 * byte array where this block starts. Bytes at indices that are less
	 * than <tt>offset</tt> did not belong to this block.
	 */
	public int offset;

	/**
	 * The <tt>int</tt> value <tt>size</tt> determines the number of bytes
	 * this block is able to store.
	 */
	public int size;

	/**
	 * The flag <tt>released</tt> signals whether this block is released
	 * or not. It is not possible to access a released block in order to
	 * read or write bytes.
	 */
	protected boolean released = false;

	/**
	 * Constructs a new Block that wraps the specified array. The block
	 * starts at the index of the byte array specified by <tt>offset</tt>
	 * and contain <tt>size</tt> bytes.
	 *
	 * @param array the byte array that contains the serialized data of
	 *        the block.
	 * @param offset the index of the byte array where the block starts.
	 * @param size the number of bytes the block contains.
	 * @throws IllegalArgumentException when the byte array is not able to
	 *         store this block. In other words, an exception is thrown
	 *         when <code>(array.length&lt;offset+size)</code>.
	 */
	public Block (byte [] array, int offset, int size) throws IllegalArgumentException {
		if (array.length<offset+size)
			throw new IllegalArgumentException();
		this.array = array;
		this.offset = offset;
		this.size = size;
	}

	/**
	 * Constructs a new Block that wraps the specified array. The whole
	 * byte array is used for storing the block. This constructor is
	 * equivalent to the call of
	 * <code>Block(array, 0, array.length)</code>.
	 *
	 * @param array the byte array that contains the serialized data of
	 *        the block.
	 */
	public Block (byte [] array) {
		this(array, 0, array.length);
	}

	/**
	 * Constructs a new Block that contains a number of bytes specified by
	 * <tt>blockSize</tt>. The block creates a new byte array in order to
	 * store its serialized data. This constructor is equivalent to the
	 * call of <code>Block(new byte[blockSize], 0, blockSize)</code>.
	 *
	 * @param blockSize the number of bytes the block contains.
	 */
	public Block (int blockSize) {
		this(new byte[blockSize], 0, blockSize);
	}

	/**
	 * Signals that the serialized data of this block is no longer needed
	 * and releses the block. After releasing a block, it is not possible
	 * to access it in order to read or write bytes.<br>
	 * Note that this method is not idempotent. A call of this method on a
	 * released block will cause an <tt>IllegalStateException</tt>.
	 *
	 * @throws IllegalStateException if this block is already released.
	 */
	public void release () throws IllegalStateException {
		if (released)
			throw new IllegalStateException("Block has already been released.");
		released = true;
	}

	/**
	 * Replaces the byte at the specified index of this block with the
	 * specified byte and returns the set byte. When this block is
	 * released or the specified index is out of its bounds, an exception
	 * will be thrown.
	 *
	 * @param index the index of the byte that should be set.
	 * @param b the new value of the byte at index <tt>index</tt>.
	 * @return the set byte.
	 * @throws IllegalStateException if this block is already released.
	 * @throws IndexOutOfBoundsException if the specified index is out of
	 *         this block's bounds. In other words, an exception is thrown
	 *         when <code>(index<0 || index>=size)</code>.
	 */
	public byte set (int index, byte b) throws IllegalStateException, IndexOutOfBoundsException {
		if (released)
			throw new IllegalStateException("Block has already been released.");
		if (index<0 || index>=size)
			throw new IndexOutOfBoundsException("Index accessed: "+index);
		return array[offset+index] = b;
	}

	/**
	 * Returns the byte at the specified index of this block. When this
	 * block is released or the specified index is out of its bounds, an
	 * exception will be thrown.
	 *
	 * @param index position which is retrieved.
	 * @return the byte at the specified index.
	 * @throws IllegalStateException if this block is already released.
	 * @throws IndexOutOfBoundsException if the specified index is out of
	 *         this block's bounds. In other words, an exception is thrown
	 *         when <code>(index<0 || index>=size)</code>.
	 */
	public byte get (int index) throws IllegalStateException, IndexOutOfBoundsException {
		if (released)
			throw new IllegalStateException("Block has already been released.");
		if (index<0 || index>=size)
			throw new IndexOutOfBoundsException();
		return array[offset+index];
	}

	/**
	 * Writes an integer inside the Block to a specified position (little endian).
	 * @param position write offset
	 * @param value value to be written
	 */
	public void writeInteger(int position, int value) {
		ByteArrayConversions.convIntToByteArrayLE(value, array, position);
	}

	/**
	 * Reads an integer from a specified position inside the Block (little endian).
	 * @param position read offset
	 * @return the integer value
	 */
	public int readInteger(int position) {
		return ByteArrayConversions.convIntLE(array, position);
	}

	/**
	 * Writes a long inside the Block to a specified position (little endian).
	 * @param position write offset
	 * @param value value to be written
	 */
	public void writeLong(int position, long value) {
		ByteArrayConversions.convLongToByteArrayLE(value, array, position);
	}

	/**
	 * Reads a long from a specified position inside the Block (little endian).
	 * @param position read offset
	 * @return the long value
	 */
	public long readLong(int position) {
		return ByteArrayConversions.convLongLE(array, position);
	}

	/**
	 * Returns a new output stream that depends on this block. The output
	 * stream starts writing at index <tt>base</tt> (<i>inclusive</i>) and
	 * stops at index <tt>end</tt> (<i>exclusive</i>) of this block. When
	 * writing more than <tt>(end-base)</tt> bytes, the write method of
	 * the output stream will cause an <tt>IOException</tt>.
	 *
	 * @param base the index of this block where the output stream starts
	 *        writing (<i>inclusive</i>).
	 * @param end the index of this block where the output stream stops
	 *        writing (<i>exclusive</i>).
	 * @return a new output stream, that depends on this block.
	 */
	public OutputStream outputStream(final int base, final int end) {
		return new OutputStream() {
			int position = base;

			public final void write(int b) throws IOException {
				if (released)
					throw new IllegalStateException("Block has already been released.");
				if (position >= end)
					throw new IndexOutOfBoundsException("Index accessed: "+(position+1));
				array[offset+position] = (byte)b;
				position++;
			}

			public final void write(byte[] b) throws IOException {
				write(b, 0, b.length);
			}

			public final void write(byte[] b, int off, int len) throws IOException {
				if (released)
					throw new IllegalStateException("Block has already been released.");
				if (position<0 || position+len > end)
					throw new IndexOutOfBoundsException("Index accessed: "+(position+len));
				System.arraycopy(b, off, array, position, len);
				position += len;
			}
		};
	}

	/**
	 * Returns a new output stream that depends on this block. The output
	 * stream starts writing at index <tt>base</tt> (<i>inclusive</i>) and
	 * stops at the end of this block. When writing more than
	 * <tt>(size-base)</tt> bytes, the write method of the output stream
	 * will cause an <tt>IOException</tt>.
	 *
	 * @param base the index of this block where the output stream starts
	 *        writing (<i>inclusive</i>).
	 * @return a new output stream, that depends on this block.
	 */
	public OutputStream outputStream (final int base) {
		return outputStream(base, size);
	}

	/**
	 * Returns a new output stream that depends on this block. The output
	 * stream writes the whole block. When writing more than <tt>size</tt>
	 * bytes, the write method of the output stream will cause an
	 * <tt>IOException</tt>.
	 *
	 * @return a new output stream, that depends on this block.
	 */
	public OutputStream outputStream () {
		return outputStream(0);
	}

	/**
	 * Returns a new data output stream that depends on this block. The
	 * data output stream wraps an output stream created by calling
	 * <code>outputStream(base, end)</code>. The underlying output stream
	 * starts writing at index <tt>base</tt> (<i>inclusive</i>) and stops
	 * at index <tt>end</tt> (<i>exclusive</i>) of this block. When
	 * writing more than <tt>(end-base)</tt> bytes, the write method of
	 * the underlying output stream will cause an <tt>IOException</tt>.
	 *
	 * @param base the index of this block where the data output stream
	 *        starts writing (<i>inclusive</i>).
	 * @param end the index of this block where the data output stream
	 *        stops writing (<i>exclusive</i>).
	 * @return a new data output stream, that depends on this block.
	 */
	public DataOutputStream dataOutputStream (int base, int end) {
		return new DataOutputStream(outputStream(base, end));
	}

	/**
	 * Returns a new data output stream that depends on this block. The
	 * data output stream wraps an output stream created by calling
	 * <code>outputStream(base)</code>. The underlying output stream
	 * starts writing at index <tt>base</tt> (<i>inclusive</i>) and stops
	 * at the end of this block. When writing more than
	 * <tt>(size-base)</tt> bytes, the write method of the underlying
	 * output stream will cause an <tt>IOException</tt>.
	 *
	 * @param base the index of this block where the data output stream
	 *        starts writing (<i>inclusive</i>).
	 * @return a new data output stream, that depends on this block.
	 */
	public DataOutputStream dataOutputStream (int base) {
		return new DataOutputStream(outputStream(base));
	}

	/**
	 * Returns a new data output stream that depends on this block. The
	 * data output stream wraps an output stream created by calling
	 * <code>outputStream()</code>. The underlying output stream writes
	 * the whole block. When writing more than <tt>size</tt> bytes, the
	 * write method of the underlying output stream will cause an
	 * <tt>IOException</tt>.
	 *
	 * @return a new data output stream, that depends on this block.
	 */
	public DataOutputStream dataOutputStream () {
		return new DataOutputStream(outputStream());
	}

	/**
	 * Returns a new input stream that depends on this block. The input
	 * stream starts reading at index <tt>base</tt> (<i>inclusive</i>) and
	 * stops at index <tt>end</tt> (<i>exclusive</i>) of this block. When
	 * reading more than <tt>(end-base)</tt> bytes, the read method of the
	 * input stream will return <tt>-1</tt>.
	 *
	 * @param base the index of this block where the input stream starts
	 *        reading (<i>inclusive</i>).
	 * @param end the index of this block where the input stream stops
	 *        reading (<i>exclusive</i>).
	 * @return a new input stream, that depends on this block.
	 */
	public InputStream inputStream (final int base, final int end) {
		return new InputStream () {
			int position = base;

			public final int read () {
				return position>=end? -1: get(position++)&255;
			}
			public final int read(byte[] b) {
				return read(b, 0, b.length);
			}
			public final int read(byte[] b, int off, int len) {
				if (released)
					throw new IllegalStateException("Block has already been released.");
				if (position<0 || position>=size)
					throw new IndexOutOfBoundsException();
				if (position+len > size)
					len = size-position;
				System.arraycopy(array, position, b, off, len);
				position += len;
				return len;
			}
			public final int available () {
				return end-position;
			}
		};
	}

	/**
	 * Returns a new input stream that depends on this block. The input
	 * stream starts reading at index <tt>base</tt> (<i>inclusive</i>) and
	 * stops at the end of this block. When reading more than
	 * <tt>(size-base)</tt> bytes, the read method of the input stream
	 * will return <tt>-1</tt>.
	 *
	 * @param base the index of this block where the input stream starts
	 *        reading (<i>inclusive</i>).
	 * @return a new input stream, that depends on this block.
	 */
	public InputStream inputStream (final int base) {
		return inputStream(base, size);
	}

	/**
	 * Returns a new input stream that depends on this block. The input
	 * stream reads the whole block. When reading more than <tt>size</tt>
	 * bytes, the read method of the input stream will return <tt>-1</tt>.
	 *
	 * @return a new input stream, that depends on this block.
	 */
	public InputStream inputStream () {
		return inputStream(0);
	}

	/**
	 * Returns a new data input stream that depends on this block. The
	 * data input stream wraps an input stream created by calling
	 * <code>inputStream(base, end)</code>. The underlying input stream
	 * starts reading at index <tt>base</tt> (<i>inclusive</i>) and stops
	 * at index <tt>end</tt> (<i>exclusive</i>) of this block. When
	 * reading more than <tt>(end-base)</tt> bytes, the read method of the
	 * underlying input stream will return <tt>-1</tt>.
	 *
	 * @param base the index of this block where the data input stream
	 *        starts reading (<i>inclusive</i>).
	 * @param end the index of this block where the data input stream
	 *        stops reading (<i>exclusive</i>).
	 * @return a new data input stream, that depends on this block.
	 */
	public DataInputStream dataInputStream (int base, int end) {
		return new DataInputStream(inputStream(base, end));
	}

	/**
	 * Returns a new data input stream that depends on this block. The
	 * data input stream wraps an input stream created by calling
	 * <code>inputStream(base)</code>. The underlying input stream starts
	 * reading at index <tt>base</tt> (<i>inclusive</i>) and stops at the
	 * end of this block. When reading more than <tt>(size-base)</tt>
	 * bytes, the read method of the underlying input stream will return
	 * <tt>-1</tt>.
	 *
	 * @param base the index of this block where the data input stream
	 *        starts reading (<i>inclusive</i>).
	 * @return a new data input stream, that depends on this block.
	 */
	public DataInputStream dataInputStream (int base) {
		return new DataInputStream(inputStream(base));
	}

	/**
	 * Returns a new data input stream that depends on this block. The
	 * data input stream wraps an input stream created by calling
	 * <code>inputStream()</code>. The underlying input stream reads the
	 * whole block. When reading more than <tt>size</tt> bytes, the read
	 * method of the underlying input stream will return <tt>-1</tt>.
	 *
	 * @return a new data input stream, that depends on this block.
	 */
	public DataInputStream dataInputStream () {
		return new DataInputStream(inputStream());
	}

	/**
	 * Compresses (zips) the current Block and returns the compressed Block.
	 *
	 * This function works with a ByteArrayOutputStream, because the size of
	 * the compressed array cannot be evaluated before the ZipOutputStream
	 * has run.
	 * @return the compressed Block.
	 */
	public Block compress() {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ZipOutputStream zos = new ZipOutputStream(baos);
			zos.setMethod(ZipOutputStream.DEFLATED);
			zos.putNextEntry(new ZipEntry("default"));
			zos.write(array,offset,size);
			zos.finish();
			zos.close();
			return new Block(baos.toByteArray());
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Uncompresses (unzips) the current Block and returns the original Block.
	 *
	 * This function works with a ByteArrayOutputStream, because the size of
	 * the decompressed array cannot be evaluated before the ZipInputStream
	 * has finished its work.
	 * @return the decompressed Block.
	 */
	public Block decompress() {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ZipInputStream zis = new ZipInputStream(inputStream());
			// ZipEntry entry 
			zis.getNextEntry();
			int len;
			byte buffer[] = new byte[512];
			while (true) {
				len = zis.read(buffer);
				if (len==-1)
					break;
				else
					baos.write(buffer,0,len);
			}
			return new Block(baos.toByteArray());
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Indicates whether some other Record is "equal to" this one.
	 * Implementation: Get the InputStream of each Record and
	 * read single bytes until the bytes are not equal.
	 * @param object - the reference object with which to compare.
	 * @return true if this Block is the same as the object argument, false otherwise.
	 */
	public boolean equals(Object object) {
		return 
			InputStreamEqualPredicate.DEFAULT_INSTANCE.invoke(
				this.inputStream(),
				((Block) object).inputStream()
			);
	}

	/** 
	 * Returns a hash code for the Block. The implementation is efficient
	 * and produces different hashCodes for most of the blocks.
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		// Efficient
		if (size>=5)
			return readInteger(0) ^ readInteger(size-4);
		else {
			int ret=42;
			for (int i=0; i<size; i++) {
				ret <<= 8;
				ret = ret | array[i];
			}
			return ret;
		}
	}

	/**
	 * Clones the Block object.
	 * @return the (deeply) cloned object.
	 * @throws CloneNotSupportedException Never should do this.
	 */
	public Object clone() throws CloneNotSupportedException {
		Block b = (Block) super.clone();
		b.array = (byte[]) this.array.clone();
		return b;
	}

	/**
	 * Returns the converted size of the node in bytes.
	 * @return The size in bytes.
	 */
	public int getMemSize() {
		return size;
	}
	
	/**
	 * Outputs the bytes of a block to a String. The String contains the
	 * byte values as well as the character representation.
	 * @return the String
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer("Size: "+size+"\nBEGINNING OF BLOCK\n");
		StringBuffer lineHex = new StringBuffer();
		StringBuffer lineChar = new StringBuffer();
		DecimalFormat int6Format = new DecimalFormat("000000");
		DecimalFormat int3Format = new DecimalFormat("000 ");
		
		for (int i=0; i<size; i++) {
			int value = get(i);
			if (value<0)
				value += 256;
			lineHex.append(int3Format.format(value));
			if (get(i)<32)
				lineChar.append(".");
			else
				lineChar.append((char) get(i));
			if (i%16==15) {
				sb.append(int6Format.format((i/16)*16));
				sb.append(" ");
				sb.append(lineHex);
				sb.append(" ");
				sb.append(lineChar);
				sb.append("\n");
				lineHex.setLength(0);
				lineChar.setLength(0);
			}
		}

		if (size%16>0) {
			lineHex.setLength(4*16);
			sb.append(int6Format.format((size/16)*16));
			sb.append(" ");
			sb.append(lineHex);
			sb.append(" ");
			sb.append(lineChar);
			sb.append("\n");
		}
		sb.append("END OF BLOCK");
		
		return sb.toString();
	}
}
