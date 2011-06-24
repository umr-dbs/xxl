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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a buffered random access file that reads and writes
 * whole blocks of buffered bytes to and from the random access file. When
 * reading bytes from the file, the buffer is checked first, whether it
 * contains the desired bytes. When the desired bytes are not buffered,
 * whole blocks containing the desired bytes are read into the buffer.
 * When writing bytes to the file, the bytes are written to the buffered
 * blocks (that are read from the file into the buffer when needed). So
 * every read and write operation works actually on the buffer and does
 * only access the file, when the desired bytes are not buffered. Every
 * time a block is flushed out of the buffer, the according bytes in the
 * random access file are updated.<p>
 *
 * Example usage (1).
 * <pre>
 *     // catch IOExceptions
 *
 *     try {
 *
 *         // create a new buffered random access file with ...
 *
 *         BufferedRandomAccessFile file = new BufferedRandomAccessFile(
 *
 *             // a new file
 *
 *             new File("file.dat"),
 *
 *             // read and write access
 *
 *             "rw",
 *
 *             // an LRU buffer with 5 slots
 *
 *             new LRUBuffer(5),
 *
 *             // a block size of 20 bytes
 *
 *             20
 *         ); 
 *
 *         // write data to the data output stream
 *
 *         file.writeInt(200);
 *         file.writeUTF("Some data!");
 *         file.writeUTF("More data!");
 *         file.writeBoolean(true);
 *         file.writeUTF("Another bundle of data!");
 *         file.writeUTF("The last bundle of data!");
 *
 *         // set the file pointer to the first string
 *
 *         file.seek(4);
 *
 *         // print the data of the data input stream
 *
 *         System.out.println(file.readUTF());
 *         System.out.println(file.readUTF());
 *         System.out.println(file.readBoolean());
 *         System.out.println(file.readUTF());
 *         System.out.println(file.readUTF());
 *
 *         // set the file pointer to the beginning of the file
 *
 *         file.seek(0);
 *
 *         // print the first integer
 *
 *         System.out.println(file.readInt());
 *
 *         // close the open streams
 *
 *         file.close();
 *     }
 *     catch (IOException ioe) {
 *         System.out.println("An I/O error occured.");
 *     }
 * </pre>
 *
 * @see File
 * @see Function
 * @see IOException
 * @see RandomAccessFile
 * @see WrappingRuntimeException
 */
public class BufferedRandomAccessFile extends RandomAccessFile {

	/**
	 * The buffer that is used for storing the blocks of buffered bytes.
	 */
	protected Buffer buffer;

	/**
	 * The size of the blocks that are used for storing the buffered bytes
	 * of this random access file. In other words, every block can be used
	 * for buffering <tt>blockSize</tt> bytes of this random access file.
	 */
	protected int blockSize;

	/**
	 * The file pointer is a kind of cursor, or index into the random
	 * access file; input operations read bytes starting at the file
	 * pointer and advance the file pointer past the bytes read. If the
	 * random access file is in read/write mode, output operations write
	 * bytes starting at the file pointer and advance the file pointer
	 * past the bytes written.
	 */
	protected long filePointer;

	/**
	 * The length of the buffered random access file.
	 */
	protected long length;

	/**
	 * The block of buffered bytes that is accessed at last. This byte
	 * array contains the bytes of the random access file that are read or
	 * written at last.
	 */
	protected byte [] block = null;

	/**
	 * The id of the actual block in the buffer. The <tt>blockId</tt> is
	 * determined by the offset of the block's bytes in the random access
	 * file  and <tt>blockSize</tt>, i.e. the first <tt>blockSize</tt>
	 * bytes of the file will be buffered in the block with <tt>blockId
	 * 0</tt>, the next <tt>blockSize</tt> bytes int hte block with
	 * <tt>blockSize 1</tt> and so on.
	 */
	protected long blockId = 0;

	/**
	 * A simple counter for counting I/Os. It is increased every time a
	 * block is read from the file into the buffer or the other way round.
	 * It can be used for comparing the efficiency of buffered and
	 * unbuffered files or of buffers with different displacement
	 * strategies.
	 */
	public final IOCounter counter;

	/**
	 * Constructs a new buffered random access file stream to read from,
	 * and optionally to write to, the file specified by the <tt>File</tt>
	 * argument. <br>
	 * The mode argument must either be equal to <tt>"r"</tt> or
	 * <tt>"rw"</tt>, indicating that the file is to be opened for input
	 * only or for both input and output, respectively. The write methods
	 * on this object will always throw an <tt>IOException</tt> if the
	 * file is opened with a mode of <tt>"r"</tt>. If the mode is
	 * <tt>"rw"</tt> and the file does not exist, then an attempt is made
	 * to create it. An <tt>IOException</tt> is thrown if the file
	 * argument refers to a directory.<br>
	 * The specified buffer is used for buffering the bytes of the random
	 * access file. The buffered bytes are stored in blocks with the
	 * specified size.<br>
	 * This constructor is equivalent to th call of
	 * <code>BufferedRandomAccessFile(file, mode, buffer, blockSize, new IOCounter())</code>.
	 *
	 * @param file the file object.
	 * @param mode the access mode.
	 * @param buffer the buffer that is used for buffering the file.
	 * @param blockSize the size of a block of buffered bytes.
	 * @throws IllegalArgumentException if the mode argument is not equal
	 *         to <tt>"r"</tt> or to <tt>"rw"</tt>.
	 * @throws java.io.FileNotFoundException if the file exists but is a
	 *         directory rather than a regular file, or cannot be opened
	 *         or created for any other reason.
	 * @throws IOException if an I/O error occurs.
	 * @throws SecurityException if a security manager exists and its
	 *         checkRead method denies read access to the file or the mode
	 *         is <tt>"rw"</tt> and the security manager's checkWrite
	 *         method denies write access to the file.
	 */
	public BufferedRandomAccessFile (File file, String mode, Buffer buffer, int blockSize) throws IOException {
		this(file, mode, buffer, blockSize, new IOCounter());
	}

	/**
	 * Creates a random access file stream to read from, and optionally to
	 * write to, a file with the specified name. <br>
	 * The mode argument must either be equal to <tt>"r"</tt> or
	 * <tt>"rw"</tt>, indicating that the file is to be opened for input
	 * only or for both input and output, respectively. The write methods
	 * on this object will always throw an <tt>IOException</tt> if the
	 * file is opened with a mode of <tt>"r"</tt>. If the mode is
	 * <tt>"rw"</tt> and the file does not exist, then an attempt is made
	 * to create it. An <tt>IOException</tt> is thrown if the name
	 * argument refers to a directory.<br>
	 * The specified buffer is used for buffering the bytes of the random
	 * access file. The buffered bytes are stored in blocks with the
	 * specified size.
	 *
	 * @param name the system-dependent filename.
	 * @param mode the access mode.
	 * @param buffer the buffer that is used for buffering the file.
	 * @param blockSize the size of a block of buffered bytes.
	 * @throws IllegalArgumentException if the mode argument is not equal
	 *         to <tt>"r"</tt> or to <tt>"rw"</tt>.
	 * @throws java.io.FileNotFoundException if the file exists but is a
	 *         directory rather than a regular file, or cannot be opened
	 *         or created for any other reason.
	 * @throws IOException if an I/O error occurs.
	 * @throws SecurityException if a security manager exists and its
	 *         checkRead method denies read access to the file or the mode
	 *         is <tt>"rw"</tt> and the security manager's checkWrite
	 *         method denies write access to the file.
	 */
	public BufferedRandomAccessFile (String name, String mode, Buffer buffer, int blockSize) throws IOException {
		this(name, mode, buffer, blockSize, new IOCounter());
	}

	/**
	 * Constructs a new buffered random access file stream to read from,
	 * and optionally to write to, the file specified by the <tt>File</tt>
	 * argument. <br>
	 * The mode argument must either be equal to <tt>"r"</tt> or
	 * <tt>"rw"</tt>, indicating that the file is to be opened for input
	 * only or for both input and output, respectively. The write methods
	 * on this object will always throw an <tt>IOException</tt> if the
	 * file is opened with a mode of <tt>"r"</tt>. If the mode is
	 * <tt>"rw"</tt> and the file does not exist, then an attempt is made
	 * to create it. An <tt>IOException</tt> is thrown if the file
	 * argument refers to a directory.<br>
	 * The specified buffer is used for buffering the bytes of the random
	 * access file. The buffered bytes are stored in blocks with the
	 * specified size. The specified I/O counter will be continued in
	 * order to count the I/Os caused by the buffered random access file.
	 *
	 * @param file the file object.
	 * @param mode the access mode.
	 * @param buffer the buffer that is used for buffering the file.
	 * @param blockSize the size of a block of buffered bytes.
	 * @param counter an I/O counter to be continued.
	 * @throws IllegalArgumentException if the mode argument is not equal
	 *         to <tt>"r"</tt> or to <tt>"rw"</tt>.
	 * @throws java.io.FileNotFoundException if the file exists but is a
	 *         directory rather than a regular file, or cannot be opened
	 *         or created for any other reason.
	 * @throws IOException if an I/O error occurs.
	 * @throws SecurityException if a security manager exists and its
	 *         checkRead method denies read access to the file or the mode
	 *         is <tt>"rw"</tt> and the security manager's checkWrite
	 *         method denies write access to the file.
	 */
	public BufferedRandomAccessFile (File file, String mode, Buffer buffer, int blockSize, IOCounter counter) throws IOException {
		super(file, mode);
		this.buffer = buffer;
		this.blockSize = blockSize;
		this.length = super.length();
		this.filePointer = super.getFilePointer();
		this.counter = counter;
	}

	/**
	 * Creates a random access file stream to read from, and optionally to
	 * write to, a file with the specified name. <br>
	 * The mode argument must either be equal to <tt>"r"</tt> or
	 * <tt>"rw"</tt>, indicating that the file is to be opened for input
	 * only or for both input and output, respectively. The write methods
	 * on this object will always throw an <tt>IOException</tt> if the
	 * file is opened with a mode of <tt>"r"</tt>. If the mode is
	 * <tt>"rw"</tt> and the file does not exist, then an attempt is made
	 * to create it. An <tt>IOException</tt> is thrown if the name
	 * argument refers to a directory.<br>
	 * The specified buffer is used for buffering the bytes of the random
	 * access file. The buffered bytes are stored in blocks with the
	 * specified size. The specified I/O counter will be continued in
	 * order to count the I/Os caused by the buffered random access file.
	 *
	 * @param name the system-dependent filename.
	 * @param mode the access mode.
	 * @param buffer the buffer that is used for buffering the file.
	 * @param blockSize the size of a block of buffered bytes.
	 * @param counter an I/O counter to be continued.
	 * @throws IllegalArgumentException if the mode argument is not equal
	 *         to <tt>"r"</tt> or to <tt>"rw"</tt>.
	 * @throws java.io.FileNotFoundException if the file exists but is a
	 *         directory rather than a regular file, or cannot be opened
	 *         or created for any other reason.
	 * @throws IOException if an I/O error occurs.
	 * @throws SecurityException if a security manager exists and its
	 *         checkRead method denies read access to the file or the mode
	 *         is <tt>"rw"</tt> and the security manager's checkWrite
	 *         method denies write access to the file.
	 */
	public BufferedRandomAccessFile (String name, String mode, Buffer buffer, int blockSize, IOCounter counter) throws IOException {
		super(name, mode);
		this.buffer = buffer;
		this.blockSize = blockSize;
		this.length = super.length();
		this.filePointer = super.getFilePointer();
		this.counter = counter;
	}

	/**
	 * Closes this buffered random access file stream and releases any
	 * system resources associated with the stream. A closed buffered
	 * random access file cannot perform input or output operations and
	 * cannot be reopened.<br>
	 * This implementation flushes the buffer first. Thereafter all
	 * buffered blocks are removed out of the buffer and actual block is
	 * set to <tt>null</tt>. At last the random access file is closed.
	 *
	 * @throws IOException if an I/O error occurs.
	 */
	public void close () throws IOException {
		try {
			buffer.flushAll(this);
			buffer.removeAll(this);
			block = null;
		}
		catch (WrappingRuntimeException wre) {
			if (wre.throwable instanceof IOException)
				throw (IOException)wre.throwable;
			else
				throw wre;
		}
		super.close();
	}

	/**
	 * Returns the current offset in this file.
	 *
	 * @return the offset from the beginning of the file, in bytes, at
	 *         which the next read or write occurs.
	 * @throws IOException if an I/O error occurs.
	 */
	public long getFilePointer () throws IOException {
		return filePointer;
	}

	/**
	 * Returns the length of this file.
	 *
	 * @return the length of this file, measured in bytes.
	 * @throws IOException if an I/O error occurs.
	 */
	public long length () throws IOException {
		return length;
	}

	/**
	 * Sets <tt>block</tt> to the block of buffered bytes that contains
	 * the byte the file pointer points to. When this block is not
	 * buffered, the actual block is unfixed and a new block containing
	 * the desired bytes is inserted into the buffer.
	 *
	 * @param readBlock determines whether to read the block from file
	 * 		or just to provide an empty block frame.
	 * @throws IOException if an I/O error occurs.
	 */
	protected void getBlock (boolean readBlock) throws IOException {
		if (block!=null && filePointer/blockSize!=blockId) {
			buffer.unfix(this, new Long(blockId));
			block = null;
		}
		if (block==null) {
			blockId = filePointer/blockSize;
			try {
				block = readBlock?
					(byte[])buffer.get(this, new Long(blockId),
						new AbstractFunction() {
							public Object invoke (Object id) {
								return readBlock(id);
							}
						},
						false
					):
					new byte[blockSize];
			}
			catch (WrappingRuntimeException wre) {
				if (wre.throwable instanceof IOException)
					throw (IOException)wre.throwable;
				else
					throw wre;
			}
		}
	}

	/**
	 * Reads the block of bytes with the given id and returns it. The id
	 * and <tt>blockSize</tt> determines the block of bytes to read. The
	 * id <tt>0</tt> identifies the first <tt>blockSize</tt> bytes of the
	 * file, the id <tt>1</tt> identifies the second <tt>blockSize</tt>
	 * bytes and so on. After reading the block of bytes, the I/O counter
	 * is increased and the byte array containing this block is returned.
	 * @param id The id of the block (type Long).
	 * @return The block as a byte array.
	 */
	protected byte [] readBlock (Object id) {
		try {
			block = new byte[blockSize];
			super.seek(((Long)id).longValue()*blockSize);
			super.read(block);
			counter.readIO++;
			return block;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Reads up to <tt>len</tt> bytes of data from the buffered file into
	 * an array of bytes. This method blocks until at least one byte of
	 * input is available.<br>
	 * Although <tt>BufferedRandomAccessFile</tt> is not a subclass of
	 * <tt>InputStream</tt>, this method behaves in the exactly the same
	 * way as the <tt>InputStream.read(byte[], int, int)</tt> method of
	 * <tt>InputStream</tt>.
	 *
	 * @param array the byte array into which the data is read.
	 * @param off the start offset of the data.
	 * @param len the maximum number of bytes read.
	 * @return the total number of bytes read into the array, or
	 *         <tt>-1</tt> if there is no more data because the end of the
	 *         file has been reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read (byte [] array, int off, int len) throws IOException {
		int n = 0;
		int b;

		while (len-->0 && (b = read())>=0)
			array[off+n++] = (byte)b;
		return n==0? -1: n;
	}

	/**
	 * Reads up to <tt>array.length</tt> bytes of data from the buffered
	 * file into an array of bytes. This method blocks until at least one
	 * byte of input is available.<br>
	 * Although <tt>BufferedRandomAccessFile</tt> is not a subclass of
	 * <tt>InputStream</tt>, this method behaves in the exactly the same
	 * way as the <tt>InputStream.read(byte[])</tt> method of
	 * <tt>InputStream</tt>.
	 *
	 * @param array the byte array into which the data is read.
	 * @return the total number of bytes read into the array, or
	 *         <tt>-1</tt> if there is no more data because the end of
	 *         this file has been reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read (byte [] array) throws IOException {
		return read(array, 0, array.length);
	}

	/**
	 * Reads a byte of data from this file. The byte is returned as an
	 * <tt>int</tt> value in the range 0 to 255 <tt>(0x00-0x0ff)</tt>.
	 * This method blocks if no input is yet available. <br>
	 * This implementation sets <tt>block</tt> to the block that contains
	 * the desired byte by calling the getBlock method. Thereafter the
	 * desired byte is read and returned.<br>
	 * Although <tt>BufferedRandomAccessFile</tt> is not a subclass of
	 * <tt>InputStream</tt>, this method behaves in exactly the same way
	 * as the <tt>InputStream.read()</tt> method of <tt>InputStream</tt>.
	 *
	 * @return the next byte of data, or <tt>-1</tt> if the end of the
	 *         file has been reached.
	 * @throws IOException if an I/O error occurs. Not thrown if
	 *         end-of-file has been reached.
	 */
	public int read () throws IOException {
		if (filePointer>=length)
			return -1;
		getBlock(true);
		return block[(int)(filePointer++%blockSize)]&255;
	}

	/**
	 * Sets the file-pointer offset, measured from the beginning of this
	 * file, at which the next read or write occurs. The offset may be set
	 * beyond the end of the file. Setting the offset beyond the end of
	 * the file does not change the file length. The file length will
	 * change only by writing after the offset has been set beyond the end
	 * of the file.
	 *
	 * @param pos the offset position, measured in bytes from the
	 *        beginning of the file, at which to set the file pointer.
	 * @throws IOException if <tt>pos</tt> is less than <tt>0</tt> or if
	 *         an I/O error occurs.
	 */
	public void seek (long pos) throws IOException {
		filePointer = pos;
	}

	/**
	 * Sets the length of this file. <br>
	 * If the present length of the file as returned by the
	 * <tt>length</tt> method is greater than the <tt>newLength</tt>
	 * argument then the file will be truncated. In this case, if the file
	 * offset as returned by the <tt>getFilePointer</tt> method is greater
	 * then <tt>newLength</tt> then after this method returns the offset
	 * will be equal to <tt>newLength</tt>. Also every block storing only
	 * buffered bytes of the truncated part of the file is removed out of
	 * the buffer.<br>
	 * If the present length of the file as returned by the
	 * <tt>length</tt> method is smaller than the <tt>newLength</tt>
	 * argument then the file will be extended. In this case, the contents
	 * of the extended portion of the file are not defined.
	 *
	 * @param newLength the desired length of the file.
	 * @throws IOException if an I/O error occurs.
	 */
	public void setLength (long newLength) throws IOException {
		super.setLength(newLength);
		if (newLength<length) {
			if (blockId*blockSize>=newLength)
				block = null;
			for (long id = (length-1)/blockSize; id>(newLength-1)/blockSize; id--)
				buffer.remove(this, new Long(id));
		}
		length = newLength;
		if (filePointer>length)
			filePointer = length;
	}

	/**
	 * Overwrites the block of bytes with the given id with the specified
	 * object (byte array). The id and <tt>blockSize</tt> determines the
	 * block of bytes to overwrite. The id <tt>0</tt> identifies the first
	 * <tt>blockSize</tt> bytes of the file, the id <tt>1</tt> identifies
	 * the second <tt>blockSize</tt> bytes and so on. After writing the
	 * block of bytes, the I/O counter is increased.
	 * @param id position inside the file as Long.
	 * @param object byte array to be written.
	 */
	protected void writeBlock (Object id, Object object) {
		try {
			super.seek(((Long)id).longValue()*blockSize);
			super.write((byte[])object, 0, Math.min(blockSize, (int)(length-((Long)id).longValue()*blockSize)));
			counter.writeIO++;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Writes <tt>len</tt> bytes from the specified byte array starting at
	 * offset <tt>off</tt> to this buffered file.
	 *
	 * @param array the data.
	 * @param off the start offset in the data.
	 * @param len the number of bytes to write.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (byte [] array, int off, int len) throws IOException {
		while (len>0) {
			int piece = Math.min(len, (int)(blockSize-filePointer%blockSize));

			getBlock(piece<blockSize);
			System.arraycopy(array, off, block, (int)(filePointer%blockSize), piece);
			try {
				buffer.update(this, new Long(blockId), block,
					new AbstractFunction() {
						public Object invoke (Object id, Object object) {
							writeBlock(id, object);
							return null;
						}
					},
					false
				);
			}
			catch (WrappingRuntimeException wre) {
				if (wre.throwable instanceof IOException)
					throw (IOException)wre.throwable;
				else
					throw wre;
			}
			off += piece;
			len -= piece;
			filePointer += piece;
			if (length<filePointer)
				length = filePointer;
		}
	}

	/**
	 * Writes <tt>b.length</tt> bytes from the specified byte array to
	 * this buffered file, starting at the current file pointer.
	 *
	 * @param array the data.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (byte [] array) throws IOException {
		write(array, 0, array.length);
	}

	/**
	 * Writes the specified byte to this buffered file. The write starts
	 * at the current file pointer.<br>
	 * This implementation sets <tt>block</tt> to the block that contains
	 * the desired byte by calling the getBlock method. Thereafter the
	 * flush function of the buffered block is updated by a function that
	 * writes the block of bytes to the random access file, when it is
	 * invoked. At last the desired byte is written to the actual block.
	 *
	 * @param b the byte to be written.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write (int b) throws IOException {
		write(new byte[] {(byte)b});
	}
}
