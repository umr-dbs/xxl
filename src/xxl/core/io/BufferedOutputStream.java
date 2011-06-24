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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import xxl.core.collections.MapEntry;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class wraps an output stream by writing whole blocks of buffered
 * output bytes to the wrapped output stream. The buffered output stream
 * acts like an usual output stream:<br>
 * it accepts output bytes and sends them to some sink. But the buffered
 * output stream stores bytes written to it in buffered blocks. When
 * flushing the written bytes to the sink, whole blocks of bytes are
 * written to the wrapped output stream.<p>
 *
 * Example usage (1).
 * <pre>
 *     // catch IOExceptions
 *
 *     try {
 *
 *         // create a new byte array output stream
 *
 *         ByteArrayOutputStream output = new ByteArrayOutputStream();
 *
 *         // create a new LRU buffer with 5 slots
 *
 *         LRUBuffer buffer = new LRUBuffer(5);
 *
 *         // create a new block factory method
 *
 *         Function newBlock = new AbstractFunction() {
 *             public Object invoke() {
 *                 return new Block(new byte [20]);
 *             }
 *         };
 *
 *         // buffer the output stream
 *
 *         BufferedOutputStream buffered = new BufferedOutputStream(output, buffer, newBlock);
 *
 *         // create a new data output stream
 *
 *         DataOutputStream dataOutput = new DataOutputStream(buffered);
 *
 *         // write data to the data output stream
 *
 *         dataOutput.writeUTF("Some data!");
 *         dataOutput.writeUTF("More data!");
 *         dataOutput.writeUTF("Another bundle of data!");
 *         dataOutput.writeUTF("The last bundle of data!");
 *
 *         // flush the buffered output stream
 *
 *         buffered.flush();
 *
 *         // create a byte array input stream on the output stream
 *
 *         ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *
 *         // create a new data input stream
 *
 *         DataInputStream dataInput = new DataInputStream(input);
 *
 *         // print the data of the data input stream
 *
 *         while (dataInput.available() > 0)
 *             System.out.println(dataInput.readUTF());
 *
 *         // close the open streams
 *
 *         dataOutput.close();
 *         dataInput.close();
 *     }
 *     catch (IOException ioe) {
 *         System.out.println("An I/O error occured.");
 *     }
 * </pre>
 *
 * @see Collections
 * @see FeatureComparator
 * @see Function
 * @see IOException
 * @see Iterator
 * @see LinkedList
 * @see java.util.Map.Entry
 * @see MapEntry
 * @see OutputStream
 * @see WrappingRuntimeException
 */
public class BufferedOutputStream extends OutputStream {

	/**
	 * The output stream that is buffered by storing the bytes written to
	 * it in buffered blocks.
	 */
	protected OutputStream outputStream;

	/**
	 * The buffer that is used for buffering the blocks storing the
	 * written bytes.
	 */
	protected Buffer buffer;

	/**
	 * A block factory that is invoked, when a new empty block is needed
	 * for storing the written bytes.
	 */
	protected Function blockFactory;

	/**
	 * The block that is actually used for storing the bytes that are
	 * written to the output stream. When it is null, the buffer is empty
	 * or the actual block is exhausted and a new block must be created by
	 * invoking the block factory.
	 */
	protected Block block = null;

	/**
	 * The offset in the block that is actually used for storing the bytes
	 * written to the output stream. In other words, the number of bytes
	 * that are already written to the actual block. It determines the
	 * position of the next byte that is written to thie actual block.
	 */
	protected int offset = 0;

	/**
	 * The number of bytes that are buffered by this output stream. In
	 * other words, the number of bytes that are written to the buffered
	 * blocks since the last flush.
	 */
	protected int bufferedBytes = 0;

	/**
	 * An <tt>Integer</tt> object that stores the number of full blocks
	 * contained by the buffer. After creating a new emtpy block by
	 * invoking the block factory, the counter is used as id when
	 * inserting the block into the buffer.
	 */
	protected Integer counter = new Integer(0);

	/**
	 * A list for storing the buffered blocks when they are flushed. The
	 * data of the blocks cannot be written to the output stream in the
	 * order they are flushed, because the displacemant strategy can
	 * change the order they are written. Therefore the flushed blocks
	 * must be sorted according to their ids.
	 *
	 * @see BufferedOutputStream#counter
	 */
	protected LinkedList updateList = new LinkedList();

	/**
	 * Constructs a new buffered output stream that wraps the given output
	 * stream by storing the bytes written to it in buffered blocks. The
	 * specified buffer is used for buffering these blocks and the given
	 * block factory is used for creating a new empty block when needed.
	 *
	 * @param outputStream the output stream to be buffered.
	 * @param buffer the buffer used for buffering the blocks storing the
	 *        bytes written the the wrapped output stream.
	 * @param blockFactory a function that is invoked, when a new empty
	 *        block is needed.
	 */
	public BufferedOutputStream (OutputStream outputStream, Buffer buffer, Function blockFactory) {
		this.outputStream = outputStream;
		this.buffer = buffer;
		this.blockFactory = blockFactory;
	}

	/**
	 * Closes this buffered output stream and releases any system
	 * resources associated with this stream. The general contract of
	 * close is that it closes the output stream. A closed stream cannot
	 * perform output operations and cannot be reopened.<br>
	 * This implementation flushes the buffer first. Thereafter all
	 * buffered blocks are removed out of the buffer and the wrapped
	 * output stream is closed.
	 *
	 * @throws IOException if an I/O error occurs.
	 */
	public void close () throws IOException {
		flush();
		buffer.removeAll(this);
		outputStream.close();
	}

	/**
	 * Flushes this buffered output stream and forces any buffered output
	 * bytes to be written out. The general contract of flush is that
	 * calling it is an indication that, if any bytes previously written
	 * have been buffered by the implementation of the output stream, such
	 * bytes should immediately be written to their intended destination.
	 * <br>
	 * This implementation flushes all buffered elements first (this
	 * forces the insertion of the buffered blocks into
	 * <tt>updateList</tt>). Thereafter <tt>updateList</tt> is sorted
	 * according to the ids of the stored blocks (that are given by the
	 * <tt>counter</tt>). Then the data of the blocks is written to the
	 * wrapped output stream and the blocks are released.
	 *
	 * @throws IOException if an I/O error occurs.
	 */
	public void flush () throws IOException {
		buffer.flushAll(this);
		Collections.sort(updateList, new FeatureComparator(
			new ComparableComparator(),
			new AbstractFunction() {
				public Object invoke (Object entry) {
					return ((Entry)entry).getKey();
				}
			}
		));
		for (Iterator entries = updateList.iterator(); entries.hasNext();) {
			Block block = (Block)((Entry)entries.next()).getValue();

			outputStream.write(block.array, 0, (bufferedBytes -= block.size)>0 ? block.size : offset);
		}
		for (Iterator entries = updateList.iterator(); entries.hasNext();)
			((Block)((Entry)entries.next()).getValue()).release();
		updateList.clear();
		block = null;
		bufferedBytes = 0;
	}

	/**
	 * Writes the specified byte to this output stream. The general
	 * contract for write is that one byte is written to the output
	 * stream. The byte to be written is the eight low-order bits of the
	 * argument b. The 24 high-order bits of b are ignored.<br>
	 * First this implementation checks whether an actual block to write
	 * bytes to exists or not. When it does not exist, a new empty block
	 * is created by invoking the block factory and inserted into the
	 * buffer. The new block is inserted into the buffer with a flush
	 * function that inserts the block into <tt>updateList</tt>, when it
	 * is invoked. Thereafter the byte is written to the actual block and
	 * the block is checked whether it is full. When it is full, the
	 * buffered block is unfixed, <tt>counter</tt> is increased and
	 * <tt>block</tt> is set to <tt>null</tt>.
	 *
	 * @param b the byte to be written to the buffered output stream.
	 * @throws IOException if an I/O error occurs. In particular, an
	 *         IOException may be thrown if the output stream has been
	 *         closed.
	 */
	public void write (int b) throws IOException {
		if (block==null) {
			buffer.update(this, counter, block = (Block)blockFactory.invoke(),
 				new AbstractFunction() {
					public Object invoke (Object id, Object object) {
						MapEntry mapEntry = new MapEntry(id, object);

						if (updateList.isEmpty()) {
							updateList.add(mapEntry);
							try {
								flush();
							}
							catch (IOException ie) {
								throw new WrappingRuntimeException(ie);
							}
						}
						else
							if (!updateList.getFirst().equals(mapEntry)) {
								updateList.add(mapEntry);
								buffer.remove(id, object);
							}
						return null;
					}
				},
				false
			);
			offset = 0;
		}
		block.set(offset++, (byte)b);
		bufferedBytes++;
		if (offset==block.size) {
			buffer.unfix(this, counter);
			counter = new Integer(counter.intValue()+1);
			block = null;
		}
	}
}
