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

package xxl.core.collections.queues.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.queues.FIFOQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.Convertable;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a queue that uses a container for storing blocks of
 * serialized data. The <tt>peek</tt> method is not implemented by the block 
 * based queue. A converter is used for serializing and de-serializing the 
 * elements of the queue.<p>
 *
 * The block based queue serializes its elements and stores blocks of
 * this serialized data in a container. In order to keep the order of this
 * block, the id of the <i>next</i> block is stored at the end of each
 * block.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new block based queue with ...
 *
 *     BlockBasedQueue queue = new BlockBasedQueue(
 *
 *         // a map container for storing the serialized elements
 *
 *         new MapContainer(),
 *
 *         // a block size of 20 bytes
 *
 *         20,
 *
 *         // an integer converter
 *
 *         IntegerConverter.DEFAULT_INSTANCE,
 *
 *         // an input buffer of 0 bytes
 *
 *         new Constant(0),
 *
 *         // an output buffer of 4 bytes (size of a serialized integer)
 *
 *         new Constant(4)
 *     );
 *
 *     // open the queue
 * 
 *     queue.open();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *     
 *     Iterator iterator = new Enumerator(20);
 *
 *     // insert all elements of the given iterator
 *
 *     for (; iterator.hasNext(); queue.enqueue(iterator.next()));
 *
 *     // print all elements of the queue
 *     
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *
 *     System.out.println();
 *
 *     // close the open queue after use
 *     
 *     queue.close();
 * </pre>
 *
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.io.StreamQueue
 * @see xxl.core.collections.queues.FIFOQueue
 * @see xxl.core.collections.containers.Container
 */
public class BlockBasedQueue extends StreamQueue implements Convertable, FIFOQueue {

	/**
	 * A factory method to create a new BlockBasedQueue (see contract for
	 * {@link xxl.core.collections.queues.Queue#FACTORY_METHOD FACTORY_METHOD} in
	 * interface Queue). In contradiction to the contract in Queue it may
	 * only be invoked with a <i>parameter list</i> (for further
	 * details see Function) of objects. The <i>parameter list</i>
	 * will be used for initializing the random access file queue by
	 * calling the constructor
	 * <pre>
	 * 	BlockBasedQueue((Container) list.get(0), 
	 * 	                 256, 
	 *                  (Function) list.get(1), 
	 *                  new Constant(0), 
	 *                  new Constant(0))
	 * </pre>
	 *
	 * @see Function
	 */
	public static final Function<Object,BlockBasedQueue> FACTORY_METHOD = new AbstractFunction<Object,BlockBasedQueue>() {
		public BlockBasedQueue invoke(List<? extends Object> list) {
			return new BlockBasedQueue(
				(Container)list.get(0),
				256,
				(Function)list.get(1),
				new Constant(0),
				new Constant(0)
			);
		}
	};

	
	
	/**
	 * This function can be used for initialization of external sorting. 
	 * 
	 * This constructor has a parameter function, which is a factory function for storing intermediate runs.  
	 * {@link MergeSorter#MergeSorter(java.util.Iterator, java.util.Comparator, int, int, int, Function, boolean)}
	 * 
	 * 
	 * {@link MergeSorter}
	 * @param queueContainer container e.g. {@link  BlockFileContainer}
	 * @param blockSize in bytes e.g. 4096 
	 * @param converter  for data serializing
	 * @return
	 */
	public static <T> Function<Function<?, Integer>, Queue<T>> createBlockBasedQueueFunctionForMergeSorter(final Container queueContainer, final int blockSize, final Converter<T> converter){
		return new AbstractFunction<Function<?, Integer>, Queue<T>>() {
		public Queue<T> invoke(Function<?, Integer> function1, Function<?, Integer> function2) {
			return new BlockBasedQueue(queueContainer, blockSize, converter,
					function1, function2);
		}
	};
	}
	
	
	
	/**
	 * The container is internally used for storing the blocks that
	 * contains the serialized elements of the queue.
	 */
	protected Container container;

	/**
	 * The size of a block stored in the container. Every block stored in
	 * the container contains <tt>blockSize</tt> bytes of serialized data.
	 */
	protected int blockSize;

	/**
	 * The number of bytes stored by blocks in the container.
	 */
	protected int bytes;

	/**
	 * The id of <tt>readBlock</tt>, i.e. the block the queue currently
	 * reads from.
	 */
	protected Object readBlockId;

	/**
	 * The offset of <tt>readBlock</tt>, i.e. the block the queue currently
	 * reads from.
	 */
	protected int readBlockOffset;

	/**
	 * The id of <tt>writeBlock</tt>, i.e. the block the queue currently
	 * writes to.
	 */
	protected Object writeBlockId;

	/**
	 * The offset of <tt>writeBlock</tt>, i.e. the block the queue currently
	 * writes to.
	 */
	protected int writeBlockOffset;

	/**
	 * The block the queue currently reads from.
	 */
	protected Block readBlock = null;

	/**
	 * The block the queue currently writes to.
	 */
	protected Block writeBlock = null;

	/**
	 * A dummy block that is used for getting a new id when a new write
	 * block is created. When creating a new write block, the dummy block
	 * is inserted into the container in order to get a valid id for the
	 * write block. When the creation of the write block is finished, the
	 * the dummy block is updated by the write block.
	 */
	protected Block dummyBlock = new Block(new byte[0], 0, 0);

	/**
	 * The output stream is used for writing serialized elements to the
	 * queue. It is set to an output stream, that writes given bytes to
	 * the current write block. When the current write block is full, a new
	 * block is inserted into the container. In order to be able to
	 * determine the next block, when reading the serialized data, the id
	 * of the new block is stored at the end of the old write block. The
	 * output stream implicitly adjusts the number of bytes contained by
	 * the queue.
	 */
	protected OutputStream outputStream = new OutputStream() {
		public void write(int b) throws IOException {
			if (bytes > 0 && writeBlock == null)
				writeBlock = (readBlock != null && writeBlockId.equals(readBlockId)) ?
					readBlock :
					(Block)container.get(writeBlockId, false);
			if (writeBlock == null || writeBlockOffset == blockSize) {
				Block newWriteBlock = new Block(new byte[blockSize], 0, blockSize);
				Object newWriteBlockId = container.insert(dummyBlock, false);
				if (writeBlock != null) {
					byte[] byteArray = Converters.toByteArray(container.objectIdConverter(), newWriteBlockId);
					System.arraycopy(
						writeBlock.array,
						writeBlock.offset+blockSize-byteArray.length,
						newWriteBlock.array,
						ShortConverter.SIZE,
						byteArray.length
					);
					if (writeBlockId.equals(readBlockId) && readBlockOffset >= blockSize-byteArray.length) {
						container.remove(readBlockId);
						readBlockId = newWriteBlockId;
						readBlockOffset -= blockSize-byteArray.length-ShortConverter.SIZE;
						readBlock = newWriteBlock;
					}
					else {
						writeBlock.dataOutputStream().writeShort((short)byteArray.length);
						System.arraycopy(
							byteArray,
							0,
							writeBlock.array,
							writeBlock.offset+blockSize-byteArray.length,
							byteArray.length
						);
						container.update(writeBlockId, writeBlock);
					}
					writeBlockOffset = ShortConverter.SIZE+byteArray.length;
				}
				else {
					readBlockId = newWriteBlockId;
					writeBlockOffset = ShortConverter.SIZE;
				}
				writeBlockId = newWriteBlockId;
				writeBlock = newWriteBlock;
			}
			writeBlock.set(writeBlockOffset++, (byte)b);
			bytes++;
		}

		public void close() {
			if (writeBlock != null) {
				container.update(writeBlockId, writeBlock);
				writeBlock = null;
				if (readBlockId == null || !readBlockId.equals(writeBlockId))
					container.unfix(writeBlockId);
			}
		}
	};

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container. The
	 * specified converter is used for serializing and de-serializing the
	 * elements of the queue and the given factory method is used for
	 * initializing the elements of the queue before the blocks are read
	 * out. The specified functions determine the size of the input buffer
	 * and the output buffer. The given <tt>int</tt> value <tt>size</tt>
	 * and <tt>bytes</tt> determines what is the initial size of the queue
	 * and how many bytes are needed for serializing its elements.
	 * <tt>readBlockId</tt>, <tt>readBlockOffset</tt>,
	 * <tt>writeBlockId</tt> and <tt>writeBlockOffset</tt> specifies the
	 * id and the offset of the actual read and write block.
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param converter the converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param newObject a factory method that is invoked in order to
	 *        initialize the elements of the queue, before a block is read
	 *        out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param size the initial size of the queue.
	 * @param bytes the number of bytes needed for storing the initial
	 *        elements of the queue.
	 * @param readBlockId the id of the actual block to read from.
	 * @param readBlockOffset the offset of the actual block to read from.
	 * @param writeBlockId the id of the actual block to write to.
	 * @param writeBlockOffset the offset of the actual block to write to.
	 */
	public BlockBasedQueue(Container container, int blockSize, Converter converter, Function newObject, Function inputBufferSize, Function outputBufferSize, int size, int bytes, Object readBlockId, int readBlockOffset, Object writeBlockId, int writeBlockOffset) {
		super(converter, newObject, inputBufferSize, outputBufferSize, null, null);
		this.container = container;
		this.blockSize = blockSize;
		this.size = size;
		this.bytes = bytes;
		this.readBlockId = readBlockId;
		this.readBlockOffset = readBlockOffset;
		this.writeBlockId = writeBlockId;
		this.writeBlockOffset = writeBlockOffset;
		this.newInputStream = new AbstractFunction(){
			public Object invoke(){
				return new InputStream() {
					public int read() throws IOException {
						if (BlockBasedQueue.this.bytes == 0)
							return -1;
						else {
							int b;
							if (readBlock == null)
								readBlock = (writeBlock != null && BlockBasedQueue.this.readBlockId.equals(BlockBasedQueue.this.writeBlockId)) ?
									writeBlock :
									(Block)BlockBasedQueue.this.container.get(BlockBasedQueue.this.readBlockId, false);
							b = readBlock.get(BlockBasedQueue.this.readBlockOffset++)&255;
							if (--BlockBasedQueue.this.bytes == 0) {
								BlockBasedQueue.this.container.remove(BlockBasedQueue.this.readBlockId);
								readBlock = null;
								BlockBasedQueue.this.readBlockOffset = ShortConverter.SIZE;
								writeBlock = null;
								BlockBasedQueue.this.writeBlockOffset = ShortConverter.SIZE;
							}
							else
								if (BlockBasedQueue.this.readBlockOffset == BlockBasedQueue.this.blockSize-readBlock.dataInputStream().readShort())
									removeBlock();
							return b;
						}
					}
	
					public int available() {
						return BlockBasedQueue.this.bytes;
					}
	
					public void close() {
						if (readBlock != null) {
							readBlock = null;
							if (BlockBasedQueue.this.writeBlockId == null || !BlockBasedQueue.this.readBlockId.equals(BlockBasedQueue.this.writeBlockId))
							BlockBasedQueue.this.container.unfix(BlockBasedQueue.this.readBlockId);
						}
					}
				};	
			}	
		};
		this.newOutputStream = new AbstractFunction(){
			public Object invoke(){
				return new OutputStream() {
					public void write(int b) throws IOException {
						if (BlockBasedQueue.this.bytes > 0 && writeBlock == null)
							writeBlock = (readBlock != null && BlockBasedQueue.this.writeBlockId.equals(BlockBasedQueue.this.readBlockId)) ?
								readBlock :
								(Block)BlockBasedQueue.this.container.get(BlockBasedQueue.this.writeBlockId, false);
						if (writeBlock == null || BlockBasedQueue.this.writeBlockOffset == BlockBasedQueue.this.blockSize) {
							Block newWriteBlock = new Block(new byte[BlockBasedQueue.this.blockSize], 0, BlockBasedQueue.this.blockSize);
							Object newWriteBlockId = BlockBasedQueue.this.container.insert(dummyBlock, false);
							if (writeBlock != null) {
								byte[] byteArray = Converters.toByteArray(BlockBasedQueue.this.container.objectIdConverter(), newWriteBlockId);
								System.arraycopy(
									writeBlock.array,
									writeBlock.offset+BlockBasedQueue.this.blockSize-byteArray.length,
									newWriteBlock.array,
									ShortConverter.SIZE,
									byteArray.length
								);
								if (BlockBasedQueue.this.writeBlockId.equals(BlockBasedQueue.this.readBlockId) && BlockBasedQueue.this.readBlockOffset >= BlockBasedQueue.this.blockSize-byteArray.length) {
									BlockBasedQueue.this.container.remove(BlockBasedQueue.this.readBlockId);
									BlockBasedQueue.this.readBlockId = newWriteBlockId;
									BlockBasedQueue.this.readBlockOffset -= BlockBasedQueue.this.blockSize-byteArray.length-ShortConverter.SIZE;
									readBlock = newWriteBlock;
								}
								else {
									writeBlock.dataOutputStream().writeShort((short)byteArray.length);
									System.arraycopy(
										byteArray,
										0,
										writeBlock.array,
										writeBlock.offset+BlockBasedQueue.this.blockSize-byteArray.length,
										byteArray.length
									);
									BlockBasedQueue.this.container.update(BlockBasedQueue.this.writeBlockId, writeBlock);
								}
								BlockBasedQueue.this.writeBlockOffset = ShortConverter.SIZE+byteArray.length;
							}
							else {
								BlockBasedQueue.this.readBlockId = newWriteBlockId;
								BlockBasedQueue.this.writeBlockOffset = ShortConverter.SIZE;
							}
							BlockBasedQueue.this.writeBlockId = newWriteBlockId;
							writeBlock = newWriteBlock;
						}
						writeBlock.set(BlockBasedQueue.this.writeBlockOffset++, (byte)b);
						BlockBasedQueue.this.bytes++;
					}
	
					public void close() {
						if (writeBlock != null) {
							BlockBasedQueue.this.container.update(BlockBasedQueue.this.writeBlockId, writeBlock);
							writeBlock = null;
							if (BlockBasedQueue.this.readBlockId == null || !BlockBasedQueue.this.readBlockId.equals(BlockBasedQueue.this.writeBlockId))
								BlockBasedQueue.this.container.unfix(BlockBasedQueue.this.writeBlockId);
						}
					}
				};	
			}	
		};
	}

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container. The
	 * specified converter is used for serializing and de-serializing the
	 * elements of the queue. The specified functions determine the size
	 * of the input buffer and the output buffer. The given <tt>int</tt>
	 * value <tt>size</tt> and <tt>bytes</tt> determines what is the
	 * initial size of the queue and how many bytes are needed for
	 * serializing its elements. <tt>readBlockId</tt>,
	 * <tt>readBlockOffset</tt>, <tt>writeBlockId</tt> and
	 * <tt>writeBlockOffset</tt> specifies the id and the offset of the
	 * actual read and write block.<br>
	 * This constructor is equivalent to the call of
	 * <code>BlockBasedQueue(container, blockSize, converter, new Constant(null), inputBufferSize, outputBufferSize, size, bytes, readBlockId, readBlockOffset, writeBlockId, writeBlockOffset)</code>
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param converter the converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param size the initial size of the queue.
	 * @param bytes the number of bytes needed for storing the initial
	 *        elements of the queue.
	 * @param readBlockId the id of the actual block to read from.
	 * @param readBlockOffset the offset of the actual block to read from.
	 * @param writeBlockId the id of the actual block to write to.
	 * @param writeBlockOffset the offset of the actual block to write to.
	 */
	public BlockBasedQueue(Container container, int blockSize, Converter converter, Function inputBufferSize, Function outputBufferSize, int size, int bytes, Object readBlockId, int readBlockOffset, Object writeBlockId, int writeBlockOffset) {
		this(container, blockSize, converter, new Constant(null), inputBufferSize, outputBufferSize, size, bytes, readBlockId, readBlockOffset, writeBlockId, writeBlockOffset);
	}

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container. A
	 * convertable converter is used for serializing and de-serializing
	 * the elements of the queue and the given factory method is used for
	 * initializing the elements of the queue before the blocks are read
	 * out. The specified functions determine the size of the input buffer
	 * and the output buffer. The given <tt>int</tt> value <tt>size</tt>
	 * and <tt>bytes</tt> determines what is the initial size of the queue
	 * and how many bytes are needed for serializing its elements.
	 * <tt>readBlockId</tt>, <tt>readBlockOffset</tt>,
	 * <tt>writeBlockId</tt> and <tt>writeBlockOffset</tt> specifies the
	 * id and the offset of the actual read and write block.<br>
	 * This constructor is equivalent to the call of
	 * <code>BlockBasedQueue(container, blockSize, ConvertableConverter.DEFAULT_INSTANCE, new Constant(null), inputBufferSize, outputBufferSize, size, bytes, readBlockId, readBlockOffset, writeBlockId, writeBlockOffset)</code>
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param newObject a factory method that is invoked in order to
	 *        initialize the elements of the queue, before a block is read
	 *        out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param size the initial size of the queue.
	 * @param bytes the number of bytes needed for storing the initial
	 *        elements of the queue.
	 * @param readBlockId the id of the actual block to read from.
	 * @param readBlockOffset the offset of the actual block to read from.
	 * @param writeBlockId the id of the actual block to write to.
	 * @param writeBlockOffset the offset of the actual block to write to.
	 */
	public BlockBasedQueue(Container container, int blockSize, Function newObject, Function inputBufferSize, Function outputBufferSize, int size, int bytes, Object readBlockId, int readBlockOffset, Object writeBlockId, int writeBlockOffset) {
		this(container, blockSize, ConvertableConverter.DEFAULT_INSTANCE, newObject, inputBufferSize, outputBufferSize, size, bytes, readBlockId, readBlockOffset, writeBlockId, writeBlockOffset);
	}

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container (see
	 * methods <tt>read</tt> and <tt>write</tt> for further detail). The
	 * specified converter is used for serializing and de-serializing the
	 * elements of the queue and the given factory method is used for
	 * initializing the elements of the queue before the blocks are read
	 * out. The specified functions determine the size of the input buffer
	 * and the output buffer.<br>
	 * This constructor is equivalent to the call of
	 * <code>BlockBasedQueue(container, blockSize, converter, newObject, inputBufferSize, outputBufferSize, 0, 0, null, ShortConverter.SIZE, null, ShortConverter.SIZE)</code>.
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param converter the converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param newObject a factory method that is invoked in order to
	 *        initialize the elements of the queue, before a block is read
	 *        out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public BlockBasedQueue(Container container, int blockSize, Converter converter, Function newObject, Function inputBufferSize, Function outputBufferSize) {
		this(container, blockSize, converter, newObject, inputBufferSize, outputBufferSize, 0, 0, null, ShortConverter.SIZE, null, ShortConverter.SIZE);
	}

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container (see
	 * methods <tt>read</tt> and <tt>write</tt> for further detail). The
	 * specified converter is used for serializing and de-serializing the
	 * elements of the queue. The specified functions determine the size
	 * of the input buffer and the output buffer.<br>
	 * This constructor is equivalent to the call of
	 * <code>BlockBasedQueue(container, blockSize, converter, new Constant(null), inputBufferSize, outputBufferSize)</code>.
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param converter the converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public BlockBasedQueue(Container container, int blockSize, Converter converter, Function inputBufferSize, Function outputBufferSize) {
		this(container, blockSize, converter, new Constant(null), inputBufferSize, outputBufferSize);
	}

	/**
	 * Constructs a new block based queue containing the elements that are
	 * stored in blocks of the specified size in the given container (see
	 * methods <tt>read</tt> and <tt>write</tt> for further detail). A
	 * Convertable converter is used for serializing and de-serializing
	 * the elements of the queue and the given factory method is used for
	 * initializing the elements of the queue before the blocks are read
	 * out. The specified functions determine the size of the input buffer
	 * and the output buffer.<br>
	 * This constructor is equivalent to the call of
	 * <code>BlockBasedQueue(container, blockSize, ConvertableConverter.DEFAULT_INSTANCE, newObject, inputBufferSize, outputBufferSize)</code>.
	 *
	 * @param container the container that is used for storing the blocks
	 *        of serialized data. The serialized objects contained by this
	 *        container can be used as initial elements of the queue.
	 * @param blockSize the size of a block that is used for storing the
	 *        serialized elements of the queue.
	 * @param newObject a factory method that is invoked in order to
	 *        initialize the elements of the queue, before a block is read
	 *        out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public BlockBasedQueue (Container container, int blockSize, Function newObject, Function inputBufferSize, Function outputBufferSize) {
		this(container, blockSize, ConvertableConverter.DEFAULT_INSTANCE, newObject, inputBufferSize, outputBufferSize);
	}

	/**
	 * Reads the state of this block based queue from a data input. This
	 * method can be used for initializing the queue with the elements
	 * that are stored in blocks of bytes in the container without using
	 * the corresponding constructor (you know, the one with twelve
	 * parameters).<br>
	 * This implementation reads the fields <tt>size</tt>, <tt>bytes</tt>,
	 * <tt>readBlockId</tt>, <tt>readBlockOffset</tt>,
	 * <tt>writeBlockId</tt> and <tt>writeBlockOffset</tt> from the given
	 * data input.
	 *
	 * @param dataInput the data input to read from the state of this queue.
	 * @throws IOException if an I/O error occurs.
	 */
	public void read(DataInput dataInput) throws IOException {
		size = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
		bytes = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
		readBlockId = BooleanConverter.DEFAULT_INSTANCE.readBoolean(dataInput) ?
			container.objectIdConverter().read(dataInput) :
			null;
		readBlockOffset = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
		writeBlockId = BooleanConverter.DEFAULT_INSTANCE.readBoolean(dataInput) ?
			container.objectIdConverter().read(dataInput) :
			null;
		writeBlockOffset = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
	}

	/**
	 * Writes the actual state of this block based queue to a data output.
	 * This method can be used for serializing the actual state of the
	 * queue in order to use it again in another session without calling
	 * the corresponding constructor (you know, the one with twelve
	 * parameters).<br>
	 * This implementation writes the fields <tt>size</tt>,
	 * <tt>bytes</tt>, <tt>readBlockId</tt>, <tt>readBlockOffset</tt>,
	 * <tt>writeBlockId</tt> and <tt>writeBlockOffset</tt> to the given
	 * data output.
	 *
	 * @param dataOutput the data output to write to the state of this queue.
	 * @throws IOException if an I/O error occurs.
	 */
	public void write(DataOutput dataOutput) throws IOException {
		IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, size);
		IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, bytes);
		BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dataOutput, readBlockId!=null);
		if (readBlockId!=null)
			container.objectIdConverter().write(dataOutput, readBlockId);
		IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, readBlockOffset);
		BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dataOutput, writeBlockId!=null);
		if (writeBlockId!=null)
			container.objectIdConverter().write(dataOutput, writeBlockId);
		IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, writeBlockOffset);
	}

	/**
	 * Returns the number of bytes needed for serializing the elements of
	 * this queue.
	 *
	 * @return the number of bytes stored by blocks in the container.
	 */
	public int bytes() {
		return bytes;
	}

	/**
	 * Returns the id of the the actual block to read from.
	 *
	 * @return the id stored in <tt>readBlockId</tt>.
	 */
	public Object readBlockId () {
		return readBlockId;
	}

	/**
	 * Returns the offset of the actual block to read from.
	 *
	 * @return the offset stored in <tt>readBlockOffset</tt>.
	 */
	public int readBlockOffset() {
		return readBlockOffset;
	}

	/**
	 * Returns the id of the the actual block to write to.
	 *
	 * @return the id stored in <tt>writeBlockId</tt>.
	 */
	public Object writeBlockId() {
		return writeBlockId;
	}

	/**
	 * Returns the offset of the actual block to write to.
	 *
	 * @return the offset stored in <tt>writeBlockOffset</tt>.
	 */
	public int writeBlockOffset() {
		return writeBlockOffset;
	}

	/**
	 * Removes the actual block to read from out of the container and sets
	 * <tt>readBlockId</tt> to the id of the <i>next</i> block to read
	 * from. The id of the <i>next</i> block is stored at the end of the
	 * removed block.
	 * @throws IOException if an I/O error occurs.
	 */
	protected void removeBlock() throws IOException {
		container.remove(readBlockId);
		readBlockId = container.objectIdConverter().read(
			readBlock.dataInputStream(blockSize-readBlock.dataInputStream().readShort())
		);
		readBlock = null;
		readBlockOffset = ShortConverter.SIZE;
	}

	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * This implementation removes all blocks from the container and sets
	 * <tt>size</tt> and <tt>bytes</tt> to <tt>0</tt>. Thereafter the
	 * actual blocks are set to <tt>null</tt>.
	 */
	public void clear() {
		if (bytes > 0) {
			while (!readBlockId.equals(writeBlockId)) {
				readBlock = (Block)container.get(readBlockId);
				try {
					removeBlock();
				}
				catch (IOException ie) {
					throw new WrappingRuntimeException(ie);
				}
			}
			container.remove(readBlockId);
		}
		size = 0;
		bytes = 0;
		readBlock = null;
		readBlockOffset = ShortConverter.SIZE;
		writeBlock = null;
		writeBlockOffset = ShortConverter.SIZE;
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method takes the same effect as a single call.<br>
	 * This implementation secures that the actual state of the block to
	 * write to is stored in the container and sets the actual blocks to
	 * <tt>null</tt>.
	 */
	public void close() {
		if (isClosed) return;
		super.close();
		if (writeBlock != null) {
			container.update(writeBlockId, writeBlock);
			writeBlock = null;
		}
		readBlock = null;
	}
}
