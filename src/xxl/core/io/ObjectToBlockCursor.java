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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;
import xxl.core.io.converters.Converter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class transforms objects into a sequence of Blocks. The objects
 * are delivered by an input Iterator. The Blocks are having a header
 * (which can be used to build up a linked list of Blocks). 
 * <p>
 * The last (nth) Block may not be full whereas the first n-1 Blocks are 
 * completely filled (except empty space inside the header).
 * To get the number of bytes which are used inside the last Block seen by next,
 * call the method getNumberOfBytesInsideLastBlock().
 * <p>
 * To go the way back use the class MultiBlockInputStream.
 */
public class ObjectToBlockCursor extends AbstractCursor {
	/** Input iterator */
	private Cursor cursor;
	
	/** Converter which converts the objects of the input iterator. */
	private Converter converter;
	
	/** Size of the blocks which are delivered to the outside. */
	private int blockSize;

	/** Space left free at the begining of each Block. */
	private int headerSize;
	
	/** Array representing the current status of the next Block. */
	private byte[] array;

	/** Write offset inside the current Block. */
	private int currentWriteOffset;

	/** Serialization of the last converted object */
	private byte[] serializationArray;

	/** Determines how much of the serializationArray has been transfered into array. */
	private int serializationOffset;

	/** Stored the next Object of the input Iterator. */
	private Object nextObject;

	/** Used for serialization */
	private ByteArrayOutputStream bao;

	/** Used for serialization */
	private DataOutputStream dos;

	/** Storing the last length of a transfered Block */
	private int lastLen;
	
	/** Function which is called for every new created Block */
	private Function writeLength;
	
	/**
	 * Creates a new ObjectToBlockIterator.
	 * @param it input Iterator delivering Objects.
	 * @param converter converter used to convert the objects of the input Iterator.
	 * @param blockSize size of the Blocks of the resulting sequence.
	 * @param headerSize size of the header of the Blocks which is left free.
	 * @param maxElementSize size of the biggest input Object to be converted in bytes (at least).
	 * @param writeLength Function which is called for every new created Block (parameters: Block and
	 *	length which is used inside the Block as Integer). This function can be used to 
	 *	encode the real length of the Block inside the Block. If writeLength==null then no
	 *	function call is performed.
	 */
	public ObjectToBlockCursor (Iterator it, Converter converter, int blockSize, int headerSize, int maxElementSize, Function writeLength) {
		if (headerSize<0)
			throw new RuntimeException("headerSize must be at least 0");
		if (headerSize>=blockSize)
			throw new RuntimeException("headerSize must not exceed blockSize");
		
		this.cursor = Cursors.wrap(it);
		this.converter = converter;
		this.blockSize = blockSize;
		this.headerSize = headerSize;
		this.writeLength = writeLength;
		
		bao = new ByteArrayOutputStream(maxElementSize);
		dos = new DataOutputStream(bao);
		
		array = new byte[blockSize];
		currentWriteOffset = headerSize;
		serializationArray = null;
		serializationOffset = 0;
		
		lastLen = -1;

		if (cursor.hasNext())
			nextObject = cursor.next();
		else
			nextObject = null;
	}

	/**
	 * Returns true iff there is another Block in the Iteration.
	 * @return true iff there is another Block in the Iteration.
	 */
	public boolean hasNextObject() {
		if (nextObject!=null)
			return true;
		else
			return serializationArray!=null;
	}

	/**
	 * Copies the next part of the serialization array into the array which is
	 * then used by the Block.
	 * @return true iff the array is full and the next Block can be generated.
	 */
	private boolean copySerializationToArray() {
		boolean blockFull = false;
		
		int currentLength = serializationArray.length-serializationOffset;
		if (currentLength>blockSize-currentWriteOffset) {
			currentLength = blockSize-currentWriteOffset;
			blockFull = true;
		}
		System.arraycopy(serializationArray,serializationOffset,array,currentWriteOffset,currentLength);
		serializationOffset += currentLength;
		currentWriteOffset += currentLength;

		if (serializationOffset == serializationArray.length) {
			serializationArray = null;
			serializationOffset = 0;
		}
		
		return blockFull;
	}

	/** 
	 * Constructs a new Block which gets array as internal array. The first four bytes
	 * represent the total space used inside the Block (blockSize-headerSize iff the
	 * Block has been completly used).
	 * @return the new Block
	 */
	private Object getBlock() {
		// Write the length into the array
		lastLen = currentWriteOffset-headerSize;
		
		// convert it to a Block
		Block block = new Block(array);
		
		// make a new array
		array = new byte[blockSize];
		currentWriteOffset = headerSize;
		
		if (writeLength!=null)
			writeLength.invoke(block, new Integer(lastLen));
		return block;
	}

	/** 
	 * Returns the next Object of the sequence of Blocks.
	 * @return the next Object
	 */
	public Object nextObject() {
		if (serializationArray!=null)
			if (copySerializationToArray())
				return getBlock();
		
		try {
			while (true) {
				if (nextObject != null) {
					converter.write(dos,nextObject);
					dos.flush();
					serializationArray = bao.toByteArray();
					bao.reset();
					if (copySerializationToArray()) {
						if (cursor.hasNext())
							nextObject = cursor.next();
						else
							nextObject = null;
							
						return getBlock();
					}
				}
	
				if (cursor.hasNext())
					nextObject = cursor.next();
				else {
					nextObject = null;
					break;
				}
			}
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}

		return getBlock();
	}

	/**
	 * Returns the number of bytes which are used inside the last Block
	 * returned with next.
	 * @return the number of bytes (-1 if there has not been a next call before).
	 */
	public long getNumberOfBytesInsideLastBlock() {
		return lastLen;
	}

	/**
	 * Closes the cursor and the underlying iteration.
	 */
	public void close() {
		if (isClosed) return;
		cursor.close();
		super.close();
	}
}
