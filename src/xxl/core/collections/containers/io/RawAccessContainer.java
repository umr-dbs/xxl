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

package xxl.core.collections.containers.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.AbstractContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.ByteArrayConversions;
import xxl.core.io.MultiBlockInputStream;
import xxl.core.io.ObjectToBlockCursor;
import xxl.core.io.converters.ByteConverter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.io.raw.RawAccess;
import xxl.core.util.BitSet;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a container that is able to store blocks directly
 * on a raw access. The implementation is quite similar to the
 * implementation of the BlockFileContainer.
 *
 * @see xxl.core.collections.containers.Container
 * @see xxl.core.collections.containers.io.BlockFileContainer
 * @see IOException
 * @see Iterator
 * @see NoSuchElementException
 * @see WrappingRuntimeException
 */
public class RawAccessContainer extends AbstractContainer{

	/**
	 * The container file of this container. The container file is used
	 * for storing the blocks of this container. In addition, the offset
	 * of an block in the container file determines the id of the block in
	 * this container. The name of the container file is determined by the
	 * <tt>String prefix</tt> and the file extension <tt>.ctr</tt>.
	 */
	protected RawAccess ra;

	/**
	 * The size reserved for storing a block in this container. Every
	 * block stored in the container file takes <tt>blockSize</tt> bytes,
	 * therefore blocks that contains more than <tt>blockSize</tt> bytes
	 * cannot be stored in this container.
	 */
	protected int blockSize;

	protected int maxBlocks;

	protected int maxFreeListBlocks;

	/**
	 * The number of blocks stored in this container.
	 */
	protected long size;
	protected long lastBlockNumber;
	protected int freeListSize;
	protected int freeListEntriesPerBlock;
	// TODO: Sicherheitscheck beim Lesen/Schreiben, ob in Metadaten (oder Metadaten doch vorne?)
	protected long lenBitSetsInSectors;

	protected long sectorInBuffer;
	protected boolean bufferDirty;
	protected byte[] block;

	/**
	 * Determines the type of the identifyers which are produced:
	 * 1: Byte, 2: Short, 3: Integer, 4: Long (default).
	 */
	protected byte idType=4;

	protected BitSet updatedBitSet;
	protected BitSet reservedBitSet;

/*	getUpdatedBitMap(blockNumber);
	unsetUpdatedBitMap
	setUpdatedBitMap(blockNumber);

	private final void unsetReservedBitMap(long blockNumber) {
		long sector = blockNumber/blockSize;
		
		ra.read()
		reservedBitMap.seek(offset/blockSize/8);
		b = reservedBitMap.read();
		reservedBitMap.seek(reservedBitMap.getFilePointer()-1);
		reservedBitMap.write();
	}

	getReservedBitMap(
	setReservedBitMap(offset); // 
		b|(1<<(offset/blockSize%8)
	*/
	public void pushFreeList(long blockNumber) {
		int sectorNumber = freeListSize/freeListEntriesPerBlock+1;
		if (sectorNumber>maxFreeListBlocks+1)
			throw new RuntimeException("Free block list has an overflow");
		if (sectorInBuffer!=sectorNumber) {
			commit();
			ra.read(block, sectorNumber);
			sectorInBuffer = sectorNumber;
		}
		int positionInBuffer = (freeListSize%freeListEntriesPerBlock)*8;
		ByteArrayConversions.convLongToByteArrayLE(blockNumber, block, positionInBuffer);
		freeListSize++;
		bufferDirty = true;
	}
	public long popFreeListElement() {
		freeListSize--;
		int sectorNumber = freeListSize/freeListEntriesPerBlock+1;
		if (sectorInBuffer!=sectorNumber) {
			// no commit, because the sector is thrown away!
			ra.read(block, sectorNumber);
			sectorInBuffer = sectorNumber;
		}
		int positionInBuffer = (freeListSize%freeListEntriesPerBlock)*8;
		return ByteArrayConversions.convLongLE(block, positionInBuffer);
	}
	public void commit() {
		if (sectorInBuffer>=0 && bufferDirty) {
			ra.write(block, sectorInBuffer);
			bufferDirty = false;
		}
	}

	/**
	 * Constructs an empty BlockFileContainer that is able to store blocks
	 * with a maximum size of <tt>blockSize</tt> bytes. The given
	 * <tt>String prefix</tt> specifies the names of the files the are
	 * used for storing the elements of the container. When using existing
	 * files to store the container their data will be overwritten.
	 * <p>
	 * This constructor is useful if you want to keep your file in
	 * a special self developed filesystem.
	 */
	public RawAccessContainer(RawAccess ra, int maxFreeListBlocks) {
		this.ra = ra;
		this.maxFreeListBlocks = maxFreeListBlocks; 
		
		blockSize = ra.getSectorSize();
		block = new byte[blockSize];
		maxBlocks = (int) ra.getNumSectors();
		
		freeListEntriesPerBlock = blockSize/8;
		sectorInBuffer = -1;
		updatedBitSet = new BitSet(maxBlocks);
		reservedBitSet = new BitSet(maxBlocks);
		
		lenBitSetsInSectors = ((2*BitSet.getSize(maxBlocks))+blockSize-1) / blockSize;
		
		reset();
	}

	/**
	 * Constructs a BlockFileContainer that consists of existing files
	 * given by the specified file name. Every information the container
	 * needs will be taken from the meta file.
	 */
	public RawAccessContainer(RawAccess ra) {
		this.ra = ra;
		blockSize = ra.getSectorSize();
		block = new byte[blockSize];
		freeListEntriesPerBlock = blockSize/8;
		
		ra.read(block,0);
		sectorInBuffer = -1;
		size = ByteArrayConversions.convLongLE(block, 0);
		lastBlockNumber = ByteArrayConversions.convLongLE(block, 8);
		maxBlocks = ByteArrayConversions.convIntLE(block, 16);
		maxFreeListBlocks = ByteArrayConversions.convIntLE(block, 20);
		freeListSize = ByteArrayConversions.convIntLE(block, 24);
		
		lenBitSetsInSectors = ((2*BitSet.getSize(maxBlocks))+blockSize-1) / blockSize;
		// final long sector = ra.getNumSectors() - lenSectors;
		
		InputStream is = new MultiBlockInputStream(
			new Iterator() {
				long sector =  RawAccessContainer.this.ra.getNumSectors() - lenBitSetsInSectors ;
				public boolean hasNext() {
					return sector < RawAccessContainer.this.ra.getNumSectors();
				}
				public Object next() {
					byte b[] = new byte[blockSize];
					RawAccessContainer.this.ra.read(b,sector);
					sector++;
					return new Block(b);
				}
				public void remove() {
					throw new UnsupportedOperationException();
				}
			},
			0,
			blockSize,
			null
		);
		try {
			// is.read();
			reservedBitSet = (BitSet) BitSet.DEFAULT_CONVERTER.read(new DataInputStream(is));
			updatedBitSet = (BitSet) BitSet.DEFAULT_CONVERTER.read(new DataInputStream(is));
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Because of using the offset in the container file (<tt>long</tt>
	 * value) as id, this method always returns a <tt>LongConverter</tt>.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	public FixedSizeConverter objectIdConverter() {
		switch (idType) {
		case 1: return ByteConverter.DEFAULT_INSTANCE;
		case 2: return ShortConverter.DEFAULT_INSTANCE;
		case 3: return IntegerConverter.DEFAULT_INSTANCE;
		default: return LongConverter.DEFAULT_INSTANCE;
		}
	}

	/**
	 * Returns the size of the ids generated by this container in bytes,
	 * which is 8.
	 * @return 8
	 */
	public int getIdSize() {
		return LongConverter.SIZE;
	}

	/**
	 * Returns the size reserved for storing a block in this container.
	 * Every block stored in the container file takes <tt>blockSize</tt>
	 * bytes, therefore blocks that contains more than <tt>blockSize</tt>
	 * bytes cannot be stored in this container.
	 *
	 * @return the size reserved for storing a block in this container.
	 */
	public int blockSize() {
		return blockSize;
	}

	/**
	 * Resets this container and any files associated with it.<br>
	 * This implementation sets the length of the associated files to
	 * <tt>0</tt>. Thereafter the size and maximum offset of this
	 * container are corrected.
	 */
	public void reset() {
		size = 0;
		lastBlockNumber = -1;
		freeListSize = 0;
	}

	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 * This implementation only calls the <tt>reset()</tt> method.
	 */
	public void clear() {
		reset();
	}

	/**
	 * Closes the Container and releases its associated files. But before
	 * closing the meta file, the serialized state of this container must
	 * be appended. Therefore, the values of the fields <tt>size</tt> and
	 * <tt>blockSize</tt> are append to the meta file. A closed container
	 *  can be implicitly reopened by a consecutive call to one of its
	 *  methods.
	 */
	public void close() {
		commit();
		ByteArrayConversions.convLongToByteArrayLE(size, block, 0);
		ByteArrayConversions.convLongToByteArrayLE(lastBlockNumber, block, 8);
		ByteArrayConversions.convIntToByteArrayLE(maxBlocks, block, 16);
		ByteArrayConversions.convIntToByteArrayLE(maxFreeListBlocks, block, 20);
		ByteArrayConversions.convIntToByteArrayLE(freeListSize, block, 24);
		ra.write(block,0);
		
		Cursor c = new ObjectToBlockCursor(
			new ArrayCursor<BitSet>(reservedBitSet, updatedBitSet),
			BitSet.DEFAULT_CONVERTER,
			blockSize,
			0,
			BitSet.getSize(maxBlocks),
			null
		);
		
		long sector = ra.getNumSectors() - lenBitSetsInSectors;
		
		Block b;
		while (c.hasNext()) {
			b = (Block) c.next();
			ra.write(b.array, sector++);
		}
		
		ra = null;
	}

	/**
	 * Returns <tt>true</tt> if the container contains a block for the identifier
	 * <tt>id</tt>.<br>
	 * This implementation checks whether the updatedBitMap files contains
	 * an entry for the offset specified by <tt>id</tt>.
	 *
	 * @param id identifier of the block.
	 * @return true if the container has updated a block for the specified
	 *         identifier.
	 */
	public boolean contains(Object id) {
		long blockNumber = ((Number)id).longValue();
		if (blockNumber>lastBlockNumber)
			return false;
		
		return updatedBitSet.get((int) blockNumber);
	}

	/**
	 * Returns the block associated to the identifier <tt>id</tt>. An
	 * exception is thrown when the desired block is not found via contains.
	 * In this implementation the parameter unfix has no function because
	 * the container is unbuffered.
	 *
	 * @param id identifier of the block.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the block associated to the specified identifier.
	 * @throws NoSuchElementException if the desired block is not found.
	 */
	public Object get(Object id, boolean unfix) throws NoSuchElementException {
		byte [] array = new byte [blockSize];
		
		if (!contains(id))
			throw new NoSuchElementException();
		
		long blockNumber = ((Number)id).longValue();
		
		ra.read(array, blockNumber+maxFreeListBlocks+1);
		return new Block(array, 0, blockSize);
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator ids() {
		return new Iterator () {
			Long id = new Long(-1), nextId;
			boolean removeable = false;

			public boolean hasNext () {
				for (removeable = false; !isUsed(nextId = new Long(id.longValue()+1)); id = nextId)
					if (nextId.longValue()>lastBlockNumber)
						return false;
				return true;
			}

			public Object next () throws NoSuchElementException {
				if (!hasNext())
					throw new NoSuchElementException();
				removeable = true;
				return id = nextId;
			}

			public void remove () throws IllegalStateException {
				if (!removeable)
					throw new IllegalStateException();
				RawAccessContainer.this.remove(id);
				removeable = false;
			}
		};
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 * This implementation checks whether the reservedBitMap files contains
	 * an entry for the offset specified by <tt>id</tt>.
	 *
	 * @param id the id to be checked.
	 * @return <tt>true</tt> exactly if the <tt>id</tt> is still in use.
	 */
	public boolean isUsed(Object id) {
		long blockNumber = ((Number)id).longValue();
		
		if (blockNumber>lastBlockNumber)
			return false;
		
		return reservedBitSet.get((int) blockNumber);
	}

	/**
	 * Removes the block with identifier <tt>id</tt>. An exception is
	 * thrown when a block with an identifier <tt>id</tt> is not in the
	 * container. After a call of <tt>remove()</tt> all the iterators (and
	 * cursors) can be in an invalid state.<br>
	 * This implementation clears the entry for the block in both fat files
	 * and adds <tt>id</tt> to the freeList file.
	 *
	 * @param id an identifier of a block.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove(Object id) throws NoSuchElementException {
		long blockNumber = ((Number)id).longValue();
		
		if (!isUsed(id))
			throw new NoSuchElementException();
		if (--size==0)
			reset();
		else {
			if (blockNumber==lastBlockNumber) {
				while (!isUsed(new Long(--blockNumber)));
				lastBlockNumber = blockNumber;
			}
			else {
				reservedBitSet.clear((int) blockNumber);
				updatedBitSet.clear((int) blockNumber);
				
				pushFreeList(blockNumber);
			}
		}
	}

	/**
	 * Reserves an id for subsequent use.
	 * This implementation sets in the reservedBitMap file the
	 * appropriate bit for the id returned by this method.
	 *
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved. Not used by this
	 *			implementation.
	 * @return the reserved id.
	*/
	public Object reserve (Function getObject) {
		long blockNumber;
		
		while (true) {
			if (freeListSize==0) {
				blockNumber = lastBlockNumber+1;
				break;
			}
			blockNumber = popFreeListElement();
			
			if (blockNumber<=lastBlockNumber)
				break;
		}
		reservedBitSet.set((int) blockNumber);
		if (blockNumber==lastBlockNumber+1) {
			updatedBitSet.clear((int) blockNumber);
			lastBlockNumber = blockNumber;
		}
		
		size++;
		
		switch (idType) {
		case 1: return new Byte((byte) blockNumber);
		case 2: return new Short((short) blockNumber);
		case 3: return new Integer((int) blockNumber);
		default: return new Long(blockNumber);
		}
	}

	/**
	 * Returns the number of elements of the container. In other words,
	 * the number of set bits in the updatedBitMap file.
	 *
	 * @return the number of elements.
	 */
	public int size () {
		return (int) size;
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if a block with an identifier <tt>id</tt> does
	 * not exist in the container (checked via isUsed). The parameter <tt>unfix</tt>
	 * has no function because this container is unbuffered.
	 *
	 * @param id identifier of the element.
	 * @param object the new block that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals a buffered container whether the block can be
	 *        removed from the underlying buffer.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		long blockNumber = ((Number)id).longValue();
		Block block = (Block)object;
		if (block.size>blockSize)
			throw new IllegalArgumentException("Block too large");
		
		if (blockNumber>lastBlockNumber)
			throw new NoSuchElementException();
		
		if (!updatedBitSet.get((int) blockNumber)) {
			if (!isUsed(id))
				throw new NoSuchElementException();
			updatedBitSet.set((int) blockNumber);
		}
		
		byte array[];
		if (block.offset>0 || blockSize>block.array.length-block.offset) {
			array = new byte[blockSize];
			System.arraycopy(block.array, block.offset, array, 0, block.size);
		}
		else
			array = block.array;
		ra.write(array, blockNumber+maxFreeListBlocks+1);
	}
	
	/**
	 * 
	 * 
	 */
	public Object batchReserve(int addresses){
		long headBlockNumber = (Long) this.reserve(null); // start block nummer
		long blockNumber = 0L;
		for(int i = 0; i < addresses; i++){
			blockNumber = headBlockNumber+i;  
			reservedBitSet.set((int) blockNumber);
			if (blockNumber==lastBlockNumber+1) {
				updatedBitSet.clear((int) blockNumber);
				lastBlockNumber = blockNumber;
			}
			if (!updatedBitSet.get((int) blockNumber)) {
				updatedBitSet.set((int) blockNumber);
			}
			size++;
		}
		return new Long(headBlockNumber);
	}
	/**
	 * 
	 */
	public Object[] batchInsert(Object[] blocks) {
		Long[] ids = new Long[blocks.length];
		long headBlockNumber = (Long) this.reserve(null); // start block nummer
		long blockNumber = 0L;
		for(int i = 0; i < ids.length; i++){
			ids[i] = headBlockNumber+i;
			blockNumber = ids[i];  
			reservedBitSet.set((int) blockNumber);
			if (blockNumber==lastBlockNumber+1) {
				updatedBitSet.clear((int) blockNumber);
				lastBlockNumber = blockNumber;
			}
			if (!updatedBitSet.get((int) blockNumber)) {
				updatedBitSet.set((int) blockNumber);
			}
			size++;
		}
		// flatten array
		byte array[] = new byte[blocks.length * blockSize];
		for(int i = 0; i < blocks.length; i++){
			System.arraycopy(((Block)blocks[i]).array, 0, array, i*(blockSize), ((Block)blocks[i]).size);
		}
		// write 
		ra.write(array, headBlockNumber+maxFreeListBlocks+1);
		return ids;
	}
	
	public Object[] batchInsert(Object headBlockNumber,  Object[] blocks) {
		Long[] ids = new Long[blocks.length]; // start block nummer
		long blockNumber = 0L;
		long head = (Long)headBlockNumber;
		for(int i = 0; i < ids.length; i++){
			ids[i] = (Long)head+i;
			blockNumber = ids[i];  
		}
		// flatten array
		byte array[] = new byte[blocks.length * blockSize];
		for(int i = 0; i < blocks.length; i++){
			System.arraycopy(((Block)blocks[i]).array, 0, array, i*(blockSize), ((Block)blocks[i]).size);
		}
		// write 
		ra.write(array, head+maxFreeListBlocks+1);
		return ids;
	}
}
