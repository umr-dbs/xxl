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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.AbstractContainer;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.FilesystemOperations;
import xxl.core.io.JavaFilesystemOperations;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;

/**
 * This class provides a container that is able to store logical blocks
 * whose size is an arbitrary. <p>
 *
 * The implementation is based on two <tt>BlockFileContainer</tt>s:<br>
 * one serving as a primary container for the first physical block, and
 * the other storing the remaining physical blocks as a linked list of
 * blocks.<br>
 * Therefore, every logical block stored in the container is separated
 * into physical blocks of (<code>blockSize-LongConverter.SIZE</code>)
 * bytes and every physical block stores the id of the next physical
 * block (so the size of a physical block is <tt>blockSize</tt>). The end
 * of this linked list of physical blocks is marked by a negative id. This
 * id is given by the <tt>Long</tt> object
 * <code>new Long(-1-logicalBlock.size)</code> and determines the size of
 * the logical block.<br>
 * The first physical block of the linked list of physical blocks is
 * inserted into this container (the primary container) and the rest of
 * the physical blocks are inserted into the secondary container.<br>
 * Because it is based on two containers, the multi block container
 * depends on ten files: five files that names are determined by the
 * <tt>String "Primary"+prefix</tt> store the primary container and another
 * five files that names are determined by the <tt>String
 * "Secondary"+prefix</tt> store the secondary container.<p>
 *
 * Example usage (simplified part of the main method).
 * <pre>
 *     // create a new multi block container with ...
 *
 *     MultiBlockContainer container = new MultiBlockContainer(
 *
 *         // files having the file name "MultiBlockContainer"
 *
 *         "MultiBlockContainer",
 *
 *         // a block size of 11 bytes (8 bytes for the longs pointing to the next container and 3
 *         // bytes for data)
 *
 *         11
 *     );
 *
 *     // insert 10 blocks containing double values (8 bytes)
 *
 *     for (int i = 1; i < 11; i++) {
 *
 *         // create a new block of 8 bytes
 *
 *         Block block = new Block(8);
 *
 *         // catch IOExceptions
 *
 *         try {
 *
 *             // write the value of i*pi to the block
 *
 *             DoubleConverter.DEFAULT_INSTANCE.writeDouble(block.dataOutputStream(), i*Math.PI);
 *         }
 *         catch (IOException ioe) {
 *             System.out.println("An I/O error occured.");
 *         }
 *
 *         // insert the block into the multi block container
 *
 *         container.insert(block);
 *     }
 *
 *     // get the ids of all elements in the container
 *
 *     Iterator iterator = container.ids();
 *
 *     // print all elements of the container
 *
 *     while (iterator.hasNext()) {
 *
 *         // get the block from the container
 *
 *         Block block = (Block)container.get(iterator.next());
 *
 *         // catch IOExceptions
 *
 *         try {
 *
 *             // print the data of the block
 *
 *             System.out.println(DoubleConverter.DEFAULT_INSTANCE.readDouble(block.dataInputStream()));
 *         }
 *         catch (IOException ioe) {
 *             System.out.println("An I/O error occured.");
 *         }
 *     }
 *
 *     // close the open container and clear its file after use
 *
 *     container.clear();
 *     container.close();
 *     container.delete();
 * </pre>
 *
 * @see ArrayList
 * @see ByteArrayInputStream
 * @see ByteArrayOutputStream
 * @see xxl.core.collections.containers.Container
 * @see DataInputStream
 * @see DataOutputStream
 * @see IOException
 * @see List
 * @see NoSuchElementException
 */
public class MultiBlockContainer extends AbstractContainer {

	/** 
	 * The primary container always stores the first part of a block.
	 */
	protected BlockFileContainer primaryContainer;

	/**
	 * The secondary container is a <tt>BlockFileContainer</tt> that is
	 * used for storing all the physical blocks of a logical block except
	 * the first. <br>
	 * When separating a logical block into physical blocks, every
	 * physical block stores the id of the next physical block. The head
	 * of this linked list is stored in the multi block container and the
	 * rest in this secondary container.
	 */
	protected BlockFileContainer secondaryContainer;
	
	/** 
	 * The size of a block (in each container) 
	 */
	protected int blockSize;

	/**
	 * The suffix of the primary container filenames.
	 */
	private static String PRIMARY_SUFFIX = "Primary";

	/**
	 * The suffix of the secondary container filenames.
	 */
	private static String SECONDARY_SUFFIX = "Secondary";

	/**
	 * Returns the number of files which are used by the container.
	 * @return number of files
	 */
	public static int getNumberOfFiles() {
		return BlockFileContainer.getNumberOfFiles()*2;
	}
	
	/**
	 * Returns a string array with the filenames which are used by the container.
	 * @param prefix the beginning of each filename.
	 * @return String array containing the filenames.
	 */
	public static String[] getFilenamesUsed(String prefix) {
		String ar1[] = BlockFileContainer.getFilenamesUsed(prefix+PRIMARY_SUFFIX);
		String ar2[] = BlockFileContainer.getFilenamesUsed(prefix+SECONDARY_SUFFIX);
		
		String ar[] = new String[ar1.length+ar2.length];
		for (int i=0; i<ar1.length; i++)
			ar[i] = ar1[i];
		for (int i=0; i<ar2.length; i++)
			ar[ar1.length+i] = ar2[i];
		
		return ar;
	}

	/**
	 * Constructs an empty MultiBlockContainer that is able to store
	 * blocks with a size larger than <tt>blockSize</tt> bytes. The given
	 * <tt>String prefix</tt> specifies the names of the files the are
	 * used for storing the elements of the container. When using existing
	 * files to store the container their data will be overwritten.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 * @param blockSize the size of the physical blocks that are used for
	 *        storing a logical block in the container.
	 * @param fso Provides an object which performs the operations on the filesystem.
	 */
	public MultiBlockContainer (String prefix, int blockSize, FilesystemOperations fso) {
		this.primaryContainer = new BlockFileContainer(prefix+PRIMARY_SUFFIX, blockSize, fso);
		this.secondaryContainer = new BlockFileContainer(prefix+SECONDARY_SUFFIX, blockSize, fso);
		this.blockSize = primaryContainer.blockSize;
	}
	
	/**
	 * Constructs an empty MultiBlockContainer that is able to store
	 * blocks with a size larger than <tt>blockSize</tt> bytes. The given
	 * <tt>String prefix</tt> specifies the names of the files the are
	 * used for storing the elements of the container. When using existing
	 * files to store the container their data will be overwritten.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 * @param blockSize the size of the physical blocks that are used for
	 *        storing a logical block in the container.
	 */
	public MultiBlockContainer (String prefix, int blockSize) {
		this(prefix,blockSize,JavaFilesystemOperations.DEFAULT_INSTANCE);
	}

	/**
	 * Constructs a MultiBlockContainer that consists of existing files
	 * given by the specified file name. Every information the container
	 * needs will be taken from the meta file.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 * @param fso Provides an object which performs the operations on the filesystem.
	 */
	public MultiBlockContainer (String prefix, FilesystemOperations fso) {
		this.primaryContainer = new BlockFileContainer(prefix+PRIMARY_SUFFIX, fso);
		this.secondaryContainer = new BlockFileContainer(prefix+SECONDARY_SUFFIX, fso);
		this.blockSize = primaryContainer.blockSize;
	}

	/**
	 * Constructs a MultiBlockContainer that consists of existing files
	 * given by the specified file name. Every information the container
	 * needs will be taken from the meta file.
	 *
	 * @param prefix specifies the names of the files the container
	 *        consists of.
	 */
	public MultiBlockContainer (String prefix) {
		this(prefix,JavaFilesystemOperations.DEFAULT_INSTANCE);
	}

	/**
	 * Returns the id of the next block in the linked list of physical
	 * block that is stored in the specified block. Because logical blocks
	 * are stored in the container as linked lists of physical blocks,
	 * every block stores the id of the next block in the linked list.
	 * This method reads an <tt>Long</tt> object from the beginning of the
	 * specified block and returns it.<br>
	 * When an <tt>IOException</tt> occurs, <tt>null</tt> is returned.
	 *
	 * @param block the <tt>Block</tt> <i>whose</i> id should be read.
	 * @return the id of the next physical block in the linked list or
	 *         <tt>null</tt> if an <tt>IOException</tt> occurs.
	 */
	protected Long getId (Block block) {
		try {
			return (Long)LongConverter.DEFAULT_INSTANCE.read(
				new DataInputStream(new ByteArrayInputStream(block.array, block.offset, block.size))
			);
		}
		catch (IOException ie) {
			return null;
		}
	}

	/**
	 * Sets the id stored in the specified block to the specified object
	 * and returns the number of bytes that are taken by the converted id.
	 * This method writes the specified object to the block by using a
	 * <tt>LongConverter</tt>.<br>
	 * When an <tt>IOException</tt> occurs, <tt>0</tt> is returned.
	 *
	 * @param block the <tt>Block</tt> <i>whose</i> id should be set.
	 * @param id the object that should be stored in the block as id of
	 *        the next physical block in the linked list.
	 * @return the number of bytes that are taken by the converted id or
	 *         <tt>0</tt> if an <tt>IOException</tt> occurs.
	 */
	protected int setId (Block block, Object id) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			LongConverter.DEFAULT_INSTANCE.write(new DataOutputStream(baos), (Long)id);
			int size = Converters.sizeOf(LongConverter.DEFAULT_INSTANCE, (Long)id);

			System.arraycopy(baos.toByteArray(), 0, block.array, block.offset, size);
			return size;
		}
		catch (IOException ie) {
			return 0;
		}
	}

	/**
	 * Returns the logical block that is given by the specified list of
	 * physical blocks. The data of the logical block starts at offset
	 * <tt>LongConverter.SIZE</tt> of the physical blocks (because the id
	 * of the next block is stored at the beginning of each block) and the
	 * given size specifies the size of the logical block.
	 *
	 * @param blockList the list that contains the physiacl blocks
	 *        representing the logical block.
	 * @param size the size of the logical block represented by the list
	 *        of physical blocks.
	 * @return the logical block that is represented by the specified list
	 *         of physical blocks.
	 */
	protected Block getLogicalBlock (List blockList, int size) {
		Block logicalBlock = new Block(new byte[size], 0, size);
		int netBlockSize = blockSize-LongConverter.SIZE;

		for (int i = 0; i<blockList.size(); i++) {
			Block physicalBlock = (Block)blockList.get(i);

			System.arraycopy(
				physicalBlock.array, physicalBlock.offset+LongConverter.SIZE,
				logicalBlock.array, logicalBlock.offset+i*netBlockSize,
				Math.min(netBlockSize, size-i*netBlockSize)
			);
		}
		return logicalBlock;
	}

	/**
	 * Returns a list of physical blocks that represent the specified
	 * logical block. The logical block is separated into physical blocks
	 * of (<code>blockSize-LongConverter.SIZE</code>) bytes. The first
	 * <tt>LongConverter.SIZE</tt> bytes of each physical block are
	 * reserved for the id of the next physical block (so the size of a
	 * physical block is <tt>blockSize</tt>).
	 *
	 * @param logicalBlock the logical block that should be separated into
	 *        physical blocks of </tt>blockSize</tt> bytes.
	 * @return a list of physical blocks that represents the specified
	 *         logical block.
	 */
	protected List getPhysicalBlocks (Block logicalBlock) {
		List blockList = new ArrayList();
		int netBlockSize = blockSize-LongConverter.SIZE;
		int size = 0, len;

		do {
			Block physicalBlock = new Block(new byte[blockSize], 0, blockSize);

			blockList.add(physicalBlock);
			System.arraycopy(
				logicalBlock.array, logicalBlock.offset+size,
				physicalBlock.array, physicalBlock.offset+LongConverter.SIZE,
				len = Math.min(netBlockSize, logicalBlock.size-size)
			);
			size += len;
		}
		while (size<logicalBlock.size);
		// System.out.println(blockList.size());
		return blockList;
	}

	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 * This implementation calls the clear methods of the primary and the
	 * secondary container.
	 */
	public void clear () {
		primaryContainer.clear();
		secondaryContainer.clear();
	}

	/**
	 * Closes the Container and releases its associated files. <br>
	 * This implementation calls the close methods of the primary and the
	 * secondary container. A closed container can be implicitly reopened by
	 * a consecutive call to one of its methods.
	 */
	public void close () {
		primaryContainer.close();
		secondaryContainer.close();
	}

	/**
	 * Returns true if the container contains a block for the identifier
	 * <tt>id</tt>.<br>
	 * This call is forwarded to the primary Container.
	 *
	 * @param id identifier of the block.
	 * @return true if the container has updated a block for the specified
	 *         identifier.
	 */
	public boolean contains (Object id) {
		return primaryContainer.contains(id);
	}
	
	/**
	 * Returns the logical block associated to the identifier <tt>id</tt>.
	 * An exception is thrown when the desired block is not found via <tt>contains</tt>.
	 * In this implementation the parameter <tt>unfix</tt> has no function because
	 * the container is unbuffered.
	 *
	 * @param id identifier of the logical block.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the logical block associated to the specified identifier.
	 * @throws NoSuchElementException if the desired block is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		List blockList = new ArrayList();
		Block physicalBlock = (Block)primaryContainer.get(id, unfix);

		for (;;) {
			blockList.add(physicalBlock);
			id = getId(physicalBlock);
			if (((Long)id).longValue()<0)
				return getLogicalBlock(blockList, (int)(-((Long)id).longValue()-1));
			physicalBlock = (Block)secondaryContainer.get(id, unfix);
		}
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 * <br>
	 * This call is forwarded to the primary Container.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator ids () {
		return primaryContainer.ids();
	}

	/**
	 * Inserts a new logical block into the container and returns the
	 * identifier of the block in the container. <br>
	 * The logical block is separated into physical blocks of
	 * (<code>blockSize-LongConverter.SIZE</code>) bytes by using the
	 * getPhysicalBlocks method. Thereafter the id of the last block in
	 * the list of physical blocks is set to
	 * <code>new Long(-1-logicalBlock.size)</code> and the block is
	 * inserted into the secondary container. The id of the previous block
	 * in the list is set to the id returned by the insert method and it
	 * is also inserted into the secondary container and so on. When
	 * reaching the first physical block in the list, its id is set to the
	 * id returned by the insert method and it is inserted into the
	 * primary container. At last, the id that is returned by the insert
	 * method of the primary container is returned as result of this
	 * method. The parameter <code>unfix</code> has no function because this container
	 * is unbuffered.
	 *
	 * @param object the new logical block that should be inserted into
	 *        the container.
	 * @param unfix signals a buffered container whether the block can
	 *        be removed from the underlying buffer.
	 * @return the identifier of the logical block.
	 */
	public Object insert (Object object, boolean unfix) {
		Block logicalBlock = (Block)object;
		Object id = new Long(-1-logicalBlock.size);
		List blockList = getPhysicalBlocks((Block)object);

		for (int i=blockList.size(); --i>=0;) {
			Block physicalBlock = (Block)blockList.get(i);

			setId(physicalBlock, id);
			id = i==0 ? primaryContainer.insert(physicalBlock, true) : secondaryContainer.insert(physicalBlock, true);
		}
		return id;
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to <tt>insert</tt> or <tt>reserve</tt> and hasn't been removed so far.
	 * <br>
	 * This call is forwarded to the primary Container.
	 *
	 * @param id the id to be checked.
	 * @return <tt>true</tt> exactly if the <tt>id</tt> is still in use.
	 */
	public boolean isUsed (Object id) {
		return primaryContainer.isUsed(id);
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Since the identifier may have an arbitrary type (which has to be
	 * known in the container), the container has to provide such a method
	 * when the data is not stored in main memory.
	 * <br>
	 * This call is forwarded to the primary Container.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	public FixedSizeConverter objectIdConverter () {
		return primaryContainer.objectIdConverter();
	}

	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * This call is forwarded to the primary Container.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return primaryContainer.getIdSize();
	}
	
	/**
	 * Removes the logical block with identifier <tt>id</tt>. An exception
	 * is thrown when a block with an identifier <tt>id</tt> is not in the
	 * container. After a call of <tt>remove()</tt> all the iterators (and
	 * cursors) can be in an invalid state.<br>
	 * This implementation follows the linked list of physical blocks that
	 * represents the logical block and removes every physical block from
	 * the primary and secondary container.
	 *
	 * @param id an identifier of a logical block.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove (Object id) throws NoSuchElementException {
		if (primaryContainer.contains(id)) {
			Block physicalBlock = (Block)primaryContainer.get(id, true);

			primaryContainer.remove(id);
			for (;;) {
				id = getId(physicalBlock);
				if (((Long)id).longValue()<0)
					break;
				physicalBlock = (Block)secondaryContainer.get(id);
				secondaryContainer.remove(id);
			}
		}
		else
			primaryContainer.remove(id);
	}

	/**
	 * Reserves an id for subsequent use. The container may or may not
	 * need an object to be able to reserve an id, depending on the
	 * implementation. If so, it will call the parameterless function
	 * provided by the parameter <tt>getObject</tt>.
	 * <br>
	 * This call is forwarded to the primary Container.
	 *
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved.
	 * @return the reserved id.
	*/
	public Object reserve (Function getObject) {
		return primaryContainer.reserve(getObject);
	}

	/**
	 * Returns the number of elements of the container.
	 * <br>
	 * This call is forwarded to the primary Container.
	 *
	 * @return the number of elements.
	 */
	public int size () {
		return primaryContainer.size();
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if a logical block with an identifier
	 * <tt>id</tt> does not exist in the container.<br>
	 * This implementation separates the specified logical block into a
	 * list of logical blocks. Thereafter it follows the linked list of
	 * physical blocks that represents the logical block and updates every
	 * existing physical block by the new physical block. When the logical
	 * block to update is larger than the new logical block (the linked
	 * list contains more physical blocks), the remaining old physical
	 * blocks are removed from the secondary container. When the new
	 * logical block is larger than the logical block to update, the
	 * remaining new physical blocks are inserted into the secondary
	 * container. The parameter unfix has no function because this
	 * container is unbuffered.
	 *
	 * @param id identifier of the element.
	 * @param object the new logical block that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals a buffered container whether the block can be
	 *        removed from the underlying buffer.
	 * @throws NoSuchElementException if a block with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		Block logicalBlock = (Block)object;
		List blockList = getPhysicalBlocks(logicalBlock);

		for (int i = 0; i<blockList.size(); i++) {
			Block physicalBlock = (Block)blockList.get(i);
			Long nextId = (i==0 && !primaryContainer.contains(id))? new Long(-1): getId((Block)(i==0 ? primaryContainer.get(id, true) : secondaryContainer.get(id, true)));

			if (i==blockList.size()-1)
				while (nextId.longValue()>=0) {
					Block block = (Block)secondaryContainer.get(nextId);

					secondaryContainer.remove(nextId);
					nextId = getId(block);
				}
			if (nextId.longValue()<0) {
				nextId = new Long(-1-logicalBlock.size);
				for (int j = blockList.size(); --j>i;) {
					Block block = (Block)blockList.get(j);

					setId(block, nextId);
					nextId = (Long)secondaryContainer.insert(block);
				}
				blockList.clear();
			}
			setId(physicalBlock, nextId);
			if (i==0)
				primaryContainer.update(id, physicalBlock, unfix);
			else
				secondaryContainer.update(id, physicalBlock, unfix);
			id = nextId;
		}
	}

	/**
	 * Deletes the container. If necessary, the container is closed before.
	 */
	public void delete() {
		close();
		primaryContainer.delete();
		secondaryContainer.delete();
	}
}
