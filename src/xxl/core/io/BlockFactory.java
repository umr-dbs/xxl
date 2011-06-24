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

import java.util.Stack;

/**
 * This class provides a block factory, i.e. this class manages the
 * allocation of blocks on an underlying byte array.<p>
 *
 * Blocks allocated by the same block factory always have the same size.
 * Internally, the block factory separates the underlying byte array
 * logically in <i>smaller</i> arrays of <tt>blockSize</tt> bytes and
 * stores the first indices of this arrays in a stack. Every time the
 * allocate method is called, the block factory checks whether the stack
 * stores another index and returns a new block of <tt>blockSize</tt>
 * bytes that starts at this index or returns <tt>null</tt> if the stack
 * is empty.<br>
 * When releasing a block allocated by this factory its <i>offset</i> (the
 * first index of the block in the byte array) is pushed on the stack.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new byte array of 100 bytes
 *
 *     byte [] array = new byte [100];
 *
 *     // create a new block factory that creates block of 20 bytes
 *
 *     BlockFactory factory = new BlockFactory(array, 20);
 *
 *     // create a new array of 6 blocks
 *
 *     Block [] blocks = new Block [6];
 *
 *     // try to allocate 6 blocks
 *
 *     for (int i = 0; i < 6; i++) {
 *         blocks[i] = factory.allocate();
 *         if (blocks[i]==null) {
 *             System.out.println("array is exhausted");
 *             break;
 *         }
 *         else
 *             System.out.println("block allocated");
 *     }
 *
 *     // release a block
 *
 *     blocks[0].release();
 *
 *     // try again to allocate the sixth block
 *
 *     blocks[5] = factory.allocate();
 *     if (blocks[5] == null)
 *         System.out.println("array is exhausted");
 *     else
 *         System.out.println("block allocated");
 *
 *     // release the rest of the blocks
 *
 *     for (int i = 1; i < 6; i++)
 *         blocks[i].release();
 * </pre>
 *
 * @see Stack
 */
public class BlockFactory {

	/**
	 * The underlying byte array that stores the blocks allocated by this
	 * block factory.
	 */
	protected byte [] array;

	/**
	 * The size of a block allocated by this block factory. Blocks
	 * allocated by the same block factory always have the same size.
	 */
	protected int blockSize;

	/**
	 * This stack is used for storing the offsets of empty blocks. Every
	 * time a new block is allocated, its offset is popped out of the
	 * stack and every time a block allocated by this factory is released,
	 * its offset is pushed on the stack. For this reason an empty stack
	 * signals that the underlying byte array of this block factory is
	 * exhausted.
	 */
	Stack stack = new Stack();

	/**
	 * Constructs a new BlockFactory that stores blocks of the specified
	 * size in the specified byte array. Internally the byte array is
	 * logically separated in <i>smaller</i> arrays of <tt>blockSize</tt>
	 * bytes and the first indices of this arrays are stored in the stack.
	 *
	 * @param array the byte array that is internally used for storing the
	 *        blocks allocated by this block factory.
	 * @param blockSize the size of the blocks allocated by this block
	 *        factory.
	 */
	public BlockFactory (byte [] array, int blockSize) {
		this.array = array;
		this.blockSize = blockSize;
		for (int i = 0; i<array.length/blockSize;)
			stack.push(new Integer(i++*blockSize));
	}

	/**
	 * Allocates and returns a new block on the underlying array. When
	 * calling this method it checks wheteher the stack stores another
	 * index and returns a new block of <tt>blockSize</tt> bytes that
	 * starts at this index or returns null if the stack is empty. When
	 * releasing a block allocated by this factory its <i>offset</i> (the
	 * first index of the block in the byte array) is pushed on the stack.
	 *
	 * @return a new block that is stored on the underlying byte array or
	 *         null if the byte array is exhausted.
	 */
	public Block allocate () {
		if (stack.isEmpty())
			return null;
		else {
			int offset = ((Integer)stack.pop()).intValue();

			return new Block(array, offset, blockSize) {
				public void release () {
					super.release();
					stack.push(new Integer(this.offset));
				}
			};
		}
	}
}
