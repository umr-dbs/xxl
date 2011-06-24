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

/**
 * This class implements the interface FilesystemOperations with normal Java method calls.
 *
 * @see xxl.core.collections.containers.io.BlockFileContainer
 * @see xxl.core.collections.containers.io.MultiBlockContainer
 */
public class JavaFilesystemOperations implements FilesystemOperations {
	/**
	 * Mode constant: Using normal Java operations.
	 */
	public static final int MODE_NORMAL = 0;

	/**
	 * Mode constant: Each file is opened with "rws"-mode (synchronized).
	 */
	public static final int MODE_UNBUFFERED = 1;

	/**
	 * Mode constant: Each file is opened using a {link xxl.core.io.BufferedRandomAccessFile}.
	 */
	public static final int MODE_BUFFERED = 2;

	/**
	 * The default Java implementation of the FilesystemOperation interface.
	 */
	public static final FilesystemOperations DEFAULT_INSTANCE = new JavaFilesystemOperations();
	
	/**
	 * The default Java implementation of the FilesystemOperation interface for unbuffered io.
	 */
	public static final FilesystemOperations UNBUFFERED_DEFAULT_INSTANCE = new JavaFilesystemOperations(MODE_UNBUFFERED);
		
	/**
	 * Mode currently used.
	 */
	protected int mode;
	
	/**
	 * For buffering (MODE_BUFFERED): size of the blocks in the buffer (initially 512).
	 */
	protected int bufferBlockSize = 512;

	/**
	 * For buffering (MODE_BUFFERED): number of blocks in the buffer (initially 8). 
	 */
	protected int bufferNumberOfBlocks = 8;
	
	/**
	 * Constructs a FilesystemOperations instance using BufferedRandomAccessFiles.
	 * 
	 * @param mode mode of the RandomAccessFiles (MODE_NORMAL, MODE_UNBUFFERED, MODE_BUFFERED).
	 *	Here only MODE_BUFFERED is allowed.
	 * @param bufferBlockSize size of the blocks in the buffer.
	 * @param bufferNumberOfBlocks number of blocks in the buffer.
	 */
	public JavaFilesystemOperations(int mode, int bufferBlockSize, int bufferNumberOfBlocks) {
		this.mode = mode;
		if (mode!=MODE_BUFFERED)
			throw new RuntimeException("Usage of this constructor is not allowed in the not buffered mode");
		this.bufferBlockSize = bufferBlockSize;
		this.bufferNumberOfBlocks = bufferNumberOfBlocks;
	}
	
	/**
	 * Constructs a FilesystemOperations instance in the specified mode.
	 * 
	 * @param mode mode of the RandomAccessFiles (MODE_NORMAL, MODE_UNBUFFERED, MODE_BUFFERED).
	 */
	public JavaFilesystemOperations(int mode) {
		this.mode = mode;
	}
	
	/**
	 * Constructs a FilesystemOperations instance in the normal mode.
	 */
	public JavaFilesystemOperations() {
		this(MODE_NORMAL);
	}
	
	/**
	 * Opens a file and returns a RandomAccessFile.
	 * @param fileName the name of the file.
	 * @param mode the access mode ("r" or "rw").
	 * @return the new RandomAccessFile or null, if the operation was not successful.
	 */
	public RandomAccessFile openFile(String fileName, String mode) {
		try {
			switch (this.mode) {
				case MODE_NORMAL: return new RandomAccessFile(fileName, mode);
				case MODE_UNBUFFERED:	return new RandomAccessFile(fileName, "rws"); // always uses rws
				case MODE_BUFFERED:	return new BufferedRandomAccessFile(fileName, mode, new LRUBuffer(bufferNumberOfBlocks), bufferBlockSize);
				default: return null;
			}
		}
		catch (IOException ie) {
		   ie.printStackTrace(System.err);
           return null;
		}
	}

	/**
	 * Determines if a file exists or not.
	 * @param fileName the name of the file to be checked.
	 * @return true iff the file exists.
	 */
	public boolean fileExists(String fileName) {
		return new File(fileName).exists();
	}

	/**
	 * Renames the name of a file to a new name.
	 * @param oldName the old name of the file.
	 * @param newName the new name of the file.
	 * @return true iff the file exists.
	 */
	public boolean renameFile(String oldName, String newName) {
		return new File(oldName).renameTo(new File(newName));
	}

	/**
	 * Deletes a file.
	 * @param fileName the name of the file.
	 * @return true iff the operation completed successfully.
	 */
	public boolean deleteFile(String fileName) {
		return new java.io.File(fileName).delete();
	}
}
