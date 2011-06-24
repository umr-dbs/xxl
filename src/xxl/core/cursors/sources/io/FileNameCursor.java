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

package xxl.core.cursors.sources.io;

import java.io.File;
import java.io.FileFilter;

import xxl.core.collections.queues.ArrayQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.cursors.AbstractCursor;

/**
 * This class provides an implementation of the
 * {@link xxl.core.cursors.Cursor cursor} interface that iterates over the
 * files contained in the specified path and its subdirectories and returns
 * their names. The names are given relative to the specified path.
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class FileNameCursor extends AbstractCursor<String> {

	/**
	 * A queue that is used to store the files.
	 */
	protected Queue<File> files;

	/**
	 * The directory that should be searched for files.
	 */
	protected File path;

	/**
	 * A file filter that determines whether a special file name should be
	 * returned or a special subdirectory should be searched.
	 */
	protected FileFilter fileFilter;

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path. The specified file filter decides, which files
	 * should be returned and which subdirectories should be searched.
	 *
	 * @param path the directory to search for files.
	 * @param fileFilter the file filter that decides which files should be
	 *        returned and which subdirectories should be searched.
	 */
	public FileNameCursor(File path, FileFilter fileFilter) {
		files = new ArrayQueue<File>();
		this.path = path;
		this.fileFilter = fileFilter;
	}

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path. The specified file filter decides, which files
	 * should be returned and which subdirectories should be searched. The path
	 * is not allowed to contain wildcards (it must be a valid directory, i.e.,
	 * ".").
	 *
	 * @param path the directory to search for files.
	 * @param fileFilter the file filter that decides which files should be
	 *        returned and which subdirectories should be searched.
	 */
	public FileNameCursor(String path, FileFilter fileFilter) {
		this(new File(path), fileFilter);
	}

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path. The specified boolean flag decides whether
	 * subdirectories should be searched recursively.
	 *
	 * @param path the path to search for files.
	 * @param recursive decides whether the cursor should search for files in
	 *        subdirectories.
	 */
	public FileNameCursor(final File path, final boolean recursive) {
		this(
			path,
			new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.isFile() || pathname.equals(path) || recursive;
				}
			}
		);
	}

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path. The specified boolean flag decides whether
	 * subdirectories should be searched recursively. The path is not allowed
	 * to contain wildcards (it must be a valid directory, i.e., ".").
	 *
	 * @param path the path to search for files.
	 * @param recursive decides whether the cursor should search for files in
	 *        subdirectories.
	 */
	public FileNameCursor(String path, boolean recursive) {
		this(new File(path), recursive);
	}

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path and its subdirectories.
	 *
	 * @param path the path to search for files.
	 */
	public FileNameCursor(File path) {
		this(path, true);
	}

	/**
	 * Creates a new file-name cursor that iterates over the files contained in
	 * the specified path and its subdirectories. The path is not allowed to
	 * contain wildcards (it must be a valid directory, i.e., ".").
	 *
	 * @param path the path to search for files.
	 */
	public FileNameCursor(String path) {
		this(new File(path));
	}

	/**
	 * Opens the file-name cursor, i.e., signals the cursor to reserve
	 * resources, open files, etc. Before a cursor has been opened calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Therefore <code>open</code> must be called
	 * before a cursor's data can be processed. Multiple calls to
	 * <code>open</code> do not have any effect, i.e., if <code>open</code> was
	 * called the cursor remains in the state <i>opened</i> until its
	 * <code>close</code> method is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (!isOpened) {
			files.open();
			files.enqueue(path);
		}
		super.open();
	}
	
	/**
	 * Closes the file-name cursor, i.e., signals the cursor to clean up
	 * resources, close files, etc. When a cursor has been closed calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Multiple calls to <code>close</code> do not
	 * have any effect, i.e., if <code>close</code> was called the cursor
	 * remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (!isClosed)
			files.close();
		super.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the file-name cursor has more elements.
	 */
	protected boolean hasNextObject() {
		while (!files.isEmpty())
			if (files.peek().isDirectory())
				Queues.enqueueAll(files, files.dequeue().listFiles(fileFilter));
			else
				return true;
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the file-name cursor's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected String nextObject() {
		return files.dequeue().getPath().substring(path.getPath().length());
	}

	/**
	 * Resets the file-name cursor to its initial state such that the caller is
	 * able to traverse the underlying data structure again without
	 * constructing a new cursor (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the file-name cursor.
	 */
	public void reset() throws UnsupportedOperationException{
		super.reset();
		files.clear();
		files.enqueue(path);
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the file-name cursor. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the file-name cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
