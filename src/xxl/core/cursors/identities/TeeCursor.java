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

package xxl.core.cursors.identities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.collections.queues.Queue;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.wrappers.QueueCursor;
import xxl.core.io.Buffer;
import xxl.core.io.BufferedRandomAccessFile;
import xxl.core.io.converters.Converter;
import xxl.core.util.WrappingRuntimeException;

/**
 * A tee-cursor provides a method to duplicate a given iteration. That means
 * the iteration given to a constructor of this class can be consumed in the
 * standard way but all delivered elements will also be inserted in the
 * specified storage area, that is given to this tee-cursor, too. Then the
 * given storage area can be traversed calling the <code>cursor</code> method
 * on a tee-cursor.
 * 
 * <p>If you understand this class as an operator in an operator tree, this
 * operator has one input iterations, namley the given iteration in the
 * constructor, and is able to deliver several output iterations.
 * The tee-cursor itself is one of this output iterations. By calling the
 * method {@link #cursor()} one can get a further output iteration. So a lot of
 * output iterations can be produced, if the storage area can be traversed more
 * than one time, and for that reason the input iteration's elements are
 * duplicated.</p>
 * 
 * <p>The tee-cursor is closed when, at a certain time, itself and all cursors
 * returned by {@link #cursor()} are closed. This can be changed by setting the
 * <code>explicitClose</code> parameter to <code>true</code> in the constructor 
 * {@link #TeeCursor(Iterator, TeeCursor.StorageArea, boolean)}. Then, only a
 * call to {@link #closeAll()} closes the input and the StorageArea.</p>
 * 
 * <p><b>Note:</b> All output iterators are cursors that do not support the 
 * <code>remove</code> and  <code>update</code> operation. The duplication of
 * the input iteration is fulfilled by lazy evaluation.</p>
 * 
 * <p>Depending on the implementation of the inner interface
 * <i>StorageArea</i>, a tee-cursor can be based on queues, lists or files.
 * This class provides standard implementations for the these kinds of storage
 * areas:
 * <ul>
 *     <li>
 *         {@link ListStorageArea}
 *     </li>
 *     <li>
 *         {@link QueueStorageArea}
 *     </li>
 *     <li>
 *         {@link FileStorageArea}
 *     </li>
 * </ul></p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class TeeCursor<E> extends AbstractCursor<E> {

	/**
	 * This interface is used to store the objects delivered by the iteration
	 * specified in a tee-cursor's constructor and to return them, if the
	 * method <code>cursor</code> of the class implementing this interface is
	 * called. Furthermore the behaviour of a storage area can be determined
	 * implementing the method <code>isSingleton</code> with the intention to
	 * show if the storage area can be traversed more than one time.
	 * 
	 * @param <E> the type of the elements this storage area is able to store.
	 */
	public interface StorageArea<E> {

		/**
		 * Inserts the given object into the storage area.
		 *
		 * @param o the object to be inserted in the storage area.
		 */
		public abstract void insert(final E o);

		/**
		 * Returns an iteration containing all objects that are currently
		 * belonging to this storage area.
		 *
		 * @return an iteration that contains all objects of the storage area.
		 */
		public abstract Cursor<E> cursor();

		/**
		 * If a storage area can be traversed exactly one time this method
		 * should return <tt>true</tt>, otherwise <tt>false</tt>.
		 * 
		 * @return <tt>true</tt> if the storage area can be traversed exaclty
		 *         one time, <tt>false</tt> otherwise .
		 */
		public abstract boolean isSingleton();

		/**
		 * Closes the storage area. Signals the storage area to clean up
		 * resources, close files, etc. After a call to <code>close</code> a
		 * call to <code>close</code> is not guarantied to yield proper
		 * results. Multiple calls to <code>close</code> do not have any
		 * effect, i.e., if <code>close</code> was called the storage area
		 * remains in the state <i>closed</i>.
		 */
		public abstract void close();
		
	}

	
	/**
	 * A {@link TeeCursor.StorageArea} that stores elemenst in a list.
	 *  
	 * @param <E> the type of the elements this storage area is able to store.
	 */
	public static class ListStorageArea<E> implements StorageArea<E> {
		
		/**
		 * The list used to store elements. 
		 */
		protected List<E> list;
		
		/**
		 * Creates a new ListStorageArea.
		 * 
		 * @param list a list to store elements in
		 */
		public ListStorageArea(List<E> list) {
			this.list = list;
		}
		
		/**
		 * Adds the object o to the end of the list.
		 * 
		 * @param o the element to insert
		 */ 
		public void insert(final E o) {
			list.add(o);
		}

		/**
		 * Returns a cursor that delivers the list's elements from its
		 * beginning. This has to be implemented separately because otherwise a 
		 * ConcurrentModificationException would be thrown, if several 
		 * iterators work on the same list.
		 * 
		 * @return a cursor delivering the list's elements
		 */
		public Cursor<E> cursor() {
			return new AbstractCursor<E>() {
				int index = 0;
				
				@Override
				public boolean hasNextObject() {
					return list.size() > index;
				}
				
				@Override
				public E nextObject() {
					return list.get(index++);
				}
				
				@Override
				public boolean supportsReset() {
					return true;
				}
				
				@Override
				public void reset() {
					index=0;
				}
			};
		}

		/**
		 * As reading from a list is not destroying, it can be traversed by 
		 * more than one iterator and this StorageArea is not singleton.
		 * 
		 * @return <code>false</code>
		 */ 
		public boolean isSingleton () {
			return false;
		}

		/**
		 * Clears the list to release resources.
		 */ 
		public void close () {
			list.clear();
		}
	}

	/**
	 * A {@link TeeCursor.StorageArea} that stores elemenst in a queue. 
	 * If you uses this class, it is highly recommended that you use
	 * an order preserving queue. If not, the ordering of the elements
	 * in the resulting cursor cannot be predicted.
	 * 
	 * @param <E> the type of the elements this storage area is able to store.
	 */
	public static class QueueStorageArea<E> implements StorageArea<E> {

		/**
		 * The queue used to store elements. The queue should be order
		 * preserving.
		 */
		protected Queue<E> queue;

		/**
		 * Creates a new QueueStorageArea based on a queue (should be order
		 * preserving). 
		 * 
		 * @param queue a queue to store elements in.
		 */		
		public QueueStorageArea(Queue<E> queue) {
			this.queue = queue;
		}

		/**
		 * Adds the object o to the queue, i.e. calls
		 * <code>queue.enqueue(o)</code>.
		 * 
		 * @param o the element to insert
		 */ 
		public void insert(final E o) {
			queue.enqueue(o);
		}

		/**
		 * Returns a cursor that delivers the queue's elements from its
		 * beginning. As this deletes them from the queue, this StorageArea is
		 * singleton.
		 * 
		 * @return a cursor delivering the queue's elements.
		 */
		public Cursor<E> cursor() {
			return new QueueCursor<E>(queue);
		}

		/**
		 * As reading from a queue is destroying, it cannot be traversed by 
		 * more than one iterator and this StorageArea is singleton.
		 * 
		 * @return <code>true</code>.
		 */ 
		public boolean isSingleton() {
			return true;
		}

		/**
		 * Clears the queue to release resources.
		 */ 
		public void close() {
			queue.clear();
			queue.close();
		}
	}

	/**
	 * A {@link TeeCursor.StorageArea} that stores elemenst in a file. 
	 * 
	 * @param <E> the type of the elements this storage area is able to store.
	 */
	public static class FileStorageArea<E> implements StorageArea<E> {
		
		/**
		 * The {@link BufferedRandomAccessFile} used to store the objects in
		 * this StorageArea.
		 */
		protected BufferedRandomAccessFile braf;

		/**
		 * The File used for {@link #braf}; 
		 */
		protected File file;
		
		
		/**
		 * A suitable converter for the objects to store. 
		 */
		protected Converter<E> converter;
		
		
		/**
		 * A pointer indicating where the next write operation takes place.
		 */
		private long writePointer = 0;
			
		/**
		 * Creates a new FileStorageArea.
		 * 
		 * @param file an instance of the class {@link java.io.File}
		 *        representing the storage area that will be used in the method
		 *        {@link #cursor()} to deliver a further output-iteration of
		 *        this operator.
		 * @param converter the converter used to serialize the input
		 *        iterations elements.
		 * @param buffer the buffer that is used for buffering the file.
		 * @param blockSize the size of a block of buffered bytes.
		 */
		public FileStorageArea(final File file, final Converter<E> converter, Buffer<?, ?, ? super E> buffer, int blockSize) {
			this.file = file;
			this.converter = converter;
			try {
				braf = new BufferedRandomAccessFile(file, "rw", buffer, blockSize);
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
		}		

		/**
		 * Stored the object o in the file.
		 * 
		 * @param o the element to insert
		 */ 
		public void insert(final E o) {
			try {
				braf.seek(writePointer); // setting writePointer to correct position
				converter.write(braf, o); // writing object
				writePointer = braf.getFilePointer(); // saving current position
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
		}

		
		/**
		 * Returns a cursor that delivers the file's elements from its
		 * beginning. As this does not delete them from the file, this
		 * StorageArea is not singleton.
		 * 
		 * @return a cursor delivering the file's elements
		 */
		public Cursor<E> cursor() {
			return new AbstractCursor<E>() {

				private long readPointer = 0;
				private E next;

				@Override
				public boolean hasNextObject() {
					try {
						braf.seek(readPointer); // setting readPointer to correct position
						next = converter.read(braf); // reading object
						readPointer = braf.getFilePointer(); // storing readPointer
						return true;
					}
					catch (IOException ioe) {
						return  false;
					}
				}

				@Override
				public E nextObject() {
					return next;
				}
			};
		}

		/**
		 * As reading from the file is not destroying, it can be traversed by 
		 * more than one iterator and this StorageArea is not singleton.
		 * 
		 * @return <code>false</code>
		 */
		public boolean isSingleton() {
			return false;
		}

		/**
		 * Closes and deletes the file to release resources.
		 */ 
		public void close() {
			try {
				braf.close(); // closing BufferedRandomAccess file
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
			file.delete(); // deleting file
		}
	}

	/**
	 * The input cursor.
	 */
	protected Cursor<? extends E> inCursor;
			
	/**
	 * Counter determining how often next has been invoked on the underlying 
	 * cursor {@link #inCursor}.
	 */
	protected long inCursorCounter = 0;

	/**
	 * The storage area where the input iteration's elements are buffered.
	 */
	protected StorageArea<E> storageArea;
		
	/**
	 * Counter determining how many output cursors have been generated.
	 */
	protected int numberOfCursors=0;
		
	/**
	 * List of cursors over the {@link #storageArea}, one for each output
	 * except for the {@link #leader}, whos cursor is <code>null</code>.
	 */
	protected List<Cursor<E>> storageAreaCursors = new ArrayList<Cursor<E>>();
	
	/**
	 * The index of the output cursor whichs next method has mostly been
	 * called, i.e., the cursor currently consuming the input
	 * {@link #inCursor}. 
	 */
	protected int leader = 0;

	/**
	 * The output cursor used for returning elements.
	 */
	protected Cursor<E> outCursor;

	/**
	 * List of cursors returned by {@link #cursor()}.
	 */
	protected List<Cursor<E>> outCursors = new ArrayList<Cursor<E>>();
	
	/**
	 * Counter determining how many output-cursors have been closed.
	 */
	protected int numberOfClosedCursors;
	
	/**
	 * Flag determining if the tee-cursor should return new 
	 * cursors if all outputs were closed.
	 */
	protected boolean explicitClose;
	

	/**
	 * Creates a new tee-cursor and duplicates the input using the specified
	 * storage area.
	 *
	 * @param iterator the iteration representing the input of this operator.
	 * @param storageArea the storage area that will be used to buffer the
	 *        input iterator
	 * @param explicitClose flag determining if the tee-cursor should return
	 *        new cursors if all outputs were closed. 
	 */
	public TeeCursor(Iterator<? extends E> iterator, StorageArea<E> storageArea, boolean explicitClose) {
		inCursor = Cursors.wrap(iterator);
		this.storageArea = storageArea;
		this.explicitClose = explicitClose;
		numberOfClosedCursors = 0;
	}

	/**
	 * Creates a new tee-cursor and duplicates the input using a 
	 * {@link ListStorageArea} to store the elements.
	 *
	 * @param iterator the iteration representing the input of this operator.
	 * @param list the list to store elements in
	 */
	public TeeCursor(Iterator<? extends E> iterator, List<E> list) {
		this (iterator, new ListStorageArea<E>(list), false);
	}
		
	/**
	 * Creates a new tee-cursor and duplicates the input using a 
	 * {@link ListStorageArea} with an ArrayList to store the elements.
	 *
	 * @param iterator the iteration representing the input of this operator.
	 */
	public TeeCursor(Iterator<? extends E> iterator) {
		this (iterator, new ArrayList<E>());
	}

	/**
	 * Creates a new tee-cursor and duplicates the input using the given
	 * queue. 
	 *
	 * @param iterator the iteration representing the input of this operator.
	 * @param queue the queue used for storing elements. The queue should be
	 *        order preserving. 
	 */
	public TeeCursor(Iterator<? extends E> iterator, Queue<E> queue) {
		this (iterator, new QueueStorageArea<E>(queue), false);
	}
	
	/**
	 * Creates a new tee-cursor and duplicates the input using the given file.
	 *
	 * @param iterator the iteration representing the input of this operator.
	 * @param file an instance of the class {@link java.io.File} representing
	 *        the storage area that will be used in the method
	 *        {@link #cursor()} to deliver a further output-iteration of this
	 *        operator.
	 * @param converter the converter used to serialize the input iterations
	 *        elements.
	 * @param buffer the buffer that is used for buffering the file.
	 * @param blockSize the size of a block of buffered bytes.
	 */
	public TeeCursor(Iterator<? extends E> iterator, final File file, final Converter<E> converter, Buffer<?, ?, E> buffer, int blockSize) {
		this (iterator, new FileStorageArea<E>(file,converter,buffer,blockSize), false);
	}
	
	/**
	 * Opens the tee-cursor, i.e., signals the cursor to reserve resources.
	 * Before a cursor has been opened calls to methods like <code>next</code>
	 * or <code>peek</code> are not guaranteed to yield proper results.
	 * Therefore <code>open</code> must be called before a cursor's data can be
	 * processed. Multiple calls to <code>open</code> do not have any effect,
	 * i.e., if <code>open</code> was called the cursor remains in the state
	 * <i>opened</i> until its <code>close</code> method is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	@Override
	public void open() {
		if (isOpened) return;
		super.open();
		outCursor = cursor();
		outCursor.open();
	}

	/**
	 * Returns a new {@link xxl.core.cursors.Cursor cursor} delivering exactly
	 * the same elements as the decorated input iteration returned by a call to
	 * the <code>next</code> method, because these elements have been inserted
	 * in the user defined storage area. Only the order, the elements may be
	 * returned in, can be different, because calls to <code>next</code> and
	 * <code>peek</code> are delegated from the cursor to the underlying
	 * storage area that may realize a special strategy. The
	 * <code>remove</code> and <code>update</code> operation of the returned
	 * cursor throws an {@link java.lang.UnsupportedOperationException} due to
	 * the fact that a deletion of an element contained in a duplicated output
	 * iteration is not well defined and would produce a
	 * {@link java.util.ConcurrentModificationException}, because all other
	 * iterations get inconsistent.
	 * 
	 * <p><b>Note</b>, that this method checks if the underlying storage area
	 * realizes a Singleton-Design-Pattern, so only one instance of a cursor
	 * may be returned by this method. Every further call throws an
	 * {@link java.lang.IllegalStateException}.
	 *
	 * @return a cursor contaning the elements of the input iteration
	 * @throws IllegalStateException if the storage area can only be traversed
	 *         for one time, i.e., the <code>isSingleton</code> method returns
	 *         <code>true</code>, but it is traversed for a second time.
	 */
	public Cursor<E> cursor() throws IllegalStateException {
		if (storageArea.isSingleton() && numberOfCursors == 2)
			throw new IllegalStateException("The storage area is not allowed to be a singleton storage area.");
		if (numberOfClosedCursors > 0 && !explicitClose && numberOfCursors == numberOfClosedCursors)
			throw new IllegalStateException("No more output cursors can be returned after all others have been closed.");
		numberOfCursors++;	
		storageAreaCursors.add(null);

		Cursor<E> result = new AbstractCursor<E>() {
			int id = numberOfCursors-1;
			long counter=0;

			@Override
			public void open() {
				if (isOpened)
					return;
				super.open();
				if (id > 0)
					storageAreaCursors.set(id, storageArea.cursor()); 
			}

			@Override
			public boolean hasNextObject() {
				return counter == inCursorCounter ? 
					inCursor.hasNext() :
					storageAreaCursors.get(id).hasNext();
			}

			@Override
			public E nextObject() {
				if (counter == inCursorCounter) {
					if (leader != id) {
						Cursor<E> myCursor = storageAreaCursors.get(id);
						if (leader == -1)
							myCursor.close();
						else
							storageAreaCursors.set(leader, myCursor);
						storageAreaCursors.set(id, null);
						leader = id;
					}
					if (inCursor.hasNext()) {
						E next = inCursor.next();
						storageArea.insert(next);
						inCursorCounter++;
						counter++;
						return next;
					}
					else
						throw new NoSuchElementException();
				}
				counter++;
				return storageAreaCursors.get(id).next();
			}

			@Override
			public void close() {
				if (isClosed)
					return;
				numberOfClosedCursors++;
				if (numberOfClosedCursors == numberOfCursors && !explicitClose)
					closeAll();
				if (leader != id) {
					Cursor<E> cursor = storageAreaCursors.get(id);
					if (cursor != null)
						cursor.close();
				}	
				else
					leader = -1;
			}
		};
		outCursors.add(result);
		return result;
	}

	/**
	 * Closes the tee-cursor. Signals it to clean up resources. This does not
	 * always close the input cursor, which is closed by {@link #closeAll()}
	 * or, if {#explicitClose} is <code>false</code>, when this and all cursors
	 * returned by {@link #cursor()} have been closed. After a call to
	 * <code>close</code> calls to methods like <code>next</code> or
	 * <code>peek</code> are not guarantied to yield proper results. Multiple
	 * calls to <code>close</code> do not have any effect, i.e., if
	 * <code>close</code> was called the tee cursor remains in the state
	 * <i>closed</i>.
	 */
	@Override
	public void close() {
		if (isClosed)
			return;
		super.close();
		if (outCursor != null) 
			outCursor.close();
	}
	
	/**
	 * Closes this tee-cursor, the input cursor, the storagearea and all
	 * cursors returned by {@link #cursor()}.
	 */
	public void closeAll() {
		if (numberOfClosedCursors != numberOfCursors) {
			for (Cursor<E> cursor : outCursors)
				cursor.close();
			numberOfClosedCursors = numberOfCursors;
		}
		inCursor.close();
		storageArea.close();
		explicitClose = false;
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the iteration has more elements,
	 *         <code>false</code> otherwise
	 */
	@Override
	protected boolean hasNextObject() {
		return outCursor.hasNext();
	}

	/**
	 * Returns the next element in the iteration.
	 * 
	 * @return next element in the iteration
	 */ 
	@Override
	protected E nextObject() { 
		return outCursor.next();
	}
}
