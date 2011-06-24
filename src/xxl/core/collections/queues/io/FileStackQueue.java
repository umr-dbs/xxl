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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import xxl.core.collections.queues.AbstractQueue;
import xxl.core.collections.queues.LIFOQueue;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.UniformConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides an implementation of the Queue interface with a
 * LIFO (<i>last in, first out</i>) strategy that internally uses a file
 * for storing its elements. It implements the <tt>peek</tt> method.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new file
 * 
 *     File file = new File("file.dat");
 *
 *     // create a new file stack queue with ...
 *
 *     FileStackQueue queue = new FileStackQueue(
 *     // the created file
 *     
 *          file,
 *     
 *     // an integer converter
 *
 *          IntegerConverter.DEFAULT_INSTANCE
 *      );
 *
 *     // open the queue
 *
 *     queue.open();
 *
 *     // create an iteration over the Integer between 0 and 19
 *
 *     Cursor cursor = new Enumerator(20);
 *
 *     // insert all elements of the given iterator
 *
 *     for (; cursor.hasNext(); queue.enqueue(cursor.next()));
 *
 *     // print all elements of the queue
 * 
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *
 *     System.out.println();
 * 
 *     // close open queue and cursor after use
 *
 *     queue.close();
 *     cursor.close();
 *
 *     // delete the file
 *
 *     file.delete();
 * </pre>
 *
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.AbstractQueue
 * @see xxl.core.collections.queues.LIFOQueue
 * @see java.io.File
 * @see java.io.RandomAccessFile
 */
public class FileStackQueue extends AbstractQueue implements LIFOQueue {

	/**
	 * A factory method to create a new FileStackQueue (see contract for
	 * {@link xxl.core.collections.queues.Queue#FACTORY_METHOD FACTORY_METHOD} in
	 * interface Queue). In contradiction to the contract in Queue it may
	 * only be invoked with a <i>parameter list</i> (for further
	 * details see Function) of objects. The <i>parameter list</i>
	 * will be used for initializing the file stack queue by calling the
	 * constructor
	 * <code>FileStackQueue((File) list.get(0), (Function) list.get(1))</code>.
	 *
	 * @see Function
	 */
	public static final Function<Object,FileStackQueue> FACTORY_METHOD = new AbstractFunction<Object,FileStackQueue>() {
		public FileStackQueue invoke(List<? extends Object> list) {
			return new FileStackQueue(
				(File)list.get(0),
				(Function)list.get(1)
			);
		}
	};

	/**
	 * The file is internally used for storing the elements of the queue.
	 */
	protected File file;

	/**
	 * A function that implements the functionality of opening the file
	 * for random access. The function is invoked with <tt>file</tt> when
	 * this file stack queue is opened and must implement all the
	 * functionality that is needed to open a file for random access. When
	 * being invoked the function must return a <tt>RandomAccessFile</tt>.
	 * The function determines whether the queue uses objects that are
	 * stored in the file before calling a constructor of FileStackQueue
	 * as initial elements of the queue. E.g. when setting the length of
	 * the <tt>RandomAccessFile</tt> to <tt>0</tt> before returning it,
	 * objects that are stored in the file are removed.
	 */
	protected Function openFile;

	/**
	 * A converter that is used for serializing and de-serializing the
	 * elements of this queue.
	 */
	protected Converter converter;

	/**
	 * A random access file stream for reading from and writing to
	 * <tt>file</tt>. The stream is created by invoking <tt>openFile</tt>
	 * with <tt>file</tt> when the file stack queue is opened.
	 */
	protected RandomAccessFile randomAccessFile;

	/**
	 * Constructs a queue that store its elements in the given file. When
	 * opening the queue, the function <tt>openFile</tt> is invoked with
	 * the file in order to open it for random access. Whether objects
	 * that are stored into the file are used for initializing the queue
	 * or not depends on <tt>openFile</tt>. The specified converter is
	 * used for serializing and de-serializing the elements of the queue.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param openFile a function that is invoked with the file in order
	 *        to open it for random access.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 */
	public FileStackQueue(File file, Function openFile, Converter converter) {
		this.file = file;
		this.openFile = openFile;
		this.converter = converter;
	}

	/**
	 * Constructs a queue that stores its elements in the given file. When
	 * opening the queue, the file is opened for random access by calling
	 * the constructor
	 * {@link RandomAccessFile#RandomAccessFile(File, String)
	 * RandomAccessFile(file, "rw")}. Therefore objects that are stored
	 * into the file are used for initializing the queue. The specified
	 * converter is used for serializing and de-serializing the elements
	 * of the queue.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 */
	public FileStackQueue(final File file, Converter converter) {
		this(
			file,
			new AbstractFunction() {
				public Object invoke(Object object) {
					try {
						return new RandomAccessFile(file, "rw");
					}
					catch (IOException ie) {
						throw new WrappingRuntimeException(ie);
					}
				}
			},
			converter
		);
	}

	/**
	 * Constructs a queue that stores its elements in the given file. When
	 * opening the queue, the function <tt>openFile</tt> is invoked with
	 * the file in order to open it for random access. Whether objects
	 * that are stored into the file are used for initializing the queue
	 * or not depends on <tt>openFile</tt>. The given converter is used
	 * for serializing and de-serializing the elements of the queue and
	 * the specified factory method is used for initializing these
	 * elements before the file is read out.<br>
	 * This constructor is equivalent to the call of
	 * <code>FileStackQueue(file, openFile, new UniformConverter(converter, newObject))</code>.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param openFile a function that is invoked with the file in order
	 *        to open it for random access.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 */
	public FileStackQueue(File file, Function openFile, Converter converter, Function newObject) {
		this(file, openFile, new UniformConverter(converter, newObject));
	}

	/**
	 * Constructs a queue that stores its elements in the given file. When
	 * opening the queue, the file is opened for random access by calling
	 * the constructor
	 * {@link RandomAccessFile#RandomAccessFile(File, String)
	 * RandomAccessFile(file, "rw")}. Therefore objects that are stored
	 * into the file are used for initializing the queue. The given
	 * converter is used for serializing and de-serializing the elements
	 * of the queue and the specified factory method is used for
	 * initializing these elements before the file is read out.<br>
	 * This constructor is equivalent to the call of
	 * <code>FileStackQueue(file, new UniformConverter(converter, newObject))</code>.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 */
	public FileStackQueue(final File file, Converter converter, Function newObject) {
		this(file, new UniformConverter(converter, newObject));
	}

	/**
	 * Constructs a queue that stores its elements in the given file. When
	 * opening the queue, the function <tt>openFile</tt> is invoked with
	 * the the file in order to open it for random access. Whether objects
	 * that are stored into the file are used for initializing the queue
	 * or not depends on <tt>openFile</tt>. A convertable converter is
	 * used for serializing and de-serializing the elements of the queue
	 * and the specified factory method is used for initializing these
	 * elements before the file is read out.<br>
	 * This constructor is equivalent to the call of
	 * <code>FileStackQueue(file, openFile, ConvertableConverter.DEFAULT_INSTANCE, newObject)</code>.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param openFile a function that is invoked with the file in order
	 *        to open it for random access.
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 * @see ConvertableConverter#DEFAULT_INSTANCE
	 */
	public FileStackQueue(File file, Function openFile, Function newObject) {
		this(file, openFile, ConvertableConverter.DEFAULT_INSTANCE, newObject);
	}

	/**
	 * Constructs a queue that stores its elements in the given file. When
	 * opening the queue, the file is opened for random access by calling
	 * the constructor
	 * {@link RandomAccessFile#RandomAccessFile(File, String)
	 * RandomAccessFile(file, "rw")}. Therefore objects that are stored
	 * into the file are used for initializing the queue. A convertable
	 * converter is used for serializing and de-serializing the elements
	 * of the queue and the specified factory method is used for
	 * initializing these elements before the file is read out.<br>
	 * This constructor is equivalent to the call of
	 * <code>FileStackQueue(file, ConvertableConverter.DEFAULT_INSTANCE, newObject)</code>.
	 *
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 * @see ConvertableConverter#DEFAULT_INSTANCE
	 */
	public FileStackQueue(final File file, Function newObject) {
		this(file, ConvertableConverter.DEFAULT_INSTANCE, newObject);
	}

	/**
	 * Opens the file stack queue for the access to elements of this
	 * queue. This method is called implicitly every time the elements of
	 * the queue are accessed. This implementation checks whether a random
	 * access file stream already exists. When no such stream exists,
	 * <tt>openFile</tt> is invoked with <tt>file</tt> and the elements
	 * that are stored in the file after opening it for random access are
	 * used as initial elements of the queue.
	 */
	public void open() {
		if (isOpened) return;
		super.open();
		try {
			if (randomAccessFile == null) {
				randomAccessFile = (RandomAccessFile)openFile.invoke(file);
				if (randomAccessFile.length() == 0)
					size = 0;
				else {
					randomAccessFile.seek(randomAccessFile.length()-4);
					size = randomAccessFile.readInt();
					randomAccessFile.setLength(randomAccessFile.length()-4);
				}
			}
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. <br>
	 * This implementation opens the queue first. Thereafter the object
	 * and the size of its serialization are written to the file.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 */
	protected void enqueueObject(Object object) {
		try {
			randomAccessFile.seek(randomAccessFile.length());
			converter.write(randomAccessFile, object);
			randomAccessFile.writeInt(Converters.sizeOf(converter, object));
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Returns the <i>next</i> element in the queue <i>without</i> removing it.
	 * <br>
	 * This implementation opens the queue first. Thereafter the length of
	 * the serialized object and the object itself are read out of the
	 * file and the object is returned.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	protected Object peekObject() {
		try {
			randomAccessFile.seek(randomAccessFile.length()-4);
			randomAccessFile.seek(randomAccessFile.getFilePointer()-randomAccessFile.readInt());
			return converter.read(randomAccessFile);
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Returns the <i>next</i> element in the queue. <br>
	 * This implementation opens the queue first. Thereafter the length of
	 * the serialized object and the object itself are read out of the
	 * file. At last, the file is truncated to its new length (the length
	 * of the file without the serialized object) and the object is
	 * returned.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	protected Object dequeueObject() {
		try {
			randomAccessFile.seek(randomAccessFile.length()-4);
			randomAccessFile.seek(randomAccessFile.getFilePointer()-randomAccessFile.readInt());
			long length = randomAccessFile.getFilePointer();
			Object object = converter.read(randomAccessFile);
			randomAccessFile.setLength(length);
			return object;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * This implementation opens the queue first. Thereafter the length of
	 * the random access file and <tt>size</tt> are set to <tt>0</tt>.
	 */
	public void clear() {
		try {
			randomAccessFile.setLength(0);
			size = 0;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method takes the same effect as a single call.<br>
	 * This implementation closes the random access file and deletes the
	 * file, when it is empty. Therefore the file stack queue can be
	 * reopened later and will have the same state as before closing it,
	 * when <tt>openFile</tt> does not affect the elements stored in the
	 * file.
	 */
	public void close() {
		if (isClosed) return;
		super.close();
		try {
			if (randomAccessFile != null) {
				if (size == 0) {
					randomAccessFile.close();
					file.delete();
				}
				else {
					randomAccessFile.seek(randomAccessFile.length());
					randomAccessFile.writeInt(size);
					randomAccessFile.close();
				}
				randomAccessFile = null;
			}
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}
}
