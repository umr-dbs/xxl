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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.List;

import xxl.core.collections.queues.FIFOQueue;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.Convertable;
import xxl.core.io.FilesystemOperations;
import xxl.core.io.JavaFilesystemOperations;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.UniformConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a stream queue that stores the bytes of its
 * serialized elements in a random access file. The <tt>peek</tt> method
 * is not implemented by this queue. A converter is used for serializing 
 * and de-serializing the elements of the queue.<p>
 * The queue only uses a window of the file. The file is always growing
 * with further insertions. So, be careful not to use a queue too long.<br>
 *
 * Example usage (1).
 * <pre>
 *     // create a new file
 *
 *     File file = new File("queue.dat");
 *
 *     // create a new random access file queue with ...
 *
 *     RandomAccessFileQueue queue = new RandomAccessFileQueue(
 *
 *         // the created file
 *
 *         file,
 *
 *         // an integer converter
 *
 *         IntegerConverter.DEFAULT_INSTANCE,
 *
 *         // an input buffer size of 4 bytes
 *
 *         new Constant(4),
 *
 *         // an output buffer size of 4 bytes
 *
 *         new Constant(4)
 *     );
 *     
 *     // open the queue
 * 
 *     queue.open();
 * 
 *     // insert the integers from 0 to 9 into the queue
 *
 *     for (int i = 0; i &lt; 10; i++)
 *         queue.enqueue(new Integer(i));
 *
 *     // print 5 elements of the queue
 *
 *     int i = 0;
 *     while (i &lt; 5 && !queue.isEmpty()) {
 *         i = ((Integer)queue.dequeue()).intValue();
 *         System.out.println(i);
 *     }
 *
 *     // insert the integers from 20 to 29 into the queue
 *
 *     for (i = 20; i &lt; 30; i++)
 *         queue.enqueue(new Integer(i));
 *
 *     // print all elements of the queue
 *
 *     while (!queue.isEmpty())
 *          System.out.println(queue.dequeue());
 *
 *     // close and clear the queue after use
 *
 *     queue.close();
 *     queue.clear();
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.io.StreamQueue
 * @see xxl.core.collections.queues.FIFOQueue
 * @see java.io.File
 * @see java.io.RandomAccessFile
 */
public class RandomAccessFileQueue<E> extends StreamQueue<E> implements FIFOQueue<E> {

	/**
	 * A factory method to create a new RandomAccessFileQueue (see
	 * contract for
	 * {@link xxl.core.collections.queues.Queue#FACTORY_METHOD FACTORY_METHOD} in
	 * interface Queue). In contradiction to the contract in Queue it may
	 * only be invoked with a <i>parameter list</i> (for further
	 * details see Function) of objects. The <i>parameter list</i>
	 * will be used for initializing the random access file queue by
	 * calling the constructor
	 * <code>RandomAccessFileQueue((File) list.get(0), (Function)list.get(1), new Constant(0), new Constant(0))</code>.
	 *
	 * @see Function
	 */
	public static final Function<Object, RandomAccessFileQueue<Convertable>> FACTORY_METHOD = new AbstractFunction<Object, RandomAccessFileQueue<Convertable>>() {
		public RandomAccessFileQueue<Convertable> invoke(List<? extends Object> list) {
			return new RandomAccessFileQueue<Convertable>(
				(File)list.get(0),
				ConvertableConverter.DEFAULT_INSTANCE,
				(Function<?, ? extends Convertable>)list.get(1),
				new Constant<Integer>(0),
				new Constant<Integer>(0)
			);
		}
	};

	/**
	 * A function that opens an input stream for reading the serialized
	 * elements of the queue and returns it. <br>
	 * The implementation of this function opens the random access file
	 * queue first. Thereafter the file pointer of the random access
	 * file is set to <tt>readOffset</tt> and an input stream is
	 * returned, that redirects every calls to one of its methods to
	 * the corresponding method of the random access file.
	 */
	public class InputStreamFactory extends AbstractFunction<Object, InputStream> {
		/**
		 * Returns the result of the function as an object. Note that the
		 * caller has to cast the result to the desired class.
		 * @return the function value is returned.
		 */
		public InputStream invoke() {
			try {
				open();
				randomAccessFile.seek(readOffset);
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
			return new InputStream() {
				public int available() throws IOException {
					return (int)(randomAccessFile.length()-randomAccessFile.getFilePointer());
				}

				public void close() throws IOException {
					if (size() == 0)
						randomAccessFile.setLength(0);
					readOffset = randomAccessFile.getFilePointer() - unReadInputBuffer;
					unReadInputBuffer = 0;
				}

				public int read() throws IOException {
					return randomAccessFile.read();
				}

				public int read(byte[] b) throws IOException {
					return randomAccessFile.read(b);
				}

				public int read(byte[] b, int off, int len) throws IOException {
					return randomAccessFile.read(b, off, len);					
				}

				public long skip(long n) throws IOException {
					long offset = randomAccessFile.getFilePointer();
					randomAccessFile.seek(Math.min(n+offset, randomAccessFile.length()));
					return randomAccessFile.getFilePointer()-offset;
				}
			};
		}
	}

	/**
	 * A function that opens an output stream for writing serialized
	 * elements to the queue and returns it. <br>
	 * The implementation of this function opens the random access file
	 * queue first. Thereafter the file pointer of the random access
	 * file is set to its end and an output stream is returned, that
	 * redirects every calls to one of its methods to the corresponding
	 * method of the random access file.
	 */
	public class OutputStreamFactory extends AbstractFunction<Object, OutputStream> {
		/**
		 * Returns the result of the function as an object. Note that the
		 * caller has to cast the result to the desired class.
		 * @return the function value is returned.
		 */
		public OutputStream invoke() {
			try {
				open();
				randomAccessFile.seek(randomAccessFile.length());
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
			return new OutputStream() {
				public void write(int b) throws IOException {
					randomAccessFile.write(b);
				}

				public void write(byte[] b) throws IOException {
					randomAccessFile.write(b);
				}

				public void write(byte[] b, int off, int len) throws IOException {
					randomAccessFile.write(b, off, len);
				}
			};
		}
	}

	/**
	 * A file of this name is internally used for storing the elements of the queue.
	 */
	protected String filename;

	/**
	 * One constructor uses a File object (to use temporary files). Therefore, the queue
	 * works in two modes, the filename mode an the file mode.
	 */
	protected File file;
	
	/**
	 * A random access file stream for reading from and writing to
	 * <tt>file</tt>. The stream is created by invoking <tt>openFile</tt>
	 * with <tt>file</tt> when the file stack queue is opened.
	 */
	protected RandomAccessFile randomAccessFile;

	/**
	 * Contains an object which is responsible for the operations on files.
	 */
	protected FilesystemOperations fso;
	
	/**
	 * The offset from the beginning of the file, in bytes, at which the
	 * next serialized element of this queue is stored.
	 */
	protected long readOffset;

	/**
	 * Constructs a new random access file that stores its elements in the
	 * given file. 
	 * When opening the queue, the file is opened for random access
	 * by calling the openFile-method of the filesystem determined by <tt>fso</tt>. 
	 * Therefore objects that are stored
	 * into the file are used for initializing the queue. The specified
	 * converter is used for serializing and de-serializing the elements
	 * of the queue. The given functions are used for determining the size
	 * of the input and ouput buffer.
	 *
	 * @param filename the name of the file that is internally used for storing the
	 *        elements of the queue.
	 * @param fso the filesystem on which the file is created.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param newInputStream a function (factory-method) that returns a
	 *        new input stream used for reading data.
	 * @param newOutputStream a function (factory-method) that returns a
	 *        new output stream used for writing data.
	 */
	public RandomAccessFileQueue(String filename, FilesystemOperations fso, Converter<E> converter, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize, Function<?, ? extends InputStream> newInputStream, Function<?, ? extends OutputStream> newOutputStream) {
		super(converter, inputBufferSize, outputBufferSize, newInputStream, newOutputStream);
		this.filename = filename;
		this.fso = fso;
	}

	/**
	 * Constructs a new random access file that stores its elements in the
	 * given file. 
	 * When opening the queue, the file is opened for random access
	 * by calling the openFile-method of JavaFilesystemOperations. 
	 * Therefore objects that are stored
	 * into the file are used for initializing the queue. The specified
	 * converter is used for serializing and de-serializing the elements
	 * of the queue. The given functions are used for determining the size
	 * of the input and ouput buffer.
	 *
	 * @param filename the name of the file that is internally used for storing the
	 *        elements of the queue.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public RandomAccessFileQueue(String filename, Converter<E> converter, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize) {
		this(filename, JavaFilesystemOperations.DEFAULT_INSTANCE, converter, inputBufferSize, outputBufferSize, null, null);
		this.newInputStream = new InputStreamFactory(); //early bind problem...
		this.newOutputStream = new OutputStreamFactory(); //early bind problem...
	}

	/**
	 * Constructs a new random access file queue that stores its elements
	 * in the given file. 
	 * When opening the queue, the file is opened for random access
	 * by calling the openFile-method of the filesystem determined by <tt>fso</tt>. 
	 * The specified converter is used for serializing
	 * and de-serializing the elements of the queue. The given functions
	 * are used for determining the size of the input and ouput buffer.
	 *
	 * @param filename the name of the file that is internally used for storing the
	 *        elements of the queue.
	 * @param fso the filesystem on which the file is created.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public RandomAccessFileQueue(String filename, FilesystemOperations fso, Converter<E> converter, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize) {
		this(filename, fso, converter, inputBufferSize, outputBufferSize, null, null);
		this.newInputStream = new InputStreamFactory(); //early bind problem...
		this.newOutputStream = new OutputStreamFactory(); //early bind problem...
	}

	/**
	 * Constructs a new random access file queue that stores its elements
	 * in the given file.
	 * When opening the queue, the file is opened for random access
	 * by calling the openFile-method of JavaFilesystemOperations. 
	 * Therefore objects that are stored
	 * into the file are used for initializing the queue. The specified
	 * converter is used for serializing and de-serializing the elements
	 * of the queue and the specified factory method is used for
	 * initializing these elements before the file is read out. The given
	 * functions are used for determining the size of the input and ouput
	 * buffer.
	 *
	 * @param filename the name of the file that is internally used for storing the
	 *        elements of the queue.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public RandomAccessFileQueue(String filename, Converter<E> converter, Function<?, ? extends E> newObject, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize) {
		this(filename, new UniformConverter<E>(converter, newObject), inputBufferSize, outputBufferSize);
	}

	/**
	 * Constructs a new random access file queue that stores its elements
	 * in the given file. When opening the queue, the file is opened for
	 * random access by calling the constructor
	 * {@link RandomAccessFile#RandomAccessFile(File, String)
	 * RandomAccessFile(file, "rw")}. Therefore objects that are stored
	 * into the file are used for initializing the queue. A convertable
	 * converter is used for serializing and de-serializing the elements
	 * of the queue and the specified factory method is used for
	 * initializing these elements before the file is read out. The given
	 * functions are used for determining the size of the input and ouput
	 * buffer.
	 * @param file the file that is internally used for storing the
	 *        elements of the queue.
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the file is read out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @see ConvertableConverter#DEFAULT_INSTANCE
	 */
	public RandomAccessFileQueue(File file, Converter<E> converter, Function<?, ? extends E> newObject,  Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize) {
		this(file.getPath(), converter, newObject, inputBufferSize, outputBufferSize);
		this.file = file;
	}

	/**
	 * Opens the random access file queue for the access to elements of
	 * this queue. This method is called implicitly every time the
	 * <tt>inputStream</tt> or <tt>outputStream</tt> method is called.
	 * This implementation checks whether a random access file stream
	 * already exists. When no such stream exists, <tt>openFile</tt> is
	 * invoked with <tt>file</tt> and the elements that are stored in the
	 * file after opening it for random access are used as initial
	 * elements of the queue.
	 */
	@Override
	public void open() {
		if (isOpened)
			return;
		super.open();
		if (randomAccessFile == null)
			try {
				if (file != null) 
					randomAccessFile = new RandomAccessFile(file,"rw");
				else
					randomAccessFile = fso.openFile(filename,"rw");
				if (randomAccessFile.length() == 0)
					readOffset = 0;
				else {
					randomAccessFile.seek(randomAccessFile.length()-12);
					size = randomAccessFile.readInt();
					readOffset = randomAccessFile.readLong();
					randomAccessFile.setLength(randomAccessFile.length()-12);
				}
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
	}


	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * This implementation simply closes the queue, deletes the internally
	 * used file and sets <tt>size</tt> to <tt>0</tt>.
	 */
	@Override
	public void clear() {
		if (file != null)
			file.delete();
		else
			fso.deleteFile(filename);
		size = 0;
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method takes the same effect as a single call.<br>
	 * This implementation closes the random access file and deletes the
	 * file if it is empty. Therefore the queue can be reopened later 
	 * and will have the same state as before closing it,
	 * when <tt>openFile</tt> does not affect the elements stored in the
	 * file.
	 */
	@Override
	public void close () {
		if (isClosed)
			return;
		super.close();
		if (randomAccessFile != null) {
			try {
				if (size() == 0) {
					randomAccessFile.close();
					if (file != null)
						file.delete();
					else
						fso.deleteFile(filename);
				}
				else {
					randomAccessFile.seek(randomAccessFile.length());
					randomAccessFile.writeInt(size);
					randomAccessFile.writeLong(readOffset);
					randomAccessFile.close();
				}
				randomAccessFile = null;
			}
			catch (IOException ioe) {
				throw new WrappingRuntimeException(ioe);
			}
		}
	}
}
