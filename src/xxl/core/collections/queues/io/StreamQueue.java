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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import xxl.core.collections.queues.AbstractQueue;
import xxl.core.functions.Function;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.UniformConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a queue that uses a data input stream and a data
 * output stream to store its elements. The <tt>peek</tt> method 
 * is not implemented by the stream queue. A converter is used for serializing 
 * and de-serializing the elements of the queue.<p>
 *
 * Implementations of this class must only provide valid implementations
 * for the functions <tt>newInputStream</tt> and <tt>newOutputStream</tt>.
 * These functions must implement the functionality of opening an input
 * or output stream on the serialized elements of this queue.
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.AbstractQueue
 * @see java.io.DataInputStream
 * @see java.io.DataOutputStream
 */
public abstract class StreamQueue<E> extends AbstractQueue<E> {

	/**
	 * A factory method to create a default stream queue (see contract for
	 * {@link xxl.core.collections.queues.Queue#FACTORY_METHOD FACTORY_METHOD} in
	 * interface Queue). This field is set to
	 * <code>{@link RandomAccessFileQueue#FACTORY_METHOD RandomAccessFileQueue.FACTORY_METHOD}</code>.
	 */
	public static final Function FACTORY_METHOD = RandomAccessFileQueue.FACTORY_METHOD;

	/**
	 * A converter that is used for serializing and de-serializing the
	 * elements of this queue.
	 */
	protected Converter<E> converter;

	/**
	 * The data input stream is used for reading the elements of the
	 * queue.
	 */
	protected DataInputStream dataInputStream = null;

	/**
	 * The data output stream is used for writing  elements to the queue.
	 */
	protected DataOutputStream dataOutputStream = null;

	/**
	 * A function that opens an input stream for reading the serialized
	 * elements of the queue and returns it. This method must implement
	 * the functionality for opening an input stream on the serialized
	 * data of this queue.
	 */
	protected Function<?, ? extends InputStream> newInputStream;

	/**
	 * A function that opens an output stream for writing serialized
	 * elements to the queue and returns it. This method must implement
	 * the functionality for opening an output stream on the serialized
	 * data of this queue.
	 */
	protected Function<?, ? extends OutputStream> newOutputStream;

	/**
	 * A function that returns the size of the input buffer when it is
	 * invoked. Every time the queue switches (implicitly) from output
	 * mode to input mode, the function is invoked in order to determine
	 * the size of the input buffer. A function is used instead of an
	 * integer to guarantee highest flexibility, i.e. the result of an
	 * invocation can depend on the size of the available memory etc.
	 */
	protected Function<?, Integer> inputBufferSize;
	
	/**
	 * The number of bytes in the BufferedInputStream which had not been used
	 * when the BufferedInputStream was closed. This value is used  
	 * in subclasses in order to compensate that these unused buffered bytes
	 * are lost when switching from input to output stream.
	 */
	protected long unReadInputBuffer = 0;

	/**
	 * A function that returns the size of the output buffer when it is
	 * invoked. Every time the queue switches (implicitly) from input mode
	 * to output mode, the function is invoked in order to determine the
	 * size of the output buffer. A function is used instead of an integer
	 * to guarantee highest flexibility, i.e. the result of an invocation
	 * can depend on the size of the available memory etc.
	 */
	protected Function<?, Integer> outputBufferSize;

	/**
	 * Default constructor. (For invocation by subclass constructors,
	 * typically implicit.)
	 */
	protected StreamQueue() {}

	/**
	 * Constructs a new stream queue that uses the specified converter for
	 * serializing and de-serializing its elements. The given functions
	 * are used for determining the size of the input and output buffer and
	 * opening input and output streams.
	 *
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param newInputStream a function that opens an input stream for
	 *        reading the serialized elements of this queue.
	 * @param newOutputStream a function that opens an output stream for
	 *        writing the serialized elements of this queue.
	 */
	public StreamQueue(Converter<E> converter, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize, Function<?, ? extends InputStream> newInputStream, Function<?, ? extends OutputStream> newOutputStream) {
		super();
		this.converter = converter;
		this.inputBufferSize = inputBufferSize;
		this.outputBufferSize = outputBufferSize;
		this.newInputStream = newInputStream;
		this.newOutputStream = newOutputStream;
	}

	/**
	 * Constructs a new stream queue that uses the specified converter for
	 * serializing and de-serializing its elements. The factory method is
	 * used for initializing the elements of the queue before the output
	 * stream is read out. The given functions are used for determining
	 * the size of the input and output buffer and opening input and output
	 * streams.<br>
	 * This constructor is equivalent to the call of
	 * <code>StreamQueue(new UniformConverter(converter, newObject), inputBufferSize, outputBufferSize, newInputStream, newOutputStream)</code>.
	 *
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue.
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the input stream is read out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @param newInputStream a function that opens an input stream for
	 *        reading the serialized elements of this queue.
	 * @param newOutputStream a function that opens an output stream for
	 *        writing the serialized elements of this queue.
	 */
	public StreamQueue(Converter<E> converter, Function<?, ? extends E> newObject, Function<?, Integer> inputBufferSize, Function<?, Integer> outputBufferSize, Function<?, ? extends InputStream> newInputStream, Function<?, ? extends OutputStream> newOutputStream) {
		this(new UniformConverter<E>(converter, newObject), inputBufferSize, outputBufferSize, newInputStream, newOutputStream);
	}

	/**
	 * Reads (removes) an object from the queue and returns it. <br>
	 * This implementation opens the data input stream and reads the
	 * object from it by using <tt>converter</tt>.
	 *
	 * @return the <i>next</i> element in the queue.
	 * @throws IOException if an I/O error occurs.
	 */
	protected E read() throws IOException {
		openInput();
		return converter.read(dataInputStream);
	}

	/**
	 * Writes (inserts) an object to the queue.  <br>
	 * This implementation opens the data output stream and writes the
	 * object to it by using <tt>converter</tt>.
	 *
	 * @param object the object to be inserted into the queue.
	 * @throws IOException if an I/O error occurs.
	 */
	protected void write(E object) throws IOException {
		openOutput();
		converter.write(dataOutputStream, object);
	}

	/**
	 * Appends the specified element to the <i>end</i> of this queue. <br>
	 * This implementation writes (inserts) the object to the queue and
	 * increases <tt>size</tt>.
	 *
	 * @param object element to be appended to the <i>end</i> of this
	 *        queue.
	 */
	@Override
	protected void enqueueObject(E object) {
		try {
			write(object);
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}
	
	/**
	 * Returns the <i>next</i> element in the queue without removing it.
	 * The <i>next</i> element of the queue is given by its <i>strategy</i>.<br>
	 * This method is invoked by <tt>peek</tt> and has to implement the 
	 * peek procedure depending on the <i>strategy</i> and the used datastructure.<br>
	 * 
	 * @return the <i>next</i> element in the queue.
	 * @throws UnsupportedOperationException if the <tt>peek</tt> method is not
	 *         supported.
	 */
	@Override
	protected E peekObject() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the <i>next</i> element in the queue. <br>
	 * This implementation reads (removes) the <i>next</i> object from the
	 * queue, decreases <tt>size</tt> and returns the object.
	 *
	 * @return the <i>next</i> element in the queue.
	 */
	@Override
	protected E dequeueObject() {
		try {
			return read();
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * This implementation opens the data input stream and skips over and
	 * discards all data from it. Thereafter the data input stream is
	 * closed and <tt>size</tt> is set to <tt>0</tt>.
	 */
	@Override
	public void clear() {
		try {
			openInput();
			dataInputStream.skip(dataInputStream.available());
			closeInput();
			size = 0;
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Opens a data input stream for reading the elements of the queue and
	 * sets <tt>dataInputStream</tt> to it. <br>
	 * This implementation closes any data output stream on this queue and
	 * invokes <tt>inputBufferSize</tt>. If the result of this
	 * invocation is 0, <tt>dataInputStream</tt> is set to an data input
	 * stream wrapping the result of calling the inputStream method.
	 * Otherwise the input stream is buffered by using a
	 * <tt>BufferedInputStream</tt> before it is wrapped.
	 */
	protected void openInput() {
		closeOutput();
		if (dataInputStream == null) {
			int bufferSize = inputBufferSize.invoke();
			dataInputStream = new DataInputStream(
				bufferSize > 0 ?
					new BufferedInputStream(newInputStream.invoke(), bufferSize) {
						@Override
						public void close() throws IOException {
							unReadInputBuffer = (count - pos); 
							super.close();
						}
					}:
					newInputStream.invoke()
			);
		}
	}

	/**
	 * Closes the data input stream on this queue when it is open.
	 */
	protected void closeInput() {
		if (dataInputStream != null)
			try {
				dataInputStream.close();
				dataInputStream = null;
			}
			catch (IOException ie) {
				throw new WrappingRuntimeException(ie);
			}
	}

	/**
	 * Opens a data output stream for writing elements to the queue and
	 * sets <tt>dataOutputStream</tt> to it. <br>
	 * This implementation closes any data input stream on this queue and
	 * invokes <tt>outputBufferSize</tt>. When the result of this
	 * invocation is 0, <tt>dataOutputStream</tt> is set to an data output
	 * stream wrapping the result of calling the outputStream method.
	 * Otherwise the output stream is buffered by using a
	 * <tt>BufferedOutputStream</tt> before it is wrapped.
	 */
	protected void openOutput() {
		closeInput();
		if (dataOutputStream == null) {
			int bufferSize = outputBufferSize.invoke();
			dataOutputStream = new DataOutputStream(
				bufferSize > 0 ?
					new BufferedOutputStream(newOutputStream.invoke(), bufferSize) :
					newOutputStream.invoke()
			);
		}
	}

	/**
	 * Closes the data output stream on this queue if it is opened.
	 */
	protected void closeOutput() {
		if (dataOutputStream != null)
			try {
				dataOutputStream.close();
				dataOutputStream = null;
			}
			catch (IOException ie) {
				throw new WrappingRuntimeException(ie);
			}
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method takes the same effect as a single call.<br>
	 * This implementation closes the data input stream and data output
	 * stream on this queue.
	 */
	@Override
	public void close () {
		if (isClosed)
			return;
		super.close();
		closeInput();
		closeOutput();
	}
	
}
