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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import xxl.core.collections.queues.FIFOQueue;
import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.UniformConverter;

/**
 * This class provides a stream queue that stores the bytes of its
 * serialized elements in a <tt>ListQueue</tt>. The <tt>peek</tt> 
 * method is not implemented by this queue. A converter is used for 
 * serializing and de-serializing the elements of the queue.<p>
 *
 * <b>Note:</b> The queue can be in two states: either reading ({@link StreamQueue#openOutput()}, 
 * {@link StreamQueue#read()}) or writing ({@link StreamQueue#openInput()}, 
 * {@link StreamQueue#write(Object)}). 
 * <p>When switching from input to output mode the buffer
 * (more precisely the input buffer) is written to the queue; so, no data will be lost. 
 * But <b>when switching back from output to input mode the content of
 * the output buffer could get lost</b> if the buffer is able to store more than one
 * element and isn't manually emptied before switching.<br/>
 * So, here are two examples how to prevent data leakage:<br/>
 * <ul>
 * <li>If the buffer size is set to the size of one element no data can get 
 * lost because the first (in this case the only) element is processed immediately.</li>
 * <li>If the buffer is (manually) emptied before switching from output to 
 * input mode no anomaly will occur and no data will get lost.</li>
 * </ul>
 * </p>
 * <p>
 * 
 * Example usage (1).
 * <pre>
 *     // create a new list stream queue with ...
 *
 *     ListStreamQueue queue = new ListStreamQueue(
 *
 *         // an integer converter
 *
 *         IntegerConverter.DEFAULT_INSTANCE,
 *
 *         // an input buffer size of 4 bytes (size of an integer)
 *
 *         new Constant(4),
 *
 *         // an output buffer size of 4 bytes (size of an integer)
 *
 *         new Constant(4)
 *     );
 *
 *     // open the queue
 *
 *     queue.open();
 * 
 *     // insert the integer from 0 to 9 into the queue
 *
 *     for (int i = 0; i &lt; 10; i++)
 *         queue.enqueue(new Integer(i));
 *
 *     // print five elements of the queue
 *
 *     int i = 0;
 *     while (i &lt; 5 && !queue.isEmpty()) {
 *         i = ((Integer)queue.dequeue()).intValue();
 *         System.out.println(i);
 *     }
 *
 *     // insert the integers from 20 to 29
 *
 *     for (i = 20; i &lt; 30; i++)
 *         queue.enqueue(new Integer(i));
 *
 *     // print all elements of the queue
 *
 *     while (!queue.isEmpty())
 *         System.out.println(queue.dequeue());
 * 
 *     System.out.println();
 * 
 *     // close the queue
 * 
 *     queue.close();
 * </pre>
 *
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.io.StreamQueue
 * @see xxl.core.collections.queues.FIFOQueue
 */
public class ListStreamQueue extends StreamQueue implements FIFOQueue {

	/**
	 * A factory method to create a new ListStreamQueue (see contract for
	 * {@link xxl.core.collections.queues.Queue#FACTORY_METHOD FACTORY_METHOD} in
	 * interface Queue). In contradiction to the contract in Queue it may
	 * only be invoked with a <i>parameter list</i> (for further
	 * details see Function) of objects. The <i>parameter list</i>
	 * will be used for initializing the list stream queue by calling the
	 * constructor
	 * <code>ListStreamQueue((Function) list.get(0), new Constant(0), new Constant(0))</code>.
	 *
	 * @see Function
	 */
	public static final Function<Object,ListStreamQueue> FACTORY_METHOD = new AbstractFunction<Object,ListStreamQueue>() {
		public ListStreamQueue invoke(List<? extends Object> list) {
			return new ListStreamQueue(
				(Function)list.get(0),
				new Constant(0),
				new Constant(0)
			);
		}
	};

	/**
	 * A queue that is internally used for storing the bytes of the
	 * serialized elements of this queue.
	 */
	protected Queue queue = new ListQueue();

	/**
	 * Constructs a new list stream queue that uses the specified
	 * converter for serializing and de-serializing its elements. The
	 * given functions are used for determining the size of the input
	 * and output buffer.
	 *
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public ListStreamQueue(Converter converter, Function inputBufferSize, Function outputBufferSize) {
		super(converter, inputBufferSize, outputBufferSize, null, null);
		this.newInputStream = new AbstractFunction() {
			public Object invoke() {
				return new InputStream() {
					public int read() {
						int i = queue.size() == 0 ?
							-1 :
							((Integer)queue.dequeue()).intValue();
						return i;
					}
				};	
			}	
		};
		this.newOutputStream = new AbstractFunction() {
			public Object invoke() {
				return new OutputStream() {
					public void write(int b) {
						queue.enqueue(new Integer(b));
					}
				};	
			}	
		};
	}

	/**
	 * Constructs a new list stream queue that uses the specified
	 * converter for serializing and de-serializing its elements. The
	 * factory method is used for initializing the elements of the queue
	 * before the output stream is read out. The given functions are used
	 * for determining the size of the input and output buffer.<br>
	 * This constructor is equivalent to the call of
	 * <code>ListStreamQueue(new UniformConverter(converter, newObject), inputBufferSize, outputBufferSize)</code>.
	 *
	 * @param converter a converter that is used for serializing and
	 *        de-serializing the elements of the queue
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the input stream is read out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 */
	public ListStreamQueue(Converter converter, Function newObject, Function inputBufferSize, Function outputBufferSize) {
		this(new UniformConverter(converter, newObject), inputBufferSize, outputBufferSize);
	}

	/**
	 * Constructs a new list stream queue that uses a convertable
	 * converter for serializing and de-serializing its elements. The
	 * factory method is used for initializing the elements of the queue
	 * before the output stream is read out. The given functions are used
	 * for determining the size of the input and output buffer.<br>
	 * This constructor is equivalent to the call of
	 * <code>ListStreamQueue(ConvertableConverter.DEFAULT_INSTANCE, newObject, inputBufferSize, outputBufferSize)</code>.
	 *
	 * @param newObject a factory method that is used for initializing the
	 *        elements of the queue before the input stream is read out.
	 * @param inputBufferSize a function that determines the size of the
	 *        input buffer.
	 * @param outputBufferSize a function that determines the size of the
	 *        output buffer.
	 * @see ConvertableConverter#DEFAULT_INSTANCE
	 */
	public ListStreamQueue(Function newObject, Function inputBufferSize, Function outputBufferSize) {
		this(ConvertableConverter.DEFAULT_INSTANCE, newObject, inputBufferSize, outputBufferSize);
	}

	/**
	 * Removes all elements from this queue. The queue will be
	 * empty after this call returns so that <tt>size() == 0</tt>.<br>
	 * This implementation simply clears <tt>queue</tt> and sets 
	 * <tt>size</tt> to <tt>0</tt>.
	 */
	public void clear () {
		queue.clear();
		size = 0;
	}

	/**
	 * Closes this queue and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method takes the same effect as a single call.<br>
	 * This implementation simply closes <tt>queue</tt>.
	 */
	public void close () {
		if (isClosed) return;
		super.close();
		queue.close();
	}
}
