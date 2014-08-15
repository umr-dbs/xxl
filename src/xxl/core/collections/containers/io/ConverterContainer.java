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
import java.io.DataInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.ConstrainedDecoratorContainer;
import xxl.core.collections.containers.Container;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.ByteBufferDataInput;
import xxl.core.io.UnsafeDataInput;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a container that stores a set of converted
 * elements. The container decorates an underlying container that must be
 * able to store byte arrays (as blocks). <p>
 *
 * When storing an element in this converter it is converted and the
 * resulting byte array is wrapped by a block. Thereafter this block is
 * stored in the decorated container.<p>
 *
 * By using a ConverterContainer it is possible to decorate a container
 * that is based on extenal memory, because conversions between objects
 * and byte arrays will be internally done.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new coverter container with ...
 *
 *     ConverterContainer container = new ConverterContainer(
 *
 *         // a map container that stores the elements
 *
 *         new MapContainer(),
 *
 *         // an integer converter for converting the elements
 *
 *         IntegerConverter.DEFAULT_INSTANCE
 *     );
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Iterator iterator = new Enumerator(20);
 *
 *     // insert all elements of the given iterator
 *
 *     iterator = container.insertAll(iterator);
 *
 *     // print all elements of the queue
 *
 *     while (iterator.hasNext())
 *         System.out.println(container.get(iterator.next()));
 *
 *     // get the ids of the elements of the container
 *
 *     iterator = container.ids();
 *
 *     // remove 5 elements
 *
 *     for (int i = 0; i < 5 && iterator.hasNext(); i++) {
 *         container.remove(iterator.next());
 *
 *         // refresh the iterator (cause it can be in an invalid state)
 *
 *         iterator = container.ids();
 *     }
 *
 *     // update 5 elements
 *
 *     for (int i = 0; i < 5 && iterator.hasNext(); i++) {
 *         Object id = iterator.next();
 *         container.update(id, new Integer(((Integer)container.get(id)).intValue()+100));
 *     }
 *
 *     // get the ids of all elements of the container
 *
 *     iterator = container.ids();
 *
 *     // print all elements of the queue
 *
 *     while (iterator.hasNext())
 *         System.out.println(container.get(iterator.next()));
 *
 *     // close the open queue after use
 *
 *     container.close();
 * </pre>
 *
 * @see ByteArrayInputStream
 * @see ConstrainedDecoratorContainer
 * @see Container
 * @see DataInputStream
 * @see Function
 * @see IOException
 * @see NoSuchElementException
 * @see WrappingRuntimeException
 */
public class ConverterContainer extends ConstrainedDecoratorContainer {

	/**
	 * The converter that is used for converting the elements of the
	 * underlying container.
	 */
	protected Converter converter;

    /**
     * Specified the type of <tt>DataOutput</tt> used
     * for serialization resp. deserialization
     */
    protected Converters.SerializationMode serializationMode;

    /**
     * The size of the buffer used for serialization resp. deserialization.
     * This value is only relevant for serialization modes <tt>BYTE_BUFFER</tt>
     * and <tt>UNSAFE</tt>.
     */
    protected int bufferSize;

    /**
     * Constructs a new ConverterContainer that decorates the specified
     * container and uses the specified converter for converting its
     * elements.
     *
     * @param container the underlying container that is used for storing
     *        the converted elements.
     * @param converter the converter that is used for converting the
     *        elements of this container.
     */
    public ConverterContainer (Container container, Converter converter) {
        this(container, converter, Converters.SerializationMode.BYTE_ARRAY, 0);
    }

	/**
	 * Constructs a new ConverterContainer that decorates the specified
	 * container and uses the specified converter for converting its
	 * elements.
	 *
	 * @param container the underlying container that is used for storing
	 *        the converted elements.
	 * @param converter the converter that is used for converting the
	 *        elements of this container.
	 */
	public ConverterContainer (Container container, Converter converter, Converters.SerializationMode serializationMode, int bufferSize) {
		super(container);
		this.converter = converter;
        this.serializationMode = serializationMode;
        this.bufferSize = bufferSize;
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown when the desired object is not found via contains.<br>
	 * This implementation gets the block associated to the <tt>id</tt>
	 * from the underlying container and tries to convert the wrapped byte
	 * array. The parameter <tt>unfix</tt> is passed to the underlying container.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		try {
			Block block = null;
            block = (Block)super.get(id, unfix);
            if (serializationMode == Converters.SerializationMode.BYTE_BUFFER)
                return converter.read(new ByteBufferDataInput(new ByteArrayInputStream(block.array, block.offset, block.size)));
            else if (serializationMode == Converters.SerializationMode.UNSAFE)
                return converter.read(new UnsafeDataInput(new ByteArrayInputStream(block.array, block.offset, block.size)));
            else
                return converter.read(new DataInputStream(new ByteArrayInputStream(block.array, block.offset, block.size)));
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * <br>
	 * This implementation tries to convert the object and inserts a block
	 * wrapping the resulting byte array into the underlying container.
	 * The parameter <tt>unfix</tt> is passed to the decorated container.
	 * Thereafter the identifier that has been associated to the block by
	 * the underlying container is returned.
	 *
	 * @param object is the new object.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		byte [] array = Converters.toByteArray(converter, object, serializationMode, bufferSize);

		return super.insert(new Block(array, 0, array.length), unfix);
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Since the identifier may have an arbitrary type (which has to be
	 * known in the container), the container has to provide such a method
	 * when the data is not stored in main memory.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	public FixedSizeConverter objectIdConverter () {
		return container.objectIdConverter();
	}

	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * This call is forwarded to the underlying Container.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return container.getIdSize();
	}

	/**
	 * Reserves an id for subsequent use. The container may or may not
	 * need an object to be able to reserve an id, depending on the
	 * implementation. If so, it will call the parameterless function
	 * provided by the parameter <tt>getObject</tt>.
	 * This implementation wraps the function getObject by converting
	 * the object when invoking the function.
	 *
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved.
	 * @return the reserved id.
	*/
	public Object reserve (final Function getObject) {
		return super.reserve(
			new AbstractFunction () {
				public Object invoke () {
					return new Block(Converters.toByteArray(converter, getObject.invoke(), serializationMode, bufferSize));
				}
			}
		);
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container (checked via isUsed).<br>
	 * This implementation tries to convert the object and updates the
	 * existing element of the underlying container by a block wrapping
	 * the resulting byte array. The parameter <tt>unfix</tt> is passed to the
	 * decorated container.
	 *
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		byte [] array = Converters.toByteArray(converter, object, serializationMode, bufferSize);

		super.update(id, new Block(array, 0, array.length), unfix);
	}
}
