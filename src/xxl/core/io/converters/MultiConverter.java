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

package xxl.core.io.converters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import xxl.core.functions.Function;

/**
 * Converts an object with multiple convertable fields into a byte
 * representation and vice versa.
 * 
 * @param <T> the type to be converted.
 */
public class MultiConverter<T> extends Converter<T> {

	/**
	 * The converters that is wrapped. The converter must be able to read and
	 * write uniform objects.
	 */
	protected Converter<Object>[] converters;

	/**
	 * A factory method that is used for creating the object to be read. This
	 * function will be invoked when the read method is called (even if there
	 * is an object specified to restore).
	 */
	protected Function<Object, ? extends T> createObject;
	
	/**
	 * A function which converts the components of the object into an array of
	 * objects.
	 */
	protected Function<? super T, Object[]> objectToObjectArray;

	/**
	 * Constructs a new converter that wraps the specified converter and uses
	 * the specified function as factory method.
	 *
	 * @param createObject a factory method that is used for initializing the
	 *        object to read when it is not specified.
	 * @param objectToObjectArray a function which converts the components of
	 *        the object into an array of objects.
	 * @param converters the converter to be wrapped.
	 */
	public MultiConverter(Function<Object, ? extends T> createObject, Function<? super T, Object[]> objectToObjectArray, Converter... converters) {
		this.converters = converters;
		this.createObject = createObject;
		this.objectToObjectArray = objectToObjectArray;
	}

	/**
	 * Reads the state (the attributes) for the specified object from the
	 * specified data input and returns the restored object.
	 * 
	 * <p>This implementation calls the read method of the wrapped converter.
	 * When the specified object is <code>null</code> it is initialized by
	 * invoking the function (factory method).</p>
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *       object.
	 * @param object the object to be restored. If the object is
	 *        <code>null</code> it is initialized by invoking the function
	 *        (factory method).
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public T read(DataInput dataInput, T object) throws IOException {
		List<Object> o = new ArrayList<Object>(converters.length);
		for (int i = 0; i < converters.length; i++)
			o.add(converters[i].read(dataInput));
		return createObject.invoke(o);
	}

	/**
	 * Writes the state (the attributes) of the specified object to the
	 * specified data output.
	 * 
	 * <p>This implementation calls the write method of the wrapped
	 * converter.</p>
	 *
	 * @param dataOutput the stream to write the state (the attributes) of the
	 *        object to.
	 * @param object the object whose state (attributes) should be written to
	 *        the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, T object) throws IOException {
		Object[] o = objectToObjectArray.invoke(object);
		for (int i = 0; i < converters.length; i++)
			converters[i].write(dataOutput, o[i]);
	}
}
