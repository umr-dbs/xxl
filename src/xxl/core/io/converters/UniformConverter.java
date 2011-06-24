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

import xxl.core.functions.Function;

/**
 * This class provides a wrapper for a converter that reads and writes uniform
 * objects. Whenever the <code>read</code> method is called with an object that
 * is equal to <code>null</code>, the underlying converter is called with an
 * object that has been delivered by invoking function.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create two map entries
 *   
 *   MapEntry&lt;Integer, String&gt; me1 = new MapEntry&lt;Integer, String&gt;(42, "Hello world.");
 *   MapEntry&lt;Integer, String&gt; me2 = new MapEntry&lt;Integer, String&gt;(4711, "That's all, folks!");
 *   
 *   // create a converter that only stores the value of a map entry
 *   
 *   Converter&lt;MapEntry&lt;Integer, String&gt;&gt; converter = new Converter&lt;MapEntry&lt;Integer, String&gt;&gt;() {
 *   
 *       // how to write a map entry
 *       
 *       public void write(DataOutput dataOutput, MapEntry&lt;Integer, String&gt; object) throws IOException {
 *       
 *           // write the value of the map entry
 *           
 *           StringConverter.DEFAULT_INSTANCE.write(dataOutput, object.getValue());
 *       }
 *       
 *       // how to read a map entry
 *       
 *       public MapEntry&lt;Integer, String&gt; read (DataInput dataInput, MapEntry&lt;Integer, String&gt; object) throws IOException {
 *       
 *           // read the value of the map entry
 *           
 *           object.setValue(StringConverter.DEFAULT_INSTANCE.read(dataInput));
 *           return object;
 *       }
 *   };
 *   
 *   // create a factory method that produces map entries with keys of increasing integer objects
 *   
 *   Function&lt;Object, MapEntry&lt;Integer, String&gt;&gt; factory = new Function&lt;Object, MapEntry&lt;Integer, String&gt;&gt;() {
 *   
 *       // a count for the returned keys
 *       
 *       int i = 0;
 *       
 *       // how to create a map entry
 *       
 *       public MapEntry&lt;Integer, String&gt; invoke() {
 *       
 *           // return a map entry with an integer wrapping the counter as key and null as value
 *           
 *           return new MapEntry&lt;Integer, String&gt;(i++, null);
 *       }
 *   };
 *   
 *   // create an uniform converter with ...
 *   
 *   UniformConverter&lt;MapEntry&lt;Integer, String&gt;&gt; uniformConverter = new UniformConverter&lt;MapEntry&lt;Integer, String&gt;&gt;(
 *       
 *       // the created converter
 *       
 *       converter,
 *       
 *       // the created factory method
 *       
 *       factory
 *   );
 *   
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // create an data output stream
 *   
 *   DataOutputStream dataOutput = new DataOutputStream(output);
 *   
 *   // write two strings to the output stream
 *   
 *   uniformConverter.write(dataOutput, me1);
 *   uniformConverter.write(dataOutput, me2);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // create an data input stream
 *   
 *   DataInputStream dataInput = new DataInputStream(input);
 *   
 *   // read two strings from the input stream
 *   
 *   me1 = uniformConverter.read(dataInput);
 *   me2 = uniformConverter.read(dataInput);
 *   
 *   // print the value and the object
 *   
 *   System.out.println(me1);
 *   System.out.println(me2);
 *   
 *   // close the streams after use
 *   
 *   dataInput.close();
 *   dataOutput.close();
 * </pre></code></p>
 *
 * @param <T> the type to be converted.
 * @see DataInput
 * @see DataOutput
 * @see Function
 * @see IOException
 */
public class UniformConverter<T> extends Converter<T> {

	/**
	 * The converter that is wrapped. The converter must be able to read and
	 * write uniform objects.
	 */
	protected Converter<T> converter;

	/**
	 * A factory method that is used for initializing the object to read. This
	 * function will be invoked when the read method is called without
	 * specifying an object to restore.
	 */
	protected Function<?, ? extends T> function;

	/**
	 * Constructs a new uniform converter that wraps the specified converter
	 * and uses the specified function as factory method.
	 *
	 * @param converter the converter to be wrapped.
	 * @param function a factory method that is used for initializing the
	 *        object to read when it is not specified.
	 */
	public UniformConverter(Converter<T> converter, Function<?, ? extends T> function) {
		this.converter = converter;
		this.function = function;
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
	 *        object.
	 * @param object the object to be restored. If the object is
	 *        <code>null</code> it is initialized by invoking the function
	 *        (factory method).
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public T read(DataInput dataInput, T object) throws IOException {
		return converter.read(dataInput, object == null && function != null ? function.invoke() : object);
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
		converter.write(dataOutput, object);
	}
}
