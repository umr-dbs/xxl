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
import xxl.core.io.Convertable;

/**
 * This class provides a converter that is able to read and write objects that
 * implements the <code>Convertable</code> interface. The objects are read and
 * written by calling their read and write methods. When reading an object a
 * factory method can be used for creating the object instead of specifying it.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // create two convertable objects (bit sets)
 *   
 *   BitSet b1 = new BitSet(13572l);
 *   BitSet b2 = new BitSet(-1l);
 *   
 *   // create a factory method for bit sets
 *   
 *   Function&lt;Object, BitSet&gt; factory = new Function&lt;Object, BitSet&gt;() {
 *       public BitSet invoke() {
 *           return new BitSet();
 *       }
 *   };
 *   
 *   // create a new convertable converter that converts bit sets
 *   
 *   ConvertableConverter&lt;BitSet&gt; converter = new ConvertableConverter&lt;BitSet&gt;(factory);
 *   
 *   // write the bit sets to the output stream
 *   
 *   converter.write(new DataOutputStream(output), b1);
 *   converter.write(new DataOutputStream(output), b2);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read the bit sets from the input stream and compare it to
 *   // the original bit sets
 *   
 *   System.out.println(b1.compareTo(converter.read(new DataInputStream(input))));
 *   System.out.println(b2.compareTo(converter.read(new DataInputStream(input))));
 *   
 *   // close the streams after use
 *   
 *   input.close();
 *   output.close();
 * </pre></code></p>
 *
 * @param <T> the type of the object that can be converted by using this
 *        converter (must be a subtype of <code>Convertable</code>).
 * @see DataInput
 * @see DataOutput
 * @see Function
 * @see IOException
 */
public class ConvertableConverter<T extends Convertable> extends Converter<T> {

	/**
	 * This instance can be used for getting a default instance of a
	 * convertable converter. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * a convertable converter.
	 */
	public static final ConvertableConverter<Convertable> DEFAULT_INSTANCE = new ConvertableConverter<Convertable>();

	/**
	 * A factory method that is used for initializing the object to read. This
	 * function will be invoked when the read method is called without
	 * specifying an object to restore.
	 */
	protected Function<?, ? extends T> function;


	/**
	 * Constructs a new convertable converter and uses the specified function
	 * as factory method. For this reason the object to read must not be
	 * specified when calling the read method. In this case the object is
	 * initialized be invoking the specified function.
	 *
	 * @param function a factory method that is used for initializinge the
	 *        object to read when it is not specified.
	 */
	public ConvertableConverter(Function<?, ? extends T> function) {
		this.function = function;
	}

	/**
	 * Constructs a new convertable converter without a factory method. For
	 * this reason the objects to read must be explicitly specified when
	 * calling the read method.
	 */
	public ConvertableConverter() {
		this(null);
	}

	/**
	 * Reads the state (the attributes) for the specified object from the
	 * specified data input and returns the restored object.
	 * 
	 * <p>This implementation uses the specified object (that implements the
	 * convertable interface) to call its read method. When this object is
	 * <code>null</code> it is initialized by invoking the function (factory
	 * method).
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *        object.
	 * @param object the object to be restored. If the object is null it is
	 *        initialized by invoking the function (factory method).
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 * @throws NullPointerException when the given object is null and no
	 *         factory method is specified.
	 */
	@Override
	public T read(DataInput dataInput, T object) throws IOException {
		if (object == null) {
			if (function == null)
				throw new NullPointerException("missing factory method");
			object = function.invoke();
		}
		object.read(dataInput);        //fill the object with data
		return object;
	}

	/**
	 * Writes the state (the attributes) of the specified object to the
	 * specified data output.
	 * 
	 * <p>This implementation calls the write method of the specified object
	 * (that implements the convertable interface).</p>
	 *
	 * @param dataOutput the stream to write the state (the attributes) of the
	 *        object to.
	 * @param object the object whose state (attributes) should be written to
	 *        the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, T object) throws IOException {
		object.write(dataOutput);
	}
}
