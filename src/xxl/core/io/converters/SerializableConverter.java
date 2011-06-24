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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * This class provides a converter that is able to read and write objects that
 * implements the <code>Serializable</code> interface. In order to read and
 * write serializable objects, an object input or object output is needed.
 * Because of this, the read and write methods expect data inputs and data
 * outputs of the type <code>ObjectInput</code> and <code>ObjectOutput</code>.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Double, an Integer and a String to the output stream
 *   
 *   SerializableConverter.DEFAULT_INSTANCE.write(new ObjectOutputStream(output), Math.E);
 *   SerializableConverter.DEFAULT_INSTANCE.write(new ObjectOutputStream(output), 42);
 *   SerializableConverter.DEFAULT_INSTANCE.write(new ObjectOutputStream(output), "Universum");
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a Double, an Integer and a String from the input stream
 *   
 *   Double d = (Double)SerializableConverter.DEFAULT_INSTANCE.read(new ObjectInputStream(input));
 *   Integer i = (Integer)SerializableConverter.DEFAULT_INSTANCE.read(new ObjectInputStream(input));
 *   String s = (String)SerializableConverter.DEFAULT_INSTANCE.read(new ObjectInputStream(input));
 *   
 *   // print the objects
 *   
 *   System.out.println(d);
 *   System.out.println(i);
 *   System.out.println(s);
 *   
 *   // close the streams after use
 *   
 *   input.close();
 *   output.close();
 * </pre></code></p>
 *
 * @see DataInput
 * @see DataOutput
 * @see IOException
 * @see ObjectInput
 * @see ObjectOutput
 */
public class SerializableConverter extends Converter<Serializable> {

	/**
	 * This instance can be used for getting a default instance of a
	 * serializable converter. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * a serializable converter.
	 */
	public static final SerializableConverter DEFAULT_INSTANCE = new SerializableConverter();

	/**
	 * Reads the state (the attributes) for the specified object from the
	 * specified data input and returns the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns the
	 * result of the <code>readObject</code> method of the specified data input
	 * (object input). So it does not matter when the specified object is
	 * <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *        object. In order to use the serialization mechanism of Java, this
	 *        data input must be an object input.
	 * @param object the object to be restored. In this implementation it is
	 *        ignored.
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Serializable read(DataInput dataInput, Serializable object) throws IOException {
		try {
			return (Serializable)((ObjectInput)dataInput).readObject();
		}
		catch (ClassNotFoundException e) {
			return new IOException(e.getMessage());
		}
	}

	/**
	 * Writes the state (the attributes) of the specified object to the
	 * specified data output.
	 * 
	 * <p>This implementation calls the <code>writeObject</code> method of the
	 * specified data output (object output).</p>
	 *
	 * @param dataOutput the stream to write the state (the attributes) of the
	 *        object to. In order to use the serialization mechanism of Java,
	 *        this data output must be an object output.
	 * @param object the object whose state (attributes) should be written to
	 *        the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Serializable object) throws IOException {
		((ObjectOutput)dataOutput).writeObject(object);
	}
}
