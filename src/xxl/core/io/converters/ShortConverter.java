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

/**
 * This class provides a converter that is able to read and write
 * <code>Short</code> objects. In addition to the read and write methods that
 * read or write <code>Short</code> objects this class contains
 * <code>readShort</code> and <code>writeShort</code> methods that convert the
 * <code>Short</code> object after reading or before writing it to its
 * primitive <code>short</code> type.
 * 
 * <p>Example uasge (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Short and a short value to the output stream
 *   
 *   ShortConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), (short)42);
 *   ShortConverter.DEFAULT_INSTANCE.writeShort(new DataOutputStream(output), (short)4711);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a long value and a Long from the input stream
 *   
 *   short s1 = ShortConverter.DEFAULT_INSTANCE.readShort(new DataInputStream(input));
 *   Short s2 = ShortConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the value and the object
 *   
 *   System.out.println(s1);
 *   System.out.println(s2);
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
 */
public class ShortConverter extends FixedSizeConverter<Short> {

	/**
	 * This instance can be used for getting a default instance of a short
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * short converter.
	 */
	public static final ShortConverter DEFAULT_INSTANCE = new ShortConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>short</code> value of a <code>Short</code> object. Because this
	 * size is predefined it must not be measured each time.
	 */
	public static final int SIZE = 2;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public ShortConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>short</code> value for the specified
	 * (<code>Short</code>) object from the specified data input and returns
	 * the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Short</code> object. So it does not matter when the specified
	 * object is <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read the <code>short</code> value from in
	 *        order to return a <code>Short</code> object.
	 * @param object the (<code>Short</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Short</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Short read(DataInput dataInput, Short object) throws IOException {
		return dataInput.readShort();
	}

	/**
	 * Reads the <code>short</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Short</code> object to its primitive <code>short</code> type.</p>
	 *
	 * @param dataInput the stream to read the <code>short</code> value from.
	 * @return the read <code>short</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public short readShort(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the <code>short</code> value of the specified <code>Short</code>
	 * object to the specified data output.
	 * 
	 * <p>This implementation calls the writeShort method of the data output
	 * with the <code>short</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>short</code> value of
	 *        the specified <code>Short</code> object to.
	 * @param object the <code>Short</code> object that <code>short</code>
	 *        value should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Short object) throws IOException{
		dataOutput.writeShort(object);
	}

	/**
	 * Writes the specified <code>short</code> value to the specified data
	 * output.
	 * 
	 * <p>This implementation calls the write method with a <code>Short</code>
	 * object wrapping the specified <code>short</code> value.</p>
	 *
	 * @param dataOutput the stream to write the specified <code>short</code>
	 *        value to.
	 * @param s the <code>short</code> value that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeShort(DataOutput dataOutput, short s) throws IOException {
		write(dataOutput, s);
	}
}
