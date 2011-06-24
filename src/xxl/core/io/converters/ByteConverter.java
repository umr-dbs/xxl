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
 * <code>Byte</code> objects. In addition to the read and write methods that
 * read or write <code>Byte</code> objects this class contains
 * <code>readByte</code> and <code>writeByte</code> methods that convert the
 * <code>Byte</code> object after reading or before writing it to its primitive
 * <code>byte</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Byte and a byte value to the output stream
 *   
 *   ByteConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), (byte)-1);
 *   ByteConverter.DEFAULT_INSTANCE.writeByte(new DataOutputStream(output), (byte)27);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a byte value and a Byte from the input stream
 *   
 *   byte b1 = ByteConverter.DEFAULT_INSTANCE.readByte(new DataInputStream(input));
 *   Byte b2 = ByteConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the value and the object
 *   
 *   System.out.println(b1);
 *   System.out.println(b2);
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
public class ByteConverter extends FixedSizeConverter<Byte> {

	/**
	 * This instance can be used for getting a default instance of a byte
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * byte converter.
	 */
	public static final ByteConverter DEFAULT_INSTANCE = new ByteConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>byte</code> value of a <code>Byte</code> object. Because this size
	 * is predefined it must not be measured each time.
	 */
	public static final int SIZE = 1;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public ByteConverter() {
		super(1);
	}

	/**
	 * Reads the <code>byte</code> value for the specified (<code>Byte</code>)
	 * object from the specified data input and returns the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Byte</code> object. So it does not matter when the specified
	 * object is <code>null</code>.
	 *
	 * @param dataInput the stream to read the <code>byte</code> value from in
	 *        order to return a <code>Byte</code> object.
	 * @param object the (<code>Byte</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Byte</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Byte read(DataInput dataInput, Byte object) throws IOException {
		return dataInput.readByte();
	}

	/**
	 * Reads the <code>byte</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Byte</code> object to its primitive <code>byte</code> type.</p>
	 *
	 * @param dataInput the stream to read the <code>byte</code> value from.
	 * @return the read <code>byte</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public byte readByte(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the <code>byte</code> value of the specified <code>Byte</code>
	 * object to the specified data output.
	 * 
	 * <p>This implementation calls the write method of the data output with
	 * the <code>byte</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>byte</code> value of the
	 *        specified <code>Byte</code> object to.
	 * @param object the <code>Byte</code> object that <code>byte</code> value
	 *        should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Byte object) throws IOException {
		dataOutput.write(object);
	}

	/**
	 * Writes the specified <code>byte</code> value to the specified data
	 * output.
	 * 
	 * <p> This implementation calls the write method with a <code>Byte</code>
	 * object wrapping the specified <code>byte</code> value.</p>
	 *
	 * @param dataOutput the stream to write the specified <code>byte</code>
	 *        value to.
	 * @param b the <code>byte</code> value that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeByte(DataOutput dataOutput, byte b) throws IOException {
		write(dataOutput, b);
	}
}
