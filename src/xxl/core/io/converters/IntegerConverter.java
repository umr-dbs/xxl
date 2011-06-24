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
 * <code>Integer</code> objects. In addition to the read and write methods that
 * read or write <code>Integer</code> objects this class contains readInt and
 * writeInt methods that convert the <code>Integer</code> object after reading
 * or before writing it to its primitive <code>int</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *
 *   // write an Integer and an int value to the output stream
 *
 *   IntegerConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), 42);
 *   IntegerConverter.DEFAULT_INSTANCE.writeInt(new DataOutputStream(output), 666);
 *
 *   // create a byte array input stream on the output stream
 *
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *
 *   // read an int value and an Integer from the input stream
 *
 *   int i1 = IntegerConverter.DEFAULT_INSTANCE.readInt(new DataInputStream(input));
 *   Integer i2 = IntegerConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *
 *   // print the value and the object
 *
 *   System.out.println(i1);
 *   System.out.println(i2);
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
public class IntegerConverter extends FixedSizeConverter<Integer> {

	/**
	 * This instance can be used for getting a default instance of
	 * IntegerConverter. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of
	 * IntegerConverter.
	 */
	public static final IntegerConverter DEFAULT_INSTANCE = new IntegerConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>int</code> value of an <code>Integer</code> object. Because this
	 * size is predefined it must not be measured each time.
	 */
	public static final int SIZE = 4;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public IntegerConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>int</code> value for the specified
	 * (<code>Integer</code>) object from the specified data input and returns
	 * the restored object. <br />
	 * This implementation ignores the specified object and returns a new
	 * <code>Integer</code> object. So it does not matter when the specified
	 * object is <code>null</code>.
	 *
	 * @param dataInput the stream to read the <code>int</code> value from in
	 *        order to return an <code>Integer</code> object.
	 * @param object the (<code>Integer</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Integer</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Integer read(DataInput dataInput, Integer object) throws IOException {
		return dataInput.readInt();
	}

	/**
	 * Reads the <code>int</code> value from the specified data input and
	 * returns it. <br />
	 * This implementation uses the read method and converts the returned
	 * <code>Integer</code> object to its primitive <code>int</code> type.
	 *
	 * @param dataInput the stream to read the <code>int</code> value from.
	 * @return the read <code>int</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public int readInt(DataInput dataInput) throws IOException {
		return dataInput.readInt();
	}

	/**
	 * Writes the <code>int</code> value of the specified <code>Integer</code>
	 * object to the specified data output. <br />
	 * This implementation calls the writeInt method of the data output with
	 * the <code>int</code> value of the object.
	 *
	 * @param dataOutput the stream to write the <code>int</code> value of the
	 *        specified <code>Integer</code> object to.
	 * @param object the <code>Integer</code> object that <code>int</code>
	 *        value should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Integer object) throws IOException {
		dataOutput.writeInt(object.intValue());
	}

	/**
	 * Writes the specified <code>int</code> value to the specified data
	 * output. <br />
	 * This implementation calls the write method with an
	 * <code>Integer</code> object wrapping the specified <code>int</code>
	 * value.
	 *
	 * @param dataOutput the stream to write the specified <code>int</code>
	 *        value to.
	 * @param i the <code>int</code> value that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeInt(DataOutput dataOutput, int i) throws IOException {
		dataOutput.writeInt(i);
	}
}
