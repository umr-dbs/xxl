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
 * <code>Long</code> objects. In addition to the read and write methods that
 * read or write <code>Long</code> objects this class contains
 * <code>readLong</code> and <code>writeLong</code> methods that convert the
 * <code>Long</code> object after reading or before writing it to its primitive
 * <code>long</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Long and a long value to the output stream
 *   
 *   LongConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), 1234567890l);
 *   LongConverter.DEFAULT_INSTANCE.writeLong(new java.io.DataOutputStream(output), 123456789012345l);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a long value and a Long from the input stream
 *   
 *   long l1 = LongConverter.DEFAULT_INSTANCE.readLong(new DataInputStream(input));
 *   Long l2 = LongConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the value and the object
 *   
 *   System.out.println(l1);
 *   System.out.println(l2);
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
public class LongConverter extends FixedSizeConverter<Long> {

	/**
	 * This instance can be used for getting a default instance of a long
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * long converter.
	 */
	public static final LongConverter DEFAULT_INSTANCE = new LongConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>long</code> value of a <code>Long</code> object. Because this size
	 * is predefined it must not be measured each time.
	 */
	public static final int SIZE = 8;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public LongConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>long</code> value for the specified (<code>Long</code>)
	 * object from the specified data input and returns the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Long</code> object. So it does not matter when the specified
	 * object is <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read the <code>long</code> value from in
	 *        order to return a <code>Long</code> object.
	 * @param object the (<code>Long</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Long</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Long read(DataInput dataInput, Long object) throws IOException {
		return dataInput.readLong();
	}

	/**
	 * Reads the <code>long</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Long</code> object to its primitive <code>long</code> type.</p>
	 *
	 * @param dataInput the stream to read the <code>long</code> value from.
	 * @return the read <code>long</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public long readLong(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the <code>long</code> value of the specified <code>Long</code>
	 * object to the specified data output.
	 * 
	 * <p>This implementation calls the writeLong method of the data output
	 * with the <tcodet>long</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>long</code> value of the
	 *        specified <code>Long</code> object to.
	 * @param object the <code>Long</code> object that <code>long</code> value
	 *        should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Long object) throws IOException{
		dataOutput.writeLong(object);
	}

	/**
	 * Writes the specified <code>long</code> value to the specified data
	 * output.
	 * 
	 * <p>This implementation calls the write method with a <code>Long</code>
	 * object wrapping the specified <code>long</code> value.</p>
	 *
	 * @param dataOutput the stream to write the specified <code>long</code>
	 *        value to.
	 * @param l the <code>long</code> value that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeLong (DataOutput dataOutput, long l) throws IOException {
		write(dataOutput, l);
	}
}
