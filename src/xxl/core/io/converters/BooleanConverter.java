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
 * <code>Boolean</code> objects. In addition to the read and write methods that
 * read or write <code>Boolean</code> objects this class contains
 * <code>readBoolean</code> and <code>writeBoolean</code> methods that convert
 * the <code>Boolean</code> object after reading or before writing it to its
 * primitive <code>boolean</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Boolean and a boolean value to the output stream
 *   
 *   BooleanConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), true);
 *   BooleanConverter.DEFAULT_INSTANCE.writeBoolean(new DataOutputStream(output), false);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a boolean value and a Boolean from the input stream
 *   
 *   boolean b1 = BooleanConverter.DEFAULT_INSTANCE.readBoolean(new DataInputStream(input));
 *   Boolean b2 = BooleanConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
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
public class BooleanConverter extends FixedSizeConverter<Boolean> {

	/**
	 * This instance can be used for getting a default instance of a boolean
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * boolean converter.
	 */
	public static final BooleanConverter DEFAULT_INSTANCE = new BooleanConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>boolean</code> value of a <code>Boolean</code> object. Because
	 * this size is predefined it must not be measured each time.
	 */
	public static final int SIZE = 1;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public BooleanConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>boolean</code> value for the specified
	 * (<code>Boolean</code>) object from the specified data input and returns
	 * the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Boolean</code> object. So it does not matter when the specified
	 * object is <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read the <code>boolean</code> value from
	 *        in order to return a <code>Boolean</code> object.
	 * @param object the (<code>Boolean</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Boolean</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Boolean read(DataInput dataInput, Boolean object) throws IOException {
		return dataInput.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
	}

	/**
	 * Reads the <code>boolean</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Boolean</code> object to its primitive <code>boolean</code>
	 * type.</p>
	 *
	 * @param dataInput the stream to read the <code>boolean</code> value from.
	 * @return the read <code>boolean</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public boolean readBoolean(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the <code>boolean</code> value of the specified
	 * <code>Boolean</code> object to the specified data output.
	 * 
	 * <p>This implementation calls the writeBoolean method of the data output
	 * with the <code>boolean</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>boolean</code> value of
	 *        the specified <code>Boolean</code> object to.
	 * @param object the <code>Boolean</code> object that <code>boolean</code>
	 *        value should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Boolean object) throws IOException {
		dataOutput.writeBoolean(object);
	}

	/**
	 * Writes the specified <code>boolean</code> value to the specified data
	 * output.
	 * 
	 * <p>This implementation calls the write method with a
	 * <code>Boolean</code> object wrapping the specified <code>boolean</code>
	 * value.</p>
	 *
	 * @param dataOutput the stream to write the specified <code>boolean</code>
	 *        value to.
	 * @param b the <code>boolean</code> value that should be written to the
	 *        data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeBoolean(DataOutput dataOutput, boolean b) throws IOException {
		write(dataOutput, b);
	}
}
