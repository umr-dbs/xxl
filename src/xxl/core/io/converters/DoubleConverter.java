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
 * <code>Double</code> objects. In addition to the read and write methods that
 * read or write <code>Double</code> objects this class contains
 * <code>readDouble</code> and <code>writeDouble</code> methods that convert
 * the <code>Double</code> object after reading or before writing it to its
 * primitive <code>double</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Double and a double value to the output stream
 *   
 *   DoubleConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), 2.7236512);
 *   DoubleConverter.DEFAULT_INSTANCE.writeDouble(new DataOutputStream(output), 6.123853);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a double value and a Double from the input stream
 *   
 *   double d1 = DoubleConverter.DEFAULT_INSTANCE.readDouble(new DataInputStream(input));
 *   Double d2 = DoubleConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the value and the object
 *   
 *   System.out.println(d1);
 *   System.out.println(d2);
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
public class DoubleConverter extends FixedSizeConverter<Double> {

	/**
	 * This instance can be used for getting a default instance of a double
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * double converter.
	 */
	public static final DoubleConverter DEFAULT_INSTANCE = new DoubleConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>double</code> value of a <code>Double</code> object. Because this
	 * size is predefined it must not be measured each time.
	 */
	public static final int SIZE = 8;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public DoubleConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>double</code> value for the specified
	 * (<code>Double</code>) object from the specified data input and returns
	 * the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Double</code> object. So it does not matter when the specified
	 * object is <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read the <code>double</code> value from
	 *        in order to return a <code>Double</code> object.
	 * @param object the (<code>Double</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>Double</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Double read(DataInput dataInput, Double object) throws IOException {
		return dataInput.readDouble();
	}

	/**
	 * Reads the <code>double</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Double</code> object to its primitive <code>double</code>
	 * type.</p>
	 *
	 * @param dataInput the stream to read the <code>double</code> value from.
	 * @return the read <code>double</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public double readDouble(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the <code>double</code> value of the specified
	 * <code>Double</code> object to the specified data output.
	 * 
	 * <p>This implementation calls the writeDouble method of the data output
	 * with the <code>double</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>double</code> value of
	 *        the specified <code>Double</code> object to.
	 * @param object the <code>Double</code> object that <code>double</code>
	 *        value should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Double object) throws IOException {
		dataOutput.writeDouble(object);
	}

	/**
	 * Writes the specified <code>double</code> value to the specified data
	 * output.
	 * 
	 * <p>This implementation calls the write method with a <code>Double</code>
	 * object wrapping the specified <code>double</code> value.</p>
	 *
	 * @param dataOutput the stream to write the specified <code>double</code>
	 *        value to.
	 * @param d the <code>double</code> value that should be written to the
	 *        data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeDouble(DataOutput dataOutput, double d) throws IOException {
		write(dataOutput, d);
	}
}
