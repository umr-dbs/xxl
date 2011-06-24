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
 * <code>String</code> objects. In addition to the read and write methods that
 * read or write <code>String</code> objects (these methods get
 * <code>Object</code>s and cast them to <code>String</code> objects) this
 * class contains <code>readUTF</code> and <code>writeUTF</code> methods that
 * gets directly <code>String</code> objects.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write two strings to the output stream
 *   
 *   StringConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), "Hello world!");
 *   StringConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), "That's all, folks!");
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read two strings from the input stream
 *   
 *   String s1 = StringConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   String s2 = StringConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
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
public class StringConverter extends Converter<String> {

	/**
	 * This instance can be used for getting a default instance of a string
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * string converter.
	 */
	public static final StringConverter DEFAULT_INSTANCE = new StringConverter();

	/**
	 * Reads in a string that has been encoded using a modified UTF-8 format
	 * and returns the restored (<code>String</code>) object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>String</code> object by calling the <code>readUTF</code> method of
	 * the specified data input. So it does not matter when the specified
	 * object is <code>null</code></p>.
	 *
	 * @param dataInput the stream to read a modified UTF-8 representation of a
	 *        string from in order to return a <code>String</code> object.
	 * @param object the (<code>String</code>) object to be restored. In this
	 *        implementation it is ignored.
	 * @return the read <code>String</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public String read(DataInput dataInput, String object) throws IOException {
		return dataInput.readUTF();
	}

	/**
	 * Reads in a string that has been encoded using a modified UTF-8 format
	 * and returns the restored (<code>String</code>) object.
	 * 
	 * <p>This implementation calls the read method and casts its result to
	 * <code>String</code>.</p>
	 *
	 * @param dataInput the stream to read a modified UTF-8 representation of a
	 *        string from in order to return a <code>String</code> object.
	 * @return the read <code>String</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	public String readUTF(DataInput dataInput) throws IOException {
		return read(dataInput);
	}

	/**
	 * Writes the specified <code>String</code> object to the specified data
	 * output using a modified UTF-8 format.
	 * 
	 * <p>This implementation uses the writeUTF method of the specified data
	 * output in order to write the specified object.</p>
	 *
	 * @param dataOutput the stream to write the modified UTF-8 representation
	 *        of the specified <code>String</code> object to.
	 * @param object the <code>String</code> object that modified UTF-8
	 *        representation should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, String object) throws IOException {
		dataOutput.writeUTF(object);
	}

	/**
	 * Writes the specified <code>String</code> object to the specified data
	 * output using a modified UTF-8 format.
	 * 
	 * <p>This implementation uses the write method output in order to write
	 * the specified object.</p>
	 *
	 * @param dataOutput the stream to write the modified UTF-8 representation
	 *        of the specified <code>String</code> object to.
	 * @param object the <code>String</code> object that modified UTF-8
	 *        representation should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeUTF(DataOutput dataOutput, String object) throws IOException {
		write(dataOutput, object);
	}
}
