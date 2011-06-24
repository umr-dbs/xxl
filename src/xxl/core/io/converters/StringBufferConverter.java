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
 * This class provides a converter that is able to read and write string
 * buffers. This converter uses the <code>readUTF</code> and
 * <code>writeUTF</code> methods of a data output the read and write the string
 * representations of the string buffers.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write two string bufferss to the output stream
 *   
 *   StringBufferConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), new StringBuffer("Hello world!"));
 *   StringBufferConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), new StringBuffer("That's all, folks!"));
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read two strings from the input stream
 *   
 *   StringBuffer s1 = StringBufferConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   StringBuffer s2 = StringBufferConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
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
public class StringBufferConverter extends Converter<StringBuffer> {

	/**
	 * This instance can be used for getting a default instance of a string
	 * buffer converter. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * string buffer converter.
	 */
	public static final StringBufferConverter DEFAULT_INSTANCE = new StringBufferConverter();

	/**
	 * Reads in a string buffer for the specified object from the specified
	 * data input and returns the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * string buffer. So it does not matter when the specified object is
	 * <code>null</code>. The returned string buffer represents the
	 * <code>String</code> object that is returned by the <code>readUTF</code>
	 * method of the specified data input.</p>
	 *
	 * @param dataInput the stream to read a string from in order to return a
	 *        string buffer object that represents the read string.
	 * @param object the string buffer to be restored. In this implementation
	 *        it is ignored.
	 * @return the read string buffer.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public StringBuffer read(DataInput dataInput, StringBuffer object) throws IOException {
		if (object == null)
			return new StringBuffer(dataInput.readUTF());
		
		object.setLength(0);
		return object.append(dataInput.readUTF());
	}

	/**
	 * Writes the specified strong buffer to the specified data output.
	 * 
	 * <p>This implementation uses the <code>writeUTF</code> method of the
	 * specified data output in order to write the string representation of the
	 * specified object.</p>
	 *
	 * @param dataOutput the stream to write the string representation of the
	 *        specified string buffer to.
	 * @param object the string buffer that string representation should be
	 *        written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, StringBuffer object) throws IOException {
		dataOutput.writeUTF(object.toString());
	}
}
