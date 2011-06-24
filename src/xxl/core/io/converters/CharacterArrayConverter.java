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
 * This class provides a converter that is able to read and write arrays of
 * <code>char</code> values. First the converter reads or writes the length of
 * the <code>char</code> array. Thereafter the <code>char</code> values are
 * read or written.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create an char array
 *   
 *   char[] array = {'C', 'h', 'a', 'r', '-', 'A', 'r', 'r', 'a', 'y'};
 *   
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write array to the output stream
 *   
 *   CharacterArrayConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), array);
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // reset the array
 *   
 *   array = null;
 *   
 *   // read array from the input stream
 *   
 *   array = CharacterArrayConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the array
 *   
 *   for (char c : array)
 *       System.out.println(c);
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
public class CharacterArrayConverter extends SizeConverter<char[]> {

	/**
	 * This instance can be used for getting a default instance of character
	 * array converter. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * character array converter.
	 */
	public static final CharacterArrayConverter DEFAULT_INSTANCE = new CharacterArrayConverter();

	/**
	 * Reads an array of <code>char</code> values from the specified data input
	 * and returns the restored <code>char</code> array.
	 * 
	 * <p>When the specified <code>char</code> array is <code>null</code> this
	 * implementation returns a new array of <code>char</code> values,
	 * otherwise the size of the specified char array has to be sufficient.</p>
	 *
	 * @param dataInput the stream to read the <code>char</code> array from.
	 * @param object the <code>char</code> array to be restored.
	 * @return the read array of <code>char</code> values.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public char[] read(DataInput dataInput, char[] object) throws IOException {
		int length = dataInput.readInt();
		if (object == null)
			object = new char[length];
		
		for (int i = 0; i < object.length; i++)
			object[i] = dataInput.readChar();
		return object;
	}

	/**
	 * Writes the specified array of <code>char</code> values to the specified
	 * data output.
	 * 
	 * <p>This implementation first writes the length of the array to the data
	 * output. Thereafter the <code>char</code> values are written.</p>
	 *
	 * @param dataOutput the stream to write the <code>char</code> array to.
	 * @param object the <code>char</code> array that should be written to the
	 *        data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, char[] object) throws IOException {
		dataOutput.writeInt(object.length);
		for (char c : object)
			dataOutput.writeChar(c);
	}

	/**
	 * Determines the size of the character array in bytes.
	 * 
	 * @param object the character array.
	 * @return the size of the character array in bytes.
	 * @see xxl.core.io.converters.SizeConverter#getSerializedSize(java.lang.Object)
	 */
	@Override
	public int getSerializedSize(char[] object) {
		return 4 + 2*object.length;
	}
}
