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
 * <code>Character</code> objects. In addition to the read and write methods
 * that read or write <code>Character</code> objects this class contains
 * <code>readChar</code> and <code>writeChar</code> methods that convert the
 * <code>Character</code> object after reading or before writing it to its
 * primitive <code>char</code> type.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write a Character and a char value to the output stream
 *   
 *   CharacterConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), 'C');
 *   CharacterConverter.DEFAULT_INSTANCE.writeChar(new DataOutputStream(output), 'p');
 *   
 *   // create a byte array input stream on the output stream
 *   
 *   ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
 *   
 *   // read a char value and a Character from the input stream
 *   
 *   char c1 = CharacterConverter.DEFAULT_INSTANCE.readChar(new DataInputStream(input));
 *   Character c2 = CharacterConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the value and the object
 *   
 *   System.out.println(c1);
 *   System.out.println(c2);
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
public class CharacterConverter extends FixedSizeConverter<Character> {

	/**
	 * This instance can be used for getting a default instance of a charcter
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * character converter.
	 */
	public static final CharacterConverter DEFAULT_INSTANCE = new CharacterConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>char</code> value of a <code>Character</code> object. Because this
	 * size is predefined it must not be measured each time.
	 */
	public static final int SIZE = 2;

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public CharacterConverter() {
		super(SIZE);
	}

	/**
	 * Reads the <code>char</code> value for the specified
	 * (<code>Character</code>) object from the specified data input and
	 * returns the restored object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * <code>Character</code> object. So it does not matter when the specified
	 * object is <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read the <code>char</code> value from in
	 *        order to return a <code>Character</code> object.
	 * @param object the (<code>Character</code>) object to be restored. In
	 *        this implementation it is ignored.
	 * @return the read <code>Character</code> object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public Character read(DataInput dataInput, Character object) throws IOException {
		return dataInput.readChar();
	}

	/**
	 * Reads the <code>char</code> value from the specified data input and
	 * returns it.
	 * 
	 * <p>This implementation uses the read method and converts the returned
	 * <code>Character</code> object to its primitive <code>char</code>
	 * type.</p>
	 *
	 * @param dataInput the stream to read the <code>char</code> value from.
	 * @return the read <code>char</code> value.
	 * @throws IOException if I/O errors occur.
	 */
	public char readChar(DataInput dataInput) throws IOException {
		return read(dataInput, null);
	}

	/**
	 * Writes the <code>char</code> value of the specified
	 * <code>Character</code> object to the specified data output.
	 * 
	 * <p>This implementation calls the write method of the data output with
	 * the <code>char</code> value of the object.</p>
	 *
	 * @param dataOutput the stream to write the <code>char</code> value of the
	 *        specified <code>Character</code> object to.
	 * @param object the <code>Character</code> object that <code>char</code>
	 *        value should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Character object) throws IOException {
		dataOutput.writeChar(object);
	}

	/**
	 * Writes the specified <code>char</code> value to the specified data
	 * output.
	 * 
	 * <p>This implementation calls the write method with a
	 * <code>Character</code> object wrapping the specified <code>char</code>
	 * value.</p>
	 *
	 * @param dataOutput the stream to write the specified
	 *        <tt>char</tt> value to.
	 * @param c the <tt>char</tt> value that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void writeChar(DataOutput dataOutput, char c) throws IOException {
		write(dataOutput, c);
	}
}
