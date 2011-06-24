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
 * <code>boolean</code> values. First the converter reads or writes the length
 * of the <code>boolean</code> array, if necessary. Thereafter the
 * <code>boolean</code> values are read or written (8 booleans per byte).
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create an boolean array
 *
 *   boolean[] array = {true, false, false, true};
 *
 *   // create a byte array output stream
 *
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *
 *   // write array to the output stream
 *
 *   BooleanArrayConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), array);
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
 *   array = BooleanArrayConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *
 *   // print the array
 *
 *   for (boolean b : array)
 *       System.out.println(b);
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
public class BooleanArrayConverter extends SizeConverter<boolean[]> {
	
	/**
	 * This instance can be used for getting a default instance of a boolean
	 * array converter. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * boolean array converter.
	 */
	public static final BooleanArrayConverter DEFAULT_INSTANCE = new BooleanArrayConverter();

	/**
	 * Determines the length of the conversion of a boolean array.
	 * 
	 * @param withLengthInfo has to be <code>true</code> iff the length info of
	 *        the array has to be stored.
	 * @param len the length of the boolean array to be stored.
	 * @return the length of the conversion of a boolean array in bytes.
	 */
	public static int getSizeForArray(boolean withLengthInfo, int len) {
		return (withLengthInfo ? 4 : 0) + (len+7)>>3;  // >>3 <=> /8
	}

	/** 
	 * Determines the length of the array. If it is <code>-1</code>, then the
	 * size information is also serialized.
	 */
	protected int arraySize;

	/**
	 * Constructors a boolean array converter which not necessarily
	 * serializes/deserializes the length of the array.
	 * 
	 * @param arraySize if <code>-1</code> then the size is
	 *        serialized/deserialized. Else this int value represents the
	 *        number of <code>boolean</code>s which are
	 *        serialized/deserialized.
	 */
	public BooleanArrayConverter(int arraySize) {
		this.arraySize = arraySize;
	}

	/**
	 * Constructors a boolean array converter which also
	 * serializes/deserializes the length of the array.
	 */
	public BooleanArrayConverter() {
		this(-1);
	}

	/**
	 * Reads an array of <code>boolean</code> values from the specified data
	 * input and returns the restored <code>boolean</code> array.
	 *
	 * @param dataInput the stream to read the <code>boolean</code> array from.
	 * @param object the <code>boolean</code> array to be restored. The size of
	 *        the <code>boolean</code> array has to be sufficient! If this
	 *        parameter is <code>null</code>, then a new array is constructed.
	 * @return the read array of <code>boolean</code> values.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public boolean[] read(DataInput dataInput, boolean[] object) throws IOException {
		int len = arraySize;
		if (arraySize == -1)
			len = dataInput.readInt();
		if (object == null)
			object = new boolean[len];
		byte b = 0;

		for (int i = 0; i < len; i++) {
			if ((i&7) == 0) // i mod 8 == 0
				b = dataInput.readByte();
			object[i] = b<0;
			b <<= 1;
		}
		return object;
	}

	/**
	 * Writes the specified array of <code>boolean</code> values to the
	 * specified data output.
	 * 
	 * <p>This implementation first writes the length of the array to the data
	 * output. Thereafter the <code>boolean</code> values are written.</p>
	 *
	 * @param dataOutput the stream to write the <code>boolean</code> array to.
	 * @param object the <code>boolean</code> array that should be written to
	 *        the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, boolean[] object) throws IOException {
		byte b = 0;
		int i = 0;
		int len = arraySize;

		if (len == -1) {
			dataOutput.writeInt(object.length);
			len = object.length;
		}
		while (i < len) {
			b <<= 1;
			if (object[i])
				b++;
			if ((++i&7) == 0) {
				dataOutput.writeByte(b);
				b = 0;
			}
		}
		if ((i&7) != 0) {
			b <<= 8-(i&7);
			dataOutput.write(b);
		}
	}

	/**
	 * Determines the size of the <code>boolean</code> array in bytes.
	 * 
	 * @param object a boolean array.
	 * @return the size of the <code>boolean</code> array in bytes.
	 * @see xxl.core.io.converters.SizeConverter#getSerializedSize(java.lang.Object)
	 */
	@Override
	public int getSerializedSize(boolean[] object) {
		return arraySize == -1 ?
			getSizeForArray(true, object.length) :
			getSizeForArray(false, arraySize);
	}

}
