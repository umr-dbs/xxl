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
 * <code>int</code> values. First the converter reads or writes the length of
 * the <code>int</code> array. Thereafter the <code>int</code> values are read
 * or written.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create an int array
 *   
 *   int[] array = {42, 4711, 666, 190, 77};
 *   
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write array to the output stream
 *   
 *   IntegerArrayConverter.DEFAULT_INSTANCE.write(new DataOutputStream(output), array);
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
 *   array = IntegerArrayConverter.DEFAULT_INSTANCE.read(new DataInputStream(input));
 *   
 *   // print the array
 *   
 *   for (int i : array)
 *       System.out.println(i);
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
public class IntegerArrayConverter extends SizeConverter<int[]> {

	/**
	 * This instance can be used for getting a default instance of an integer
	 * array converter. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of an
	 * integer array converter.
	 */
	public static final IntegerArrayConverter DEFAULT_INSTANCE = new IntegerArrayConverter();

	/**
	 * Determines the length of the conversion of an int array.
	 * 
	 * @param withLengthInfo has to be <code>true</code> iff the length info of
	 *        the array has to be stored.
	 * @param length the length of the array to be stored.
	 * @return the length of the conversion of an int array in bytes.
	 */
	public static int getSizeForArray(boolean withLengthInfo, int length) {
		return (withLengthInfo ? 4 : 0) + 4*length;
	}
	
	/** 
	 * Determines the length of the array. If it is <code>-1</code>, then the 
	 * size information is also serialized.
	 */
	protected int arraySize;

	/**
	 * Constructors an integer array converter which not necessarily
	 * serializes/deserializes the length of the array.
	 * 
	 * @param arraySize if <code>-1</code> then the size is not
	 *        serialized/deserialized. Else this int value represents the
	 *        number of elements which are serialized/deserialized.
	 */
	public IntegerArrayConverter(int arraySize) {
		this.arraySize = arraySize;
	}

	/**
	 * Constructors an integer array converter which also
	 * serializes/deserializes the length of the array.
	 */
	public IntegerArrayConverter() {
		this(-1);
	}

	/**
	 * Reads an array of <code>int</code> values from the specified data input
	 * and returns the restored <code>int</code> array.
	 * 
	 * <p>When the specified <code>int</code> array is <code>null</code> this
	 * implementation returns a new array of <code>int</code> values,
	 * otherwise the size of the specified int array has to be sufficient.</p>
	 *
	 * @param dataInput the stream to read the <code>int</code> array from.
	 * @param object the <code>int</code> array to be restored.
	 * @return the read array of <code>int</code> values.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public int[] read(DataInput dataInput, int[] object) throws IOException {
		int length = arraySize;
		if (length == -1)
			length = dataInput.readInt();
		if (object == null)
			object = new int[length];

		for (int i = 0; i < object.length; i++)
			object[i] = dataInput.readInt();
		return object;
	}

	/**
	 * Writes the specified array of <code>int</code> values to the specified
	 * data output.
	 * 
	 * <p>This implementation first writes the length of the array to the data
	 * output. Thereafter the <code>int</code> values are written.</p>
	 *
	 * @param dataOutput the stream to write the <code>int</code> array to.
	 * @param object the <code>int</code> array that should be written to the
	 *        data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, int[] object) throws IOException {
		int len = arraySize;
		
		if (len == -1) {
			dataOutput.writeInt(object.length);
			len = object.length;
		}
		for (int i = 0; i < len; i++)
			dataOutput.writeInt(object[i]);
	}

	/**
	 * Determines the size of the int array in bytes.
	 * 
	 * @param object the int array.
	 * @return the size of the int array in bytes.
	 * @see xxl.core.io.converters.SizeConverter#getSerializedSize(java.lang.Object)
	 */
	@Override
	public int getSerializedSize(int[] object) {
		return arraySize==-1 ?
			getSizeForArray(true, object.length) :
			getSizeForArray(false, arraySize);
	}
}
