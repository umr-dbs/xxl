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
import java.lang.reflect.Array;

/**
 * This class provides a converter that is able to read and write arrays of
 * objects. Therefore the array converter needs a converter that is able to read
 * and write the single objects of the array's component type. First the
 * converter reads or writes the length of the array. Thereafter the objects
 * are read or written.
 * 
 * <p>Example usage (1).
 * <code><pre>
 *   // create an array of 3 object
 *   
 *   Vector&lt;String&gt;[] array = new Vector[3];
 *   
 *   // initialize the objects with vectors and fill them with strings
 *   
 *   array[0] = new Vector&lt;String&gt;();
 *   array[0].add("This");
 *   array[0].add("is");
 *   array[0].add("a");
 *   array[0].add("vector.");
 *   array[1] = new Vector&lt;String&gt;();
 *   array[1].add("This");
 *   array[1].add("also.");
 *   array[2] = new Vector&lt;String&gt;();
 *   array[2].add("No");
 *   array[2].add("it");
 *   array[2].add("does");
 *   array[2].add("not");
 *   array[2].add("really");
 *   array[2].add("make");
 *   array[2].add("any");
 *   array[2].add("sense.");
 *   
 *   // create a converter for vectors
 *   
 *   Converter&lt;Vector&lt;String&gt;&gt; converter = new Converter&lt;Vector&lt;String&gt;&gt;() {
 *   
 *       // how to write a vector
 *   
 *       public void write(DataOutput dataOutput, Vector&lt;String&gt; object) throws IOException {
 *       
 *           // write the size of the vector at first
 *       
 *           IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, object.size());
 *       
 *           // thereafter write the elements of the vector
 *       
 *           for (String string : object)
 *               StringConverter.DEFAULT_INSTANCE.write(dataOutput, string);
 *       }
 *   
 *       // how to read a vector
 *   
 *       public Vector&lt;String&gt; read(DataInput dataInput, Vector&lt;String&gt; object) throws IOException {
 *   
 *           // read the size of the vector at first
 *       
 *           int size = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
 *       
 *           // create a new vector
 *       
 *           Vector&lt;String&gt; vector = new Vector&lt;String&gt;();
 *       
 *           // thereafter read the elements of the vector
 *       
 *           for (int i = 0; i < size; i++)
 *               vector.add(StringConverter.DEFAULT_INSTANCE.read(dataInput));
 *           
 *           // return the restored vector
 *       
 *           return vector;
 *       }
 *   };
 *   
 *   // create an array converter that is able to read and write
 *   // arrays of vectors
 *   
 *   ArrayConverter&lt;Vector&lt;String&gt;&gt; arrayConverter = new ArrayConverter&lt;Vector&lt;String&gt;&gt;(converter);
 *   
 *   // create a byte array output stream
 *   
 *   ByteArrayOutputStream output = new ByteArrayOutputStream();
 *   
 *   // write array to the output stream
 *   
 *   arrayConverter.write(new java.io.DataOutputStream(output), array);
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
 *   array = arrayConverter.read(new DataInputStream(input), new Vector[0]);
 *   
 *   // print the array (the data of the vectors)
 *   
 *   for (Vector&lt;String&gt; vector : array)
 *       System.out.println(vector);
 *       
 *   // close the streams after use
 *   
 *   input.close();
 *   output.close();
 * </pre></code></p>
 *
 * @param <T> the component type of the array to be converted.
 * @see DataInput
 * @see DataOutput
 * @see IOException
 */
public class ArrayConverter<T> extends Converter<T[]> {

	/**
	 * The field <code>converter</code> refers to a converter that is able to
	 * read and write the single objects.
	 */
	protected Converter<T> converter;

	/**
	 * Constructs a new object array converter that is able to read and write
	 * object arrays. The specified converter is used for reading and writing
	 * the single objects.
	 *
	 * @param converter a converter that is able to read and write the single
	 *        objects.
	 */
	public ArrayConverter(Converter<T> converter) {
		this.converter = converter;
	}

	/**
	 * Reads an array of objects from the specified data input and returns the
	 * restored array. In case that the specified object is not
	 * <code>null</code>, this implementation calls the converter with each
	 * object of this array on reading. When the given array is not suitable
	 * for the objects stored in the stream, a new array is created via
	 * reflection.
	 *
	 * @param dataInput the stream to read the object array from.
	 * @param object the object array to be restored. When it is
	 *        <code>null</code> a new array is created via reflection.
	 * @return the restored array of bjects.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	@SuppressWarnings("unchecked") // an array of the correct type is created by using reflection
	public T[] read(DataInput dataInput, T[] object) throws IOException {
		int size = dataInput.readInt();
		if (object == null)
			throw new IllegalArgumentException("an array of the generic type must be specified");
		if (size > object.length)
			object = (T[])Array.newInstance(object.getClass().getComponentType(), size);

		for (int i = 0; i < size; i++)
			object[i] = converter.read(dataInput, object[i]);
		return object;
	}

	/**
	 * Writes the specified array of objects to the specified data output.
	 * 
	 * <p>This implementation first writes the length of the array to the data
	 * output. Thereafter the objects are written by calling the write method
	 * of the internally used converter.</p>
	 *
	 * @param dataOutput the stream to write the object array to.
	 * @param object the object array that should be written to the data
	 *        output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, T[] object) throws IOException {
		dataOutput.writeInt(object.length);
		for (T o : object)
			converter.write(dataOutput, o);
	}
}
