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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import xxl.core.io.HeadOfDataInput;

/**
 * This class is useful if a converter reads until the end of the input stream,
 * but the block used for storing the data is larger than the data. This is
 * important if you store your data inside a larger block. Then, reading the
 * block with a converter will fail, because the converter does not know where
 * to stop converting. A fixed-size block converter stores the real number of
 * bytes inside the first four bytes and stores the data after that. So, when
 * reading the data, the appropriate <code>EOFException</code> is thrown.
 *
 * @param <T> the type to be converted.
 * @see DataInput
 * @see DataOutput
 * @see IOException
 */
public class FixedSizeBlockConverter<T> extends Converter<T> {

	/**
	 * The converter which is decorated.
	 */
	protected Converter<T> converter;

	/**
	 * The number of bytes to read at most.
	 */
	protected int size;

	/**
	 * Constructs a new fixed-Size block converter.
	 * 
	 * @param converter the converter which reads at an unspecified length. The
	 *        converter is not allowed to store more than size-4 bytes
	 *        (respectively read them).
	 * @param size the maximum size possible.
	 */
	public FixedSizeBlockConverter(Converter<T> converter, int size) {
		this.converter = converter;
		this.size = size;
	}

	/**
	 * Returns the number of bytes which can be read at most.
	 * 
	 * @return The number of bytes.
	 */
	public int getAvailableSize() {
		return size - 4;
	}

	/**
	 * Reads in a object for the specified one from the given data input and
	 * returns it.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * one. So it does not matter when the specified object is
	 * <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read a string from in order to
	 *        return an decompressed byte array.
	 * @param object the byte array to be decompressed. In
	 *        this implementation it is ignored.
	 * @return the read decompressed byte array.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public T read(DataInput dataInput, T object) throws IOException {
		int currentSize = dataInput.readInt();
		return converter.read(new HeadOfDataInput(dataInput, currentSize), object);
	}

	/**
	 * Writes the specified object to the given data output.
	 *
	 * @param dataOutput the stream to write the specified object to.
	 * @param object the object that should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(final DataOutput dataOutput, T object) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		converter.write(dos, object);
		dos.flush();
		dos.close();
		
		byte []b = bos.toByteArray();
		dataOutput.writeInt(b.length);
		dataOutput.write(b);
		
		bos.close();
	}

}
