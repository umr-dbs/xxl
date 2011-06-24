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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import xxl.core.io.DataInputInputStream;
import xxl.core.io.DataOutputOutputStream;

/**
 * This class provides a converter that compresses/decompresses everything
 * what a different converter writes/reads. Only big Objects (>256 Bytes) are
 * worth being compressed!
 *
 * @param <T> the type to be converted.
 * @see DataInput
 * @see DataOutput
 * @see IOException
 */
public class ZipConverter<T> extends Converter<T> {

	/**
	 * The converter which is decorated.
	 */
	protected Converter<T> converter;

	/**
	 * Constructs a new zip converter.
	 * 
	 * @param converter a converter which converts the uncompressed byte arrays
	 *        into objects of a certain type and vice versa.
	 */
	public ZipConverter(Converter<T> converter) {
		this.converter = converter;
	}

	/**
	 * Reads in a zip compressed object for the specified from the given data
	 * input and returns the decompressed object.
	 * 
	 * <p>This implementation ignores the specified object and returns a new
	 * byte array. So it does not matter when the specified object is
	 * <code>null</code>.</p>
	 *
	 * @param dataInput the stream to read a string from in order to return an
	 *        decompressed byte array.
	 * @param object the object to be decompressed. In this implementation it
	 *        is ignored.
	 * @return the read decompressed byte array.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public T read(final DataInput dataInput, T object) throws IOException {
		ZipInputStream zis = new ZipInputStream(new DataInputInputStream(dataInput));
		// ZipEntry entry
		zis.getNextEntry();
		return converter.read(new DataInputStream(zis), object);
	}

	/**
	 * Writes the specified object compressed to the specified data output.
	 *
	 * @param dataOutput the stream to write the string representation of the
	 *        specified object to.
	 * @param object the object that should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(final DataOutput dataOutput, T object) throws IOException {
		ZipOutputStream zos = new ZipOutputStream(new DataOutputOutputStream(dataOutput));
		zos.setMethod(ZipOutputStream.DEFLATED);
		zos.putNextEntry(new ZipEntry("a"));
		DataOutputStream dos = new DataOutputStream(zos);
		
		converter.write(dos, object);
		dos.flush();
		zos.finish();
		dos.close();
	}
}
