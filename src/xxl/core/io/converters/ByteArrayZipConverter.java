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
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import xxl.core.io.DataInputInputStream;
import xxl.core.io.DataOutputOutputStream;

/**
 * This class provides a converter that converts a byte array into a zip
 * compressed byte array representation and vice versa. Only big byte arrays
 * (>256 Bytes) are worth compressing!
 *
 * @see DataInput
 * @see DataOutput
 * @see IOException
 */
public class ByteArrayZipConverter extends Converter<byte[]> {

	/**
	 * This instance can be used for getting a default instance of a byte array
	 * zip converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * byte array zip converter.
	 */
	public static final ByteArrayZipConverter DEFAULT_INSTANCE = new ByteArrayZipConverter();

	/**
	 * Reads in a zip compressed byte array for the specified from the
	 * specified data input and returns the decompressed byte array.
	 * 
	 * <p>When the specified <code>byte</code> array is <code>null</code> or
	 * the size of the specified byte array is not sufficient, this
	 * implementation returns a new array of <code>byte</code> values.</p>
	 *
	 * @param dataInput the stream to read a string from in order to return an
	 *        decompressed byte array.
	 * @param object the byte array to be decompressed.
	 * @return the read decompressed byte array.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	public byte[] read(DataInput dataInput, byte[] object) throws IOException {
		// decompresses the data from dataInput and returns the decompressed byte array
		ZipInputStream zis = new ZipInputStream(new DataInputInputStream(dataInput));
		// ZipEntry entry
		zis.getNextEntry();
		
		if (object != null && zis.read(object) < object.length)
			// the decompressed data fit into the given byte array
			return object;
		
		// collect the decompressed data into a byte array output stream
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (object != null)
			// write the read data
			baos.write(object);
		if (object == null || object.length < 512)
			// get a well-sized buffer
			object = new byte[512];
		
		int read;
		do {
			read = zis.read(object);
			baos.write(object, 0, Math.max(0, read));
		}
		while (read == object.length);
		
		return baos.toByteArray();
	}

	/**
	 * Writes the specified <code>byte</code> array compressed to the specified
	 * data output.
	 *
	 * @param dataOutput the stream to write the string representation of the
	 *        specified object to.
	 * @param object the byte array object that string representation should be
	 *        written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, byte[] object) throws IOException {
		// compressed the byte array object to DataOutput.
		ZipOutputStream zos = new ZipOutputStream(new DataOutputOutputStream(dataOutput));
		zos.setMethod(ZipOutputStream.DEFLATED);
		zos.putNextEntry(new ZipEntry("a"));
		zos.write(object, 0, object.length);
		zos.finish();
		zos.close();
	}
}
