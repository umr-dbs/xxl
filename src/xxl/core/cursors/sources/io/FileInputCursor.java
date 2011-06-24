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

package xxl.core.cursors.sources.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import xxl.core.io.converters.Converter;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a cursor that depends on a given file. It iterates over
 * the objects that are read out of the underlying file. A converter is used in
 * order to read out the serialized objects. This class implements only a few
 * constructors. These constructers create a new input stream cursor that
 * depends on a file input stream.
 * 
 * <p><b>Example usage (1):</b>
 * <pre>
 *   // create a new file
 *
 *   File file = new File("file.dat");
 *
 *   // catch IOExceptions
 *
 *   try {
 *
 *       // create a random access file on that file
 *
 *       RandomAccessFile output = new RandomAccessFile(file, "rw");
 *
 *       // write some data to that file
 *
 *       output.writeUTF("Some data.");
 *       output.writeUTF("More data.");
 *       output.writeUTF("Another bundle of data.");
 *       output.writeUTF("A last bundle of data.");
 *
 *       // close the random access file
 *
 *       output.close();
 *   }
 *   catch (Exception e) {
 *       System.out.println("An error occured.");
 *   }
 *
 *   // create a new file input cursor with ...
 *
 *   FileInputCursor&lt;String&gt; cursor = new FileInputCursor&lt;String&gt;(
 *
 *       // a string converter
 *
 *       StringConverter.DEFAULT_INSTANCE,
 *
 *       // the created file
 *
 *       file
 *   );
 * 
 *   // open the cursor
 * 
 *   cursor.open();
 *
 *   // print all elements of the cursor
 *
 *   while (cursor.hasNext())
 *       System.out.println(cursor.next());
 * 
 *   // close the cursor
 * 
 *   cursor.close();
 *
 *   // delete the file
 *
 *   file.delete();
 * </pre></p>
 *
 * @param <E> the type of the elements read from the underlying file.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.sources.io.InputStreamCursor
 * @see java.io.File
 */
public class FileInputCursor<E> extends InputStreamCursor<E> {

	/**
	 * Constructs a new file-input cursor that depends on the specified file
	 * and uses the specified converter in order to read out the serialized
	 * objects. An internal buffer of size <code>bufferSize</code> is used for
	 * the file input.
	 *
	 * @param converter the converter that is used for reading out the
	 *        serialized objects of this iteration.
	 * @param file the file that contains the serialized objects of this
	 *        iteration.
	 * @param bufferSize the size of the buffer that is used for the file
	 *        input.
	 */
	public FileInputCursor(Converter<? extends E> converter, File file, int bufferSize) {
		super(null, converter);
		try {
			input = new DataInputStream(
				new BufferedInputStream(
					new FileInputStream(file),
					bufferSize
				)
			);
		}
		catch (IOException ie) {
			throw new WrappingRuntimeException(ie);
		}
	}

	/**
	 * Constructs a new file-input cursor that depends on the specified file
	 * and uses the specified converter in order to read out the serialized
	 * objects. This constructor is equivalent to the call of
	 * <pre>
	 *   new FileInputIterator(converter, file, 4096)
	 * </pre>.
	 *
	 * @param converter the converter that is used for reading out the
	 *        serialized objects of this iteration.
	 * @param file the file that contains the serialized objects of this
	 *        iteration.
	 */
	public FileInputCursor(Converter<? extends E> converter, File file) {
		this(converter, file, 4096);
	}
}
