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

package xxl.core.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class contains various methods connected to the serialization of
 * convertable objects. The write methods write (iterators of) convertable
 * objects to files and buffer the writing process.<p>
 *
 * The documentation of the methods contained in this class includes a
 * brief description of the implementation. Such descriptions should be
 * regarded as implementation notes, rather than parts of the
 * specification. Implementors should feel free to substitute other
 * algorithms, as long as the specification itself is adhered to.
 *
 * @see BufferedOutputStream
 * @see DataOutputStream
 * @see File
 * @see FileOutputStream
 * @see IOException
 * @see Iterator
 * @see SingleObjectCursor
 */
public abstract class Convertables {

	/**
	 * Writes an iterator of convertable objects to the given file and returns the
	 * number of written objects. A buffer of the specified size is used for
	 * buffering the writing process.<br>
	 * This implementation opens a new buffered data output stream on the given
	 * file and writes all objects contained by the specified iterator to it.
	 *
	 * @param outputFile the file to which to write the objects.
	 * @param iterator the iterator containing the convertable objects to be
	 *        written.
	 * @param bufferSize the size of the buffer used for buffering the writing
	 *        process.
	 * @return the number of written objects (number of called to the iterator's
	 *         next method).
	 * @throws WrappingRuntimeException when any exception is throws during the
	 *         object writing process.
	 */
	public static int write (File outputFile, Iterator iterator, int bufferSize) {
		DataOutputStream dataOutput = null;
		int count = 0;
		try {
			dataOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), bufferSize));
			while (iterator.hasNext()) {
				((Convertable)iterator.next()).write(dataOutput);
				count++;
			}
			dataOutput.close();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
		return count;
	}

	/**
	 * Writes an iterator of convertable objects to the given file and returns the
	 * number of written objects. A buffer of <tt>1024*1024</tt> bytes is used for
	 * buffering the writing process.<br>
	 * This implementation is equivalent to the call of
	 * <tt>write(outputFile, iterator, 1024*1024)</tt>.
	 *
	 * @param outputFile the file to which to write the objects.
	 * @param iterator the iterator containing the convertable objects to be
	 *        written.
	 * @return the number of written objects (number of called to the iterator's
	 *         next method).
	 */
	public static int write (File outputFile, Iterator iterator) {
		return write(outputFile, iterator, 1024*1024);
	}

	/**
	 * Writes a convertable object to the given file and returns the number of
	 * written objects. A buffer of <tt>1024*1024</tt> bytes is used for
	 * buffering the writing process.<br>
	 * This implementation is equivalent to the call of
	 * <tt>write(outputFile, new SingleObjectCursor(convertable), 1024*1024)</tt>.
	 *
	 * @param outputFile the file to which to write the object.
	 * @param convertable the convertable objects to be written.
	 * @return the number of written objects (usually <tt>1</tt>).
	 * @see SingleObjectCursor
	 */
	public static int write (File outputFile, Convertable convertable) {
		return write(outputFile, new SingleObjectCursor(convertable), 1024*1024);
	}

	/**
	 * Writes an iterator of convertable objects to the file with the given name
	 * and returns the number of written objects. A buffer of the specified size
	 * is used for buffering the writing process.<br>
	 * This implementation is equivalent to the call of
	 * <tt>write(new File(outputFile), iterator, bufferSize)</tt>.
	 *
	 * @param outputFile the name of the file to which to write the objects.
	 * @param iterator the iterator containing the convertable objects to be
	 *        written.
	 * @param bufferSize the size of the buffer used for buffering the writing
	 *        process.
	 * @return the number of written objects (number of called to the iterator's
	 *         next method).
	 */
	public static int write (String outputFile, Iterator iterator, int bufferSize) {
		return write(new File(outputFile), iterator, bufferSize);
	}

	/**
	 * Writes an iterator of convertable objects to the file with the given name
	 * and returns the number of written objects. A buffer of <tt>1024*1024</tt>
	 * bytes is used for buffering the writing process.<br>
	 * This implementation is equivalent to the call of
	 * <tt>write(outputFile, iterator, 1024*1024)</tt>.
	 *
	 * @param outputFile the name of the file to which to write the objects.
	 * @param iterator the iterator containing the convertable objects to be
	 *        written.
	 * @return the number of written objects (number of called to the iterator's
	 *         next method).
	 */
	public static int write(String outputFile, Iterator iterator){
		return write(outputFile, iterator, 1024*1024);
	}

	/**
	 * Writes a convertable object to the file with the given name and returns the
	 * number of written objects. A buffer of <tt>1024*1024</tt> bytes is used for
	 * buffering the writing process.<br>
	 * This implementation is equivalent to the call of
	 * <tt>write(outputFile, new SingleObjectCursor(convertable), 1024*1024)</tt>.
	 *
	 * @param outputFile the name of the file to which to write the object.
	 * @param convertable the convertable objects to be written.
	 * @return the number of written objects (usually <tt>1</tt>).
	 * @see SingleObjectCursor
	 */
	public static int write(String outputFile, Convertable convertable){
		return write(outputFile, new SingleObjectCursor(convertable), 1024*1024);
	}
}
