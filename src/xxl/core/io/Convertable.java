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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * The interface Convertable is a replacement for the SDK Externalizable
 * interface because of its drawbacks. The SDK serialization mechanism has
 * two serious drawbacks:
 * <ul>
 * <li>it causes overhead by writing additional data like the identity of
 *     class to the output stream.
 * <li>it does not accept raw data input streams because it expects the
 *     written additional data.<p>
 * </ul>
 *
 * The <tt>write</tt> and <tt>read</tt> methods of the Convertable
 * interface are implemented by a class to give the class complete control
 * over the format and contents of the stream for an object and its
 * supertypes. These methods must explicitly coordinate with the supertype
 * to save its state.
 *
 * @see DataInput
 * @see DataOutput
 * @see IOException
 */
public interface Convertable extends Serializable {

	/**
	 * Reads the state (the attributes) for an object of this class from
	 * the specified data input and restores the calling object. The state
	 * of the object before calling <tt>read</tt> will be lost.<br>
	 * The <tt>read</tt> method must read the values in the same sequence
	 * and with the same types as were written by <tt>write</tt>.
	 *
	 * @param dataInput the stream to read data from in order to restore
	 *        the object.
	 * @throws IOException if I/O errors occur.
	 */
	public abstract void read (DataInput dataInput) throws IOException;

	/**
	 * Writes the state (the attributes) of the calling object to the
	 * specified data output. This method should serialize the state of
	 * this object without calling another <tt>write</tt> method in order
	 * to prevent recursions.
	 *
	 * @param dataOutput the stream to write the state (the attributes) of
	 *        the object to.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public abstract void write (DataOutput dataOutput) throws IOException;
}
