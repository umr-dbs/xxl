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
import java.io.Serializable;

/**
 * The class Converter is used for serializing objects that cannot implement
 * the <code>Convertable</code> interface (e.g., predefined classes of the
 * SDK). Like the Convertable interface it prevents the two drawbacks of the
 * SDK serialization mechanism:
 * <ul>
 * <li>it causes overhead by writing additional data like the identity of class
 *     to the output stream.</li>
 * <li>it does not accept raw data input streams because it expects the written
 *     additional data.</li>
 * </ul>
 * The <code>write</code> and <code>read</code> methods give an implementation
 * of <code>Converter</code> complete control over the format and contents of
 * the stream for an object and its supertypes. These methods must explicitly
 * coordinate with the supertype to save its state.
 *
 * @param <T> the type of the object that can be converted by using this
 *        converter.
 * @see java.io.DataInput
 * @see java.io.DataOutput
 * @see java.io.IOException
 */
public abstract class Converter<T> implements Serializable {

	/**
	 * Reads the state (the attributes) for the specified object from the
	 * specified data input and returns the restored object. The state of the
	 * specified object before the call of <code>read</code> will be lost. When
	 * <code>object&nbsp;==&nbsp;null</code> a new object should be created and
	 * restored.<br />
	 * The <code>read</code> method must read the values in the same sequence
	 * and with the same types as were written by <code>write</code>.
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *        object.
	 * @param object the object to be restored. When
	 *        <code>object&nbsp;==&nbsp;null</code> a new object should be
	 *        created and restored.
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	public abstract T read(DataInput dataInput, T object) throws IOException;

	/**
	 * Creates a new object by reading the state (the attributes) from the
	 * specified data input and returns the restored object. The
	 * <code>read</code> method must read the values in the same sequence and
	 * with the same types as were written by <code>write</code>.
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *        object.
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	public T read(DataInput dataInput) throws IOException {
		return read(dataInput, null);
	}

	/**
	 * Writes the state (the attributes) of the specified object to the
	 * specified data output. This method should serialize the state of this
	 * object without calling another <code>write</code> method in order to
	 * prevent recursions.
	 *
	 * @param dataOutput the stream to write the state (the attributes) of the
	 *        object to.
	 * @param object the object whose state (attributes) should be written to
	 *        the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public abstract void write(DataOutput dataOutput, T object) throws IOException;
}
