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

package xxl.core.util.metaData;

/**
 * A metadata exception is thrown to indicate that an application has attempted
 * to access a metadata object in an inappropriate way. For example, trying to
 * access a non-existent metadata fragment of a
 * {@link CompositeMetaData composite metadata} object will yield such an
 * exception.
 * 
 * @since 1.1
 */
public class MetaDataException extends RuntimeException {
	
	/**
	 * Constructs a metadata exception with the specified detail message.
	 * 
	 * @param message a detail message explaining what's the cause of this
	 *        exception.
	 */
	public MetaDataException(String message) {
		super(message);
	}
	
	/**
	 * Constructs a metadata exception with no detail message.
	 */
	public MetaDataException() {
		this("");
	}
	
}
