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

package xxl.core.cursors;

import xxl.core.util.metaData.MetaDataProvider;

/**
 * A metadata-cursor realizes a cursor additionally providing metadata. It
 * extends the interface {@link xxl.core.cursors.Cursor} and
 * {@link xxl.core.util.metaData.MetaDataProvider}. So the main difference between a
 * regular cursor and a metadata-cursor lies in the provision of the
 * <code>getMetaData</code> method, which returns metadata information about
 * the elements this metadata-cursor delivers. The return value of the
 * <code>getMetaData</code> method can be an arbitrary kind of metadata
 * information, e.g., relational metadata information, therefore it is defined
 * as a generic type.
 * 
 * <p>When using a metadata-cursor, it has to be guaranteed, that all elements
 * contained in this metadata-cursor refer to the same metadata information.
 * That means, every time <code>getMetaData</code> is called on a
 * metadata-cursor, it should return the same metadata information. Generally
 * this method is called only once.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @param <M> the type of the given meta data object.
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.util.metaData.MetaDataProvider
 */
public interface MetaDataCursor<E, M> extends Cursor<E>, MetaDataProvider<M> {

	/**
	 * Returns the metadata information for this metadata-cursor. The return
	 * value of this method can be an arbitrary kind of metadata information,
	 * e.g., relational metadata information, therefore it is defined as a
	 * generic type. When using a metadata-cursor, it has to be guaranteed,
	 * that all elements contained in this metadata-cursor refer to the same
	 * metadata information. That means, every time <code>getMetaData</code>
	 * is called on a metadata-cursor, it should return exactly the same
	 * metadata information.
	 *
	 * @return an object representing metadata information for this
	 *         metadata-cursor.
	 */
	public abstract M getMetaData();

}
