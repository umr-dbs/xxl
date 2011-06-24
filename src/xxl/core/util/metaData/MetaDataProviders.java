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
 * The class <code>MetaDataProviders</code> contains various <tt>static</tt>
 * methods for accessing the metadata of a metadata provider.
 *
 * @see xxl.core.util.metaData.MetaDataProvider
 */
public class MetaDataProviders {

	/**
	 * Returns the metadata fragment of the given metadata provider's metadata
	 * according to the specified identifier.
	 * 
	 * @param <I> the type of the identifiers.
	 * @param <M> the type of the metadata fragments.
	 * @param metaDataProvider the metadata provider containing the desired
	 *        metadata fragment.
	 * @param identifier the identifier with which the desired metadata fragment
	 *        is associated. 
	 * @return the metadata fragment of the given metadata provider's metadata
	 *         according to the specified identifier.
	 */
	public static <I, M> M getMetaDataFragment(MetaDataProvider<? extends CompositeMetaData<? super I, ? extends M>> metaDataProvider, I identifier) {
		return metaDataProvider.getMetaData().get(identifier);
	}
	
	/**
	 * The default constructor has private access in order to
	 * ensure non-instantiability.
	 */
	private MetaDataProviders() {
		// private access in order to ensure non-instantiability
	}

}
