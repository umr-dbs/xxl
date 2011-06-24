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

package xxl.core.relational.metaData;

import java.sql.SQLException;
import java.util.Comparator;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;
import xxl.core.util.metaData.MetaDataProvider;
import xxl.core.util.metaData.MetaDataProviders;

/**
 * This class provides static methods for dealing with instances implementing
 * the interface {@link ColumnMetaData column metadata}. Beside this methods,
 * it contains constants for identifying columns storing data that is rather
 * important for query processing and optimization.
 * 
 * @see ColumnMetaData
 */
public class ColumnMetaDatas {
	
	/**
	 * This constant provides a comparator for metadata of a single column in a
	 * result set. The given column metadata is compared for the column names,
	 * the column types and a possible loss of precision.
	 */
	public static final Comparator<ColumnMetaData> COLUMN_METADATA_COMPARATOR = new Comparator<ColumnMetaData>() {
		public int compare(ColumnMetaData cmd1, ColumnMetaData cmd2) {
			try {
				int compare = cmd1.getColumnName().compareToIgnoreCase(cmd2.getColumnName());
				if (compare != 0)
					return compare;
				
				compare = ((Integer)cmd1.getColumnType()).compareTo(cmd2.getColumnType());
				if (compare != 0)
					return compare;
				
				compare = ((Integer)cmd1.getPrecision()).compareTo(cmd2.getPrecision());
				if (compare != 0)
					return compare;
				return 0;
			}
			catch (SQLException sqle) {
				throw new MetaDataException("column metadata information cannot be compared because of the following SQL exception : " + sqle.getMessage());
			}
		}
	};
	
	/**
	 * This constant provides a hash function for metadata of a single column
	 * in a result set. The hash-code of a given column metadata is defined as
	 * the sum of the hash-code of the column name, the column type and a the
	 * precision.
	 */
	public static final Function<ColumnMetaData, Integer> COLUMN_METADATA_HASH_FUNCTION = new AbstractFunction<ColumnMetaData, Integer>() {
		@Override
		public Integer invoke(ColumnMetaData cmd) {
			try {
				return cmd.getColumnName().hashCode() + cmd.getColumnType() + cmd.getPrecision();
			}
			catch (SQLException sqle) {
				throw new MetaDataException("column metadata information cannot be accessed because of the following SQL exception : " + sqle.getMessage());
			}
		}
	};
	
	/**
	 * This constant can be used to identify column metadata inside a composite
	 * metadata.
	 */
	public static final String COLUMN_METADATA_TYPE = "COLUMN_METADATA";
	
	/**
	 * Returns the metadata fragment of the given metadata provider's metadata
	 * representing its column metadata.
	 * 
	 * @param metaDataProvider the metadata provider containing the desired
	 *        column metadata fragment.
	 * @return the column metadata fragment of the given metadata provider's
	 *         metadata.
	 * @throws MetaDataException when the given metadata provider's metadata
	 *         does not contain any column metadata fragment.
	 */
	public static ColumnMetaData getColumnMetaData(MetaDataProvider<? extends CompositeMetaData<? super String, ? extends Object>> metaDataProvider) throws MetaDataException {
		return (ColumnMetaData)MetaDataProviders.getMetaDataFragment(metaDataProvider, COLUMN_METADATA_TYPE);
	}
	
	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private ColumnMetaDatas() {
		// private access in order to ensure non-instantiability
	}

}
