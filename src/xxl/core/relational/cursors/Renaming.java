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

package xxl.core.relational.cursors;

import java.sql.ResultSet;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.relational.metaData.RenamedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.Arrays;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * Straight forward implementation of the operator renaming.
 * 
 * <p>In earlier versions of XXL it was possible to hand over a string array to
 * the constructor of the operators instead of an array of indices. To get this
 * functionality, use
 * {@link xxl.core.relational.resultSets.ResultSets#getColumnIndices(ResultSet, String[])}.</p>
 * 
 * <p>The example in the main method wraps an enumeration (integers 0 to 9) to
 * a metadata cursor using
 * {@link xxl.core.cursors.Cursors#wrapToMetaDataCursor(java.util.Iterator, Object)}.
 * Then, the column becomes renamed to "NewName" and the cursor is printed on
 * System.out. The interesting call is: 
 * <code><pre>
 *   cursor = new Projection(
 *       cursor,
 *       ArrayTuple.FACTORY_METHOD,
 *       new int[] {
 *           1
 *       },
 *       new String[] {
 *           "NewName"
 *       }
 *   );
 * </pre></code>
 */
public class Renaming extends SecureDecoratorCursor<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the renaming.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new instance of renaming. The specified columns of the of the
	 * tuples derived from the underlying metadata cursor are renamed using the
	 * given names.
	 *
	 * @param cursor the input metadata cursor delivering the input elements.
	 * @param columns an array of column numbers that become renamed.
	 * @param newNames an array holding the new column names.
	 */
	public Renaming(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, int[] columns, String[] newNames) {
		super(cursor);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(
			ResultSetMetaDatas.RESULTSET_METADATA_TYPE,
			new RenamedResultSetMetaData(
				ResultSetMetaDatas.getResultSetMetaData(cursor),
				columns,
				newNames
			)
		);
	}
	
	/**
	 * Creates a new instance of renaming. All columns of the of the tuples
	 * derived from the underlying metadata cursor are renamed using the
	 * given names.
	 *
	 * @param cursor the input metadata cursor delivering the input elements.
	 * @param newNames an array holding the new column names.
	 */
	public Renaming(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, String... newNames) {
		this(cursor, Arrays.enumeratedIntArray(1, newNames.length+1), newNames);
	}
	
	/**
	 * Creates a new instance of renaming. The specified columns of the of the
	 * tuples derived from the underlying result set are renamed using the
	 * given names.
	 *
	 * @param resultSet the input result set delivering the input elements.
	 * @param columns an array of column numbers that become renamed.
	 * @param newNames an array holding the new column names.
	 */
	public Renaming(ResultSet resultSet, int[] columns, String[] newNames) {
		this(new ResultSetMetaDataCursor(resultSet), columns, newNames);
	}
	
	/**
	 * Creates a new instance of renaming. All columns of the of the tuples
	 * derived from the underlying result set are renamed using the given
	 * names.
	 *
	 * @param resultSet the input result set delivering the input elements.
	 * @param newNames an array holding the new column names.
	 */
	public Renaming(ResultSet resultSet, String... newNames) {
		this(new ResultSetMetaDataCursor(resultSet), newNames);
	}
	
	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
