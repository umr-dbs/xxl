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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import xxl.core.util.Arrays;
import xxl.core.util.metaData.MetaDataException;

/**
 * Makes a renaming of a result set metadata object's columns.
 */
public class RenamedResultSetMetaData extends WrappedResultSetMetaData {
	
	/**
	 * The new name of the table represented by the renamed result set
	 * metadata.
	 */
	protected String renaming;
	
	/**
	 * A hash map containing key/value pairs consisting of the index of the
	 * renamed column and the new name of the column.
	 */
	protected HashMap<Integer, String> renamings;
	
	/**
	 * Creates a new renamed result set metadata that renames the table name as
	 * the given new table name and the columns identified by the specified
	 * indices as the given names.
	 * 
	 * @param metaData the result set metadata to be renamed.
	 * @param newTableName the name name of the table in the renamed result set
	 *        metadata.
	 * @param columns the indices of the columns which will be renamed in the
	 *        renamed result set metadata.
	 * @param newNames the new names of the specified columns in the renamed
	 *        result set metadata.
	 */
	public RenamedResultSetMetaData(ResultSetMetaData metaData, String newTableName, int[] columns, String... newNames) {
		super(metaData);
		renaming = newTableName;
		try {
			if (columns.length != newNames.length)
				throw new MetaDataException("the number of specified indices and column names does not match");
			int columnCount = metaData.getColumnCount();
			HashSet<String> hashSet = new HashSet<String>(columns.length);
			renamings = new HashMap<Integer, String>(columns.length);
			for (int i = 0; i < columns.length; i++) {
				if (columns[i] < 1 || columnCount < columns[i])
					throw new MetaDataException("the specified column " + columns[i] + " cannot be renamed because it does not exist in the underlying meta data");
				if (hashSet.contains(newNames[i]))
					throw new MetaDataException("renaming cannot be applied because the name " + newNames[i] + " is specified twice");
				hashSet.add(newNames[i]);
				renamings.put(columns[i], newNames[i]);
			}
			for (int i = 1; i <= columnCount; i++)
				if (!renamings.containsKey(i) && hashSet.contains(getColumnName(i)))
					throw new MetaDataException("renaming cannot be applied because the name " + newNames[i] + " is already exists in the underlying meta data");
		}
		catch (SQLException sqle) {
			throw new MetaDataException("meta data cannot be constructed due to the following sql exception: " + sqle.getMessage());
		}
	}

	/**
	 * Creates a new renamed result set metadata that renames the columns
	 * identified by the specified indices as the given names.
	 * 
	 * @param metaData the result set metadata to be renamed.
	 * @param columns the indices of the columns which will be renamed in the
	 *        renamed result set metadata.
	 * @param newNames the new names of the specified columns in the renamed
	 *        result set metadata.
	 */
	public RenamedResultSetMetaData(ResultSetMetaData metaData, int[] columns, String... newNames) {
		this(metaData, null, columns, newNames);
	}

	/**
	 * Creates a new renamed result set metadata that renames the table name as
	 * the given new table name and the first <code>newNames.length</code>
	 * columns of the given result set metadata as the given names.
	 * 
	 * @param metaData the result set metadata to be renamed.
	 * @param newTableName the name name of the table in the renamed result set
	 *        metadata.
	 * @param newNames the new names of the columns in the renamed result set
	 *        metadata.
	 */
	public RenamedResultSetMetaData(ResultSetMetaData metaData, String newTableName, String... newNames) {
		this(metaData, newTableName, Arrays.enumeratedIntArray(1, newNames.length+1), newNames);
	}
	
	/**
	 * Creates a new renamed result set metadata that renames the first
	 * <code>newNames.length</code> columns of the given result set metadata as
	 * the given names.
	 * 
	 * @param metaData the result set metadata to be renamed.
	 * @param newNames the new names of the columns in the renamed result set
	 *        metadata.
	 */
	public RenamedResultSetMetaData(ResultSetMetaData metaData, String[] newNames) {
		this(metaData, (String)null, newNames);
	}
	
	/**
	 * Gets the designated column's table name.
	 * 
	 * @param column the first column is 1, the second is 2, ...
	 * @return table name or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public String getTableName(int column) throws SQLException {
		return renaming != null ?
			renaming :
			metaData.getTableName(column);
	}
	
	/**
	 * Get the designated column's name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public String getColumnName(int column) throws SQLException {
		return renamings.containsKey(column = originalColumnIndex(column)) ?
			renamings.get(column) :
			metaData.getColumnName(column);
	}

	/**
	 * Gets the designated column's suggested title for use in printouts and
	 * displays.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}
	
}
