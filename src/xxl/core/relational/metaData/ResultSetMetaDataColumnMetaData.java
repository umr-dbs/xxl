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

import xxl.core.util.metaData.MetaDataException;

/**
 * This class provides relational metadata for a column in a result set by
 * wrapping the result sets metadata. The getter methods of the column metadata
 * are redirected to a specified column of the wrapped result set metadata.
 */
public class ResultSetMetaDataColumnMetaData implements ColumnMetaData {
	
	/**
	 * The metadata of the result set containing the column. Calls to the
	 * column metadata's getter methods are redirected to the result set
	 * metadata using the specified column index.
	 */
	protected ResultSetMetaData resultSetMetaData;
	
	/**
	 * The index of the column's metadata inside the wrapped result set's
	 * metadata.
	 */
	protected int columnIndex;
	
	/**
	 * Creates a new column metadata representing the
	 * <code>columnIndex</code>-th column of the given result set metadata.
	 * 
	 * @param resultSetMetaData the result set metadata containing this column
	 *        metadata.
	 * @param columnIndex the index of this column metadata inside the given
	 *        result set metadata.
	 */
	public ResultSetMetaDataColumnMetaData(ResultSetMetaData resultSetMetaData, int columnIndex) {
		this.resultSetMetaData = resultSetMetaData;
		this.columnIndex = columnIndex;
	}
	
	/**
	 * Creates a new column metadata reprsenting the column of the given result
	 * set metadata with the specified name.
	 * 
	 * @param resultSetMetaData the result set metadata containing this column
	 *        metadata.
	 * @param columnName the name of this column metadata inside the given
	 *        result set metadata.
	 * @throws SQLException if a database access error occurs.
	 */
	public ResultSetMetaDataColumnMetaData(ResultSetMetaData resultSetMetaData, String columnName) throws SQLException {
		this.resultSetMetaData = resultSetMetaData;
		
		for (int columnIndex = 1; columnIndex <= resultSetMetaData.getColumnCount(); columnIndex++)
			if (resultSetMetaData.getColumnName(columnIndex).equalsIgnoreCase(columnName)) {
				this.columnIndex = columnIndex;
				return;
			}
		
		throw new SQLException("the given result set metadata does not contain a column with the specified name \"" + columnName + "\"");
	}
	
	/**
	 * Indicates whether this column is automatically numbered, thus read-only.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isAutoIncrement() throws SQLException {
		return resultSetMetaData.isAutoIncrement(columnIndex);
	}

	/**
	 * Indicates whether this column's case matters.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCaseSensitive() throws SQLException {
		return resultSetMetaData.isCaseSensitive(columnIndex);
	}

	/**
	 * Indicates whether this column can be used in a where clause.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSearchable() throws SQLException {
		return resultSetMetaData.isSearchable(columnIndex);
	}

	/**
	 * Indicates whether this column is a cash value.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCurrency() throws SQLException {
		return resultSetMetaData.isCurrency(columnIndex);
	}

	/**
	 * Indicates the nullability of values in this column.		
	 *
	 * @return the nullability status of the given column; one of
	 *         <code>ResultSetMetaData.columnNoNulls</code>,
	 *         <code>ResultSetMetaData.columnNullable</code> or
	 *         <code>ResultSetMetaData.columnNullableUnknown</code>.
	 * @throws SQLException if a database access error occurs.
	 */
	public int isNullable() throws SQLException {
		return resultSetMetaData.isNullable(columnIndex);
	}

	/**
	 * Indicates whether values in this column are signed numbers.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSigned() throws SQLException {
		return resultSetMetaData.isSigned(columnIndex);
	}

	/**
	 * Indicates this column's normal maximum width in characters.
	 *
	 * @return the normal maximum number of characters allowed as the width
	 *         of the designated column.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getColumnDisplaySize() throws SQLException {
		return resultSetMetaData.getColumnDisplaySize(columnIndex);
	}

	/**
	 * Gets this column's suggested title for use in printouts and displays.
	 *
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnLabel() throws SQLException {
		return resultSetMetaData.getColumnLabel(columnIndex);
	}

	/**
	 * Get this column's name.
	 *
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnName() throws SQLException {
		return resultSetMetaData.getColumnName(columnIndex);
	}

	/**
	 * Get this column's table's schema.
	 *
	 * @return schema name or "" if not applicable
	 * @throws SQLException if a database access error occurs.
	 */
	public String getSchemaName() throws SQLException {
		return resultSetMetaData.getSchemaName(columnIndex);
	}

	/**
	 * Get this column's number of decimal digits.
	 *
	 * @return precision.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getPrecision() throws SQLException {
		return resultSetMetaData.getPrecision(columnIndex);
	}

	/**
	 * Gets this column's number of digits to right of the decimal point.
	 *
	 * @return scale.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getScale() throws SQLException {
		return resultSetMetaData.getScale(columnIndex);
	}

	/**
	 * Gets this column's table name. 
	 *
	 * @return table name or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getTableName() throws SQLException {
		return resultSetMetaData.getTableName(columnIndex);
	}

	/**
	 * Gets this column's table's catalog name.
	 *
	 * @return the name of the catalog for the table in which the given
	 *         column appears or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getCatalogName() throws SQLException {
		return resultSetMetaData.getCatalogName(columnIndex);
	}

	/**
	 * Retrieves this column's SQL type.
	 *
	 * @return SQL type from java.sql.Types.
	 * @throws SQLException if a database access error occurs.
	 * @see java.sql.Types
	 */
	public int getColumnType() throws SQLException {
		return resultSetMetaData.getColumnType(columnIndex);
	}

	/**
	 * Retrieves this column's database-specific type name.
	 *
	 * @return type name used by the database. If the column type is a
	 *         user-defined type, then a fully-qualified type name is
	 *         returned.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnTypeName() throws SQLException {
		return resultSetMetaData.getColumnTypeName(columnIndex);
	}

	/**
	 * Indicates whether this column is definitely not writable.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isReadOnly() throws SQLException {
		return resultSetMetaData.isReadOnly(columnIndex);
	}

	/**
	 * Indicates whether it is possible for a write on this column to succeed.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isWritable() throws SQLException {
		return resultSetMetaData.isWritable(columnIndex);
	}

	/**
	 * Indicates whether a write on this column will definitely succeed.	
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isDefinitelyWritable() throws SQLException {
		return resultSetMetaData.isDefinitelyWritable(columnIndex);
	}

	/**
	 * Returns the fully-qualified name of the Java class whose instances are
	 * manufactured if the method <code>ResultSet.getObject</code> is called to
	 * retrieve a value from the column described by this metadata.
	 * <code>ResultSet.getObject</code> may return a subclass of the class
	 * returned by this method.
	 *
	 * @return the fully-qualified name of the class in the Java programming
	 *         language that would be used by the method
	 *         <code>ResultSet.getObject</code> to retrieve the value in the
	 *         specified column. This is the class name used for custom
	 *         mapping.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnClassName() throws SQLException {
		return resultSetMetaData.getColumnClassName(columnIndex);
	}
	
	/**
	 * Returns a string representation of this column metadata.
	 * 
	 * @return a string representation of this column metadata.
	 */
	@Override
	public String toString() {
		try {
			return '[' + getColumnName() + ", " + getColumnLabel() + ", " + getSchemaName() + ", " + getTableName() + ", " + getCatalogName() + ", " + getColumnType() + ", " + getColumnTypeName() + ", " + getColumnClassName() + ", " + getColumnDisplaySize() + ", " + isAutoIncrement() + ", " + isCaseSensitive() + ", " + isSearchable() + ", " + isCurrency() + ", " + isNullable() + ", " + isSigned() + ", " + getPrecision() + ", " + getScale() + ", " + isReadOnly() + ", " + isWritable() + ", " + isDefinitelyWritable() + ']';
		}
		catch (SQLException sqle) {
			throw new MetaDataException("sql exception occured during string construction: \'" + sqle.getMessage() + "\'");
		}
	}
	
	/**
	 * Indicates whether some other object is "equal to" this column metadata.
	 * 
	 * <p>The <code>equals</code> method implements an equivalence relation: 
	 * <ul>
	 *     <li>
	 *         It is <i>reflexive</i>: for any reference value <code>x</code>,
	 *         <code>x.equals(x)</code> should return <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>symmetric</i>: for any reference values <code>x</code>
	 *         and <code>y</code>, <code>x.equals(y)</code> should return
	 *         <code>true</code> if and only if <code>y.equals(x)</code>
	 *         returns <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>transitive</i>: for any reference values
	 *         <code>x</code>, <code>y</code>, and <code>z</code>, if
	 *         <code>x.equals(y)</code> returns <code>true</code> and
	 *         <code>y.equals(z)</code> returns <code>true</code>, then
	 *         <code>x.equals(z)</code> should return <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>consistent</i>: for any reference values <code>x</code>
	 *         and <code>y</code>, multiple invocations of
	 *         <code>x.equals(y)</code> consistently return <code>true</code>
	 *         or consistently return <code>false</code>, provided no
	 *         information used in <code>equals</code> comparisons on the
	 *         object is modified.
	 *     </li>
	 *     <li>
	 *         For any non-<code>null</code> reference value <code>x</code>,
	 *         <code>x.equals(null)</code> should return <code>false</code>.
	 *     </li>
	 * </ul></p>
	 * 
	 * <p>The current <code>equals</code> method returns true if and only if
	 * the given object:
	 * <ul>
	 *     <li>
	 *         is this column metadata or
	 *     </li>
	 *     <li>
	 *         is an instance of the type <code>ColumnMetaData</code> and a
	 *         {@link ColumnMetaDatas#COLUMN_METADATA_COMPARATOR comparator}
	 *         for column metadata returns 0.
	 *     </li>
	 * </ul></p>
	 * 
	 * @param object the reference object with which to compare.
	 * @return <code>true</code> if this object is the same as the specified
	 *         object; <code>false</code> otherwise.
	 * @see #hashCode()
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null)
			return false;
		if (this == object)
			return true;
		return object instanceof ColumnMetaData && ColumnMetaDatas.COLUMN_METADATA_COMPARATOR.compare(this, (ColumnMetaData)object) == 0;
	}
	
	/**
	 * Returns the hash code value for this column metadata using a
	 * {@link ColumnMetaDatas#COLUMN_METADATA_HASH_FUNCTION hash function}
	 * for column metadata.
	 *
	 * @return the hash code value for this column metadata.
	 * @see Object#hashCode()
	 * @see #equals(Object)
	 */
	@Override
	public int hashCode() {
		return ColumnMetaDatas.COLUMN_METADATA_HASH_FUNCTION.invoke(this);
	}
	
}
