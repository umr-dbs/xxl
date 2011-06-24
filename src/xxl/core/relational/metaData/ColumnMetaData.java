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

/**
 * This class provides relational metadata for a column in a result set.
 */
public interface ColumnMetaData {

	/**
	 * Indicates whether this column is automatically numbered, thus read-only.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isAutoIncrement() throws SQLException;

	/**
	 * Indicates whether this column's case matters.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isCaseSensitive() throws SQLException;

	/**
	 * Indicates whether this column can be used in a where clause.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isSearchable() throws SQLException;

	/**
	 * Indicates whether this column is a cash value.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isCurrency() throws SQLException;

	/**
	 * Indicates the nullability of values in this column.		
	 *
	 * @return the nullability status of the given column; one of
	 *         <code>ResultSetMetaData.columnNoNulls</code>,
	 *         <code>ResultSetMetaData.columnNullable</code> or
	 *         <code>ResultSetMetaData.columnNullableUnknown</code>.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int isNullable() throws SQLException;

	/**
	 * Indicates whether values in this column are signed numbers.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isSigned() throws SQLException;

	/**
	 * Indicates this column's normal maximum width in characters.
	 *
	 * @return the normal maximum number of characters allowed as the width of
	 *         the designated column.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int getColumnDisplaySize() throws SQLException;

	/**
	 * Gets this column's suggested title for use in printouts and displays.
	 *
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getColumnLabel() throws SQLException;

	/**
	 * Get this column's name.
	 *
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getColumnName() throws SQLException;

	/**
	 * Get this column's table's schema.
	 *
	 * @return schema name or "" if not applicable
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getSchemaName() throws SQLException;

	/**
	 * Get this column's number of decimal digits.
	 *
	 * @return precision.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int getPrecision() throws SQLException;

	/**
	 * Gets this column's number of digits to right of the decimal point.
	 *
	 * @return scale.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int getScale() throws SQLException;

	/**
	 * Gets this column's table name. 
	 *
	 * @return table name or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getTableName() throws SQLException;

	/**
	 * Gets this column's table's catalog name.
	 *
	 * @return the name of the catalog for the table in which the given column
	 *         appears or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getCatalogName() throws SQLException;

	/**
	 * Retrieves this column's SQL type.
	 *
	 * @return SQL type from java.sql.Types.
	 * @throws SQLException if a database access error occurs.
	 * @see java.sql.Types
	 */
	public abstract int getColumnType() throws SQLException;

	/**
	 * Retrieves this column's database-specific type name.
	 *
	 * @return type name used by the database. If the column type is a
	 *         user-defined type, then a fully-qualified type name is returned.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract String getColumnTypeName() throws SQLException;

	/**
	 * Indicates whether this column is definitely not writable.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isReadOnly() throws SQLException;

	/**
	 * Indicates whether it is possible for a write on this column to succeed.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isWritable() throws SQLException;

	/**
	 * Indicates whether a write on this column will definitely succeed.	
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract boolean isDefinitelyWritable() throws SQLException;

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
	public abstract String getColumnClassName() throws SQLException;

}
