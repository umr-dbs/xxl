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

import xxl.core.relational.Types;

/**
 * This class provides relational metadata for a column in a result set. The
 * data is internally stored in fields with the names suggested by the getter
 * methods of the {@link ResultSetMetaData} interface.
 */
public class StoredColumnMetaData implements ColumnMetaData {
	
	/**
	 * Indicates whether this column is automatically numbered, thus read-only.
	 */
	protected boolean autoIncrement;

	/**
	 * Indicates whether this column's case matters.
	 */
	protected boolean caseSensitive;

	/**
	 * Indicates whether this column can be used in a where clause.
	 */
	protected boolean searchable;

	/**
	 * Indicates whether this column is a cash value.
	 */
	protected boolean currency;

	/**
	 * Indicates the nullability of values in this column; one of
	 * <code>ResultSetMetaData.columnNoNulls</code>,
	 * <code>ResultSetMetaData.columnNullable</code> or
	 * <code>ResultSetMetaData.columnNullableUnknown</code>.
	 */
	protected int nullable;

	/**
	 * Indicates whether values in this column are signed numbers.
	 */
	protected boolean signed;

	/**
	 * Indicates this column's normal maximum width in characters.
	 */
	protected int columnDisplaySize;

	/**
	 * This column's suggested title for use in printouts and displays.
	 */
	protected String columnLabel;

	/**
	 * This column's name.
	 */
	protected String columnName;

	/**
	 * This column's table's schema.
	 */
	protected String schemaName;

	/**
	 * This column's number of decimal digits.
	 */
	protected int precision;

	/**
	 * This column's number of digits to right of the decimal point.
	 */
	protected int scale;

	/**
	 * This column's table name. 
	 */
	protected String tableName;

	/**
	 * This column's table's catalog name.
	 */
	protected String catalogName;

	/**
	 * This column's SQL type.
	 */
	protected int columnType;

	/**
	 * This column's database-specific type name.
	 */
	protected String columnTypeName;

	/**
	 * Indicates whether this column is definitely not writable.
	 */
	protected boolean readOnly;

	/**
	 * Indicates whether it is possible for a write on this column to succeed.
	 */
	protected boolean writable;
	
	/**
	 * Indicates whether a write on this column will definitely succeed.
	 */
	protected boolean definitelyWritable;

	/**
	 * The fully-qualified name of the Java class whose instances are
	 * manufactured if the method <code>ResultSet.getObject</code> is called to
	 * retrieve a value from the column described by this metadata.
	 * <code>ResultSet.getObject</code> may return a subclass of the class
	 * returned by this method.
	 */
	protected String columnClassName;
	
	/**
	 * Creates a new stored column metadata that sets its fields to the given
	 * parameters.
	 * 
	 * @param autoIncrement indicates whether this column is automatically
	 *        numbered, thus read-only.
	 * @param caseSensitive indicates whether this column's case matters.
	 * @param searchable indicates whether this column can be used in a where
	 *        clause.
	 * @param currency indicates whether this column is a cash value.
	 * @param nullable indicates the nullability of values in this column; one
	 *        of <code>ResultSetMetaData.columnNoNulls</code>,
	 *        <code>ResultSetMetaData.columnNullable</code> or
	 *        <code>ResultSetMetaData.columnNullableUnknown</code>.
	 * @param signed indicates whether values in this column are signed
	 *        numbers.
	 * @param columnDisplaySize indicates this column's normal maximum width in
	 *        characters.
	 * @param columnLabel this column's suggested title for use in printouts
	 *        and displays.
	 * @param columnName this column's name.
	 * @param schemaName this column's table's schema.
	 * @param precision this column's number of decimal digits.
	 * @param scale this column's number of digits to right of the decimal
	 *        point.
	 * @param tableName this column's table name. 
	 * @param catalogName this column's table's catalog name.
	 * @param columnType this column's SQL type.
	 * @param columnTypeName this column's database-specific type name.
	 * @param readOnly indicates whether this column is definitely not
	 *        writable.
	 * @param writable indicates whether it is possible for a write on this
	 *        column to succeed.
	 * @param definitelyWritable indicates whether a write on this column will
	 *        definitely succeed.
	 * @param columnClassName the fully-qualified name of the Java class whose
	 *        instances are manufactured if the method
	 *        <code>ResultSet.getObject</code> is called to retrieve a value
	 *        from the column described by this metadata.
	 *        <code>ResultSet.getObject</code> may return a subclass of the
	 *        class returned by this method.
	 */
	public StoredColumnMetaData(boolean autoIncrement, boolean caseSensitive, boolean searchable, boolean currency, int nullable, boolean signed, int columnDisplaySize, String columnLabel, String columnName, String schemaName, int precision, int scale, String tableName, String catalogName, int columnType, String columnTypeName, boolean readOnly, boolean writable, boolean definitelyWritable, String columnClassName) {
		this.autoIncrement = autoIncrement;
		this.caseSensitive = caseSensitive;
		this.searchable = searchable;
		this.currency = currency;
		this.nullable = nullable;
		this.signed = signed;
		this.columnDisplaySize = columnDisplaySize;
		this.columnLabel = columnLabel;
		this.columnName = columnName;
		this.schemaName = schemaName;
		this.precision = precision;
		this.scale = scale;
		this.tableName = tableName;
		this.catalogName = catalogName;
		this.columnType = columnType;
		this.columnTypeName = columnTypeName;
		this.readOnly = readOnly;
		this.writable = writable;
		this.definitelyWritable = definitelyWritable;
		this.columnClassName = columnClassName;
	}
	
	/**
	 * Creates a new stored column metadata that sets its fields to the given
	 * parameters. The type name and the class name of the column is resolved
	 * via the class {@link xxl.core.relational.Types}.
	 * 
	 * @param autoIncrement indicates whether this column is automatically
	 *        numbered, thus read-only.
	 * @param caseSensitive indicates whether this column's case matters.
	 * @param searchable indicates whether this column can be used in a where
	 *        clause.
	 * @param currency indicates whether this column is a cash value.
	 * @param nullable indicates the nullability of values in this column; one
	 *        of <code>ResultSetMetaData.columnNoNulls</code>,
	 *        <code>ResultSetMetaData.columnNullable</code> or
	 *        <code>ResultSetMetaData.columnNullableUnknown</code>.
	 * @param signed indicates whether values in this column are signed
	 *        numbers.
	 * @param columnDisplaySize indicates this column's normal maximum width in
	 *        characters.
	 * @param columnLabel this column's suggested title for use in printouts
	 *        and displays.
	 * @param columnName this column's name.
	 * @param schemaName this column's table's schema.
	 * @param precision this column's number of decimal digits.
	 * @param scale this column's number of digits to right of the decimal
	 *        point.
	 * @param tableName this column's table name. 
	 * @param catalogName this column's table's catalog name.
	 * @param columnType this column's SQL type.
	 * @param readOnly indicates whether this column is definitely not
	 *        writable.
	 * @param writable indicates whether it is possible for a write on this
	 *        column to succeed.
	 * @param definitelyWritable indicates whether a write on this column will
	 *        definitely succeed.
	 */
	public StoredColumnMetaData(boolean autoIncrement, boolean caseSensitive, boolean searchable, boolean currency, int nullable, boolean signed, int columnDisplaySize, String columnLabel, String columnName, String schemaName, int precision, int scale, String tableName, String catalogName, int columnType, boolean readOnly, boolean writable, boolean definitelyWritable) {
		this(
			autoIncrement,
			caseSensitive,
			searchable,
			currency,
			nullable,
			signed,
			columnDisplaySize,
			columnLabel,
			columnName,
			schemaName,
			precision,
			scale,
			tableName,
			catalogName,
			columnType,
			Types.getSqlTypeName(columnType),
			readOnly,
			writable,
			definitelyWritable,
			Types.getJavaTypeName(Types.getJavaType(columnType))
		);
	}
	
	/**
	 * Indicates whether this column is automatically numbered, thus read-only.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isAutoIncrement() throws SQLException {
		return autoIncrement;
	}

	/**
	 * Indicates whether this column's case matters.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCaseSensitive() throws SQLException {
		return caseSensitive;
	}

	/**
	 * Indicates whether this column can be used in a where clause.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSearchable() throws SQLException {
		return searchable;
	}

	/**
	 * Indicates whether this column is a cash value.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCurrency() throws SQLException {
		return currency;
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
		return nullable;
	}

	/**
	 * Indicates whether values in this column are signed numbers.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSigned() throws SQLException {
		return signed;
	}

	/**
	 * Indicates this column's normal maximum width in characters.
	 *
	 * @return the normal maximum number of characters allowed as the width
	 *         of the designated column.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getColumnDisplaySize() throws SQLException {
		return columnDisplaySize;
	}

	/**
	 * Gets this column's suggested title for use in printouts and displays.
	 *
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnLabel() throws SQLException {
		return columnLabel;
	}

	/**
	 * Get this column's name.
	 *
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnName() throws SQLException {
		return columnName;
	}

	/**
	 * Get this column's table's schema.
	 *
	 * @return schema name or "" if not applicable
	 * @throws SQLException if a database access error occurs.
	 */
	public String getSchemaName() throws SQLException {
		return schemaName;
	}

	/**
	 * Get this column's number of decimal digits.
	 *
	 * @return precision.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getPrecision() throws SQLException {
		return precision;
	}

	/**
	 * Gets this column's number of digits to right of the decimal point.
	 *
	 * @return scale.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getScale() throws SQLException {
		return scale;
	}

	/**
	 * Gets this column's table name. 
	 *
	 * @return table name or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getTableName() throws SQLException {
		return tableName;
	}

	/**
	 * Gets this column's table's catalog name.
	 *
	 * @return the name of the catalog for the table in which the given
	 *         column appears or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getCatalogName() throws SQLException {
		return catalogName;
	}

	/**
	 * Retrieves this column's SQL type.
	 *
	 * @return SQL type from java.sql.Types.
	 * @throws SQLException if a database access error occurs.
	 * @see java.sql.Types
	 */
	public int getColumnType() throws SQLException {
		return columnType;
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
		return columnTypeName;
	}

	/**
	 * Indicates whether this column is definitely not writable.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isReadOnly() throws SQLException {
		return readOnly;
	}

	/**
	 * Indicates whether it is possible for a write on this column to succeed.
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isWritable() throws SQLException {
		return writable;
	}

	/**
	 * Indicates whether a write on this column will definitely succeed.	
	 *
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isDefinitelyWritable() throws SQLException {
		return definitelyWritable;
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
		return columnClassName;
	}
	
	/**
	 * Returns a string representation of this column metadata.
	 * 
	 * @return a string representation of this column metadata.
	 */
	@Override
	public String toString() {
		return '[' + columnName + ", " + columnLabel + ", " + schemaName + ", " + tableName + ", " + catalogName + ", " + columnType + ", " + columnTypeName + ", " + columnClassName + ", " + columnDisplaySize + ", " + autoIncrement + ", " + caseSensitive + ", " + searchable + ", " + currency + ", " + nullable + ", " + signed + ", " + precision + ", " + scale + ", " + readOnly + ", " + writable + ", " + definitelyWritable + ']';
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
