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
 * This class provides relational metadata as specified by the
 * {@link ResultSetMetaData} interface. The metadata characterizing the columns
 * of the result set described by this metadata is given by instances of the
 * class {@link ColumnMetaData}. Thus an array of this type fulfills the task
 * of storing the whole result set's metadata.
 */
public class ColumnMetaDataResultSetMetaData implements ResultSetMetaData {

	/**
	 * The array <code>columnMetaData</code> holds the metadata of the columns
	 * of the result set described by this metadata.
	 */
	protected ColumnMetaData[] columnMetaData;
	
	/**
	 * Creates a new relational metadata for a result set with columns
	 * described in the specified array.
	 * 
	 * @param columnMetaData an array holding the stored metadata of the
	 *        columns of the result set described by the metadata to be
	 *        created.
	 */
	public ColumnMetaDataResultSetMetaData(ColumnMetaData... columnMetaData) {
		this.columnMetaData = columnMetaData;
	}
	
	/**
	 * Returns the number of columns in this <code>ResultSet</code> object.
	 *
	 * @return the number of columns.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getColumnCount() throws SQLException {
		return columnMetaData.length;
	}
	
	/**
	 * Indicates whether the designated column is automatically numbered, thus
	 * read-only.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isAutoIncrement(int column) throws SQLException {
		return columnMetaData[column-1].isAutoIncrement();
	}
	
	/**
	 * Indicates whether a column's case matters.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCaseSensitive(int column) throws SQLException {
		return columnMetaData[column-1].isCaseSensitive();
	}
	
	/**
	 * Indicates whether the designated column can be used in a where clause.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSearchable(int column) throws SQLException {
		return columnMetaData[column-1].isSearchable();
	}
	
	/**
	 * Indicates whether the designated column is a cash value.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isCurrency(int column) throws SQLException {
		return columnMetaData[column-1].isCurrency();
	}
	
	/**
	 * Indicates the nullability of values in the designated column.		
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the nullability status of the given column; one of
	 *         <code>ResultSetMetaData.columnNoNulls</code>,
	 *         <code>ResultSetMetaData.columnNullable</code> or
	 *         <code>ResultSetMetaData.columnNullableUnknown</code>.
	 * @throws SQLException if a database access error occurs.
	 */
	public int isNullable(int column) throws SQLException {
		return columnMetaData[column-1].isNullable();
	}
	
	/**
	 * Indicates whether values in the designated column are signed numbers.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isSigned(int column) throws SQLException {
		return columnMetaData[column-1].isSigned();
	}
	
	/**
	 * Indicates the designated column's normal maximum width in characters.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the normal maximum number of characters allowed as the width of
	 *         the designated column.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getColumnDisplaySize(int column) throws SQLException {
		return columnMetaData[column-1].getColumnDisplaySize();
	}
	
	/**
	 * Gets the designated column's suggested title for use in printouts and
	 * displays.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnLabel(int column) throws SQLException {
		return columnMetaData[column-1].getColumnLabel();
	}
	
	/**
	 * Get the designated column's name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnName(int column) throws SQLException {
		return columnMetaData[column-1].getColumnName();
	}
	
	/**
	 * Get the designated column's table's schema.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return schema name or "" if not applicable
	 * @throws SQLException if a database access error occurs
	 */
	public String getSchemaName(int column) throws SQLException {
		return columnMetaData[column-1].getSchemaName();
	}
	
	/**
	 * Get the designated column's number of decimal digits.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return precision.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getPrecision(int column) throws SQLException {
		return columnMetaData[column-1].getPrecision();
	}
	
	/**
	 * Gets the designated column's number of digits to right of the decimal
	 * point.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return scale.
	 * @throws SQLException if a database access error occurs.
	 */
	public int getScale(int column) throws SQLException {
		return columnMetaData[column-1].getScale();
	}
	
	/**
	 * Gets the designated column's table name. 
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return table name or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getTableName(int column) throws SQLException {
		return columnMetaData[column-1].getTableName();
	}
	
	/**
	 * Gets the designated column's table's catalog name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the name of the catalog for the table in which the given column
	 *         appears or "" if not applicable.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getCatalogName(int column) throws SQLException {
		return columnMetaData[column-1].getCatalogName();
	}
	
	/**
	 * Retrieves the designated column's SQL type.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return SQL type from java.sql.Types.
	 * @throws SQLException if a database access error occurs.
	 * @see java.sql.Types
	 */
	public int getColumnType(int column) throws SQLException {
		return columnMetaData[column-1].getColumnType();
	}
	
	/**
	 * Retrieves the designated column's database-specific type name.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return type name used by the database. If the column type is a
	 *         user-defined type, then a fully-qualified type name is returned.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnTypeName(int column) throws SQLException {
		return columnMetaData[column-1].getColumnTypeName();
	}
	
	/**
	 * Indicates whether the designated column is definitely not writable.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isReadOnly(int column) throws SQLException {
		return columnMetaData[column-1].isReadOnly();
	}
	
	/**
	 * Indicates whether it is possible for a write on the designated column to
	 * succeed.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isWritable(int column) throws SQLException {
		return columnMetaData[column-1].isWritable();
	}
	
	/**
	 * Indicates whether a write on the designated column will definitely
	 * succeed.	
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return <code>true</code> if so; <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return columnMetaData[column-1].isDefinitelyWritable();
	}
	
	/**
	 * Returns the fully-qualified name of the Java class whose instances are
	 * manufactured if the method <code>ResultSet.getObject</code> is called to
	 * retrieve a value from the column. <code>ResultSet.getObject</code> may
	 * return a subclass of the class returned by this method.
	 *
	 * @param column the first column is 1, the second is 2, ...
	 * @return the fully-qualified name of the class in the Java programming
	 *         language that would be used by the method
	 *         <code>ResultSet.getObject</code> to retrieve the value in the
	 *         specified column. This is the class name used for custom
	 *         mapping.
	 * @throws SQLException if a database access error occurs.
	 */
	public String getColumnClassName(int column) throws SQLException {
		return columnMetaData[column-1].getColumnClassName();
	}
	
	/**
	 * Returns a string representation of this relational metadata.
	 * 
	 * @return a string representation of this relational metadata.
	 */
	@Override
	public String toString() {
		try {
			if (getColumnCount() == 0)
				return "[]";
			
			StringBuffer string = new StringBuffer().append('[').append(getColumnTypeName(1)).append(' ').append(getColumnName(1));
			
			for (int column = 2; column <= getColumnCount(); column++)
				string.append(", ").append(getColumnTypeName(column)).append(' ').append(getColumnName(column));
			
			return string.append(']').toString();
		}
		catch (SQLException sqle) {
			throw new MetaDataException("sql exception occured during string construction: \'" + sqle.getMessage() + "\'");
		}
	}
	
	/**
	 * Indicates whether some other object is "equal to" this relational
	 * metadata.
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
	 *         is this relational metadata or
	 *     </li>
	 *     <li>
	 *         is an instance of the type <code>ResultSetMetaData</code> and a
	 *         {@link ResultSetMetaDatas#RESULTSET_METADATA_COMPARATOR comparator}
	 *         for relational metadata returns 0.
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
		return object instanceof ResultSetMetaData && ResultSetMetaDatas.RESULTSET_METADATA_COMPARATOR.compare(this, (ResultSetMetaData)object) == 0;
	}
	
	/**
	 * Returns the hash code value for this relational metadata using a
	 * {@link ResultSetMetaDatas#RESULTSET_METADATA_HASH_FUNCTION hash function}
	 * for relational metadata.
	 *
	 * @return the hash code value for this relational metadata.
	 * @see Object#hashCode()
	 * @see #equals(Object)
	 */
	@Override
	public int hashCode() {
		return ResultSetMetaDatas.RESULTSET_METADATA_HASH_FUNCTION.invoke(this);
	}
	
	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. The
	 * result may be either the object found to implement the interface or a
	 * proxy for that object. If the receiver implements the interface then
	 * that is the object. If the receiver is a wrapper and the wrapped object
	 * implements the interface then that is the object. Otherwise the object
	 * is the result of calling <code>unwrap</code> recursively on the wrapped
	 * object. If the receiver is not a wrapper and does not implement the
	 * interface, then an <code>SQLException</code> is thrown.
	 *
	 * @param iface a class defining an interface that the result must
	 *        implement.
	 * @return an object that implements the interface. May be a proxy for the
	 *         actual implementing object.
	 * @throws SQLException if no object found that implements the interface.
	 */
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("this method is not implemented yet.");
	}
	
	/**
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if
	 * this is a wrapper then return the result of recursively calling
	 * <code>isWrapperFor</code> on the wrapped object. If this does not
	 * implement the interface and is not a wrapper, return false. This method
	 * should be implemented as a low-cost operation compared to
	 * <code>unwrap</code> so that callers can use this method to avoid
	 * expensive <code>unwrap</code> calls that may fail. If this method
	 * returns true then calling <code>unwrap</code> with the same argument
	 * should succeed.
	 *
	 * @param iface a class defining an interface.
	 * @return true if this implements the interface or directly or indirectly
	 *         wraps an object that does.
	 * @throws SQLException if an error occurs while determining whether this
	 *         is a wrapper for an object with the given interface.
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("this method is not implemented yet.");
	}
	
	//*******************************************************************//
	
	/**
	 * This static method creates a stored metadata for a relation "actors" of
	 * a movie database.
	 * 
	 * @return a stored metadata for a relation "actors" of a movie database.
	 */
	public static ColumnMetaDataResultSetMetaData actors() {
		return new ColumnMetaDataResultSetMetaData(
			//  StoredColumnMetaData(autoIncrement, caseSensitive, searchable, currency, nullable,                         signed, columnDisplaySize, columnLabel,  columnName, schemaName, precision, scale, tableName, catalogName, columnType,              columnTypeName, readOnly, writable, definitelyWritable, columnClassName    ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  40,                "stage name", "stagenm",  "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNullable, false,  20,                "birth name", "birthnm",  "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNullable, false,  20,                "first name", "firstnm",  "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         false,         true,       false,    ResultSetMetaData.columnNullable, false,  4,                 "birth year", "dob",      "",         4,         0,     "actors",  "",          java.sql.Types.SMALLINT, "SMALLINT",     true,     false,    false,              "java.lang.Integer"),
			new StoredColumnMetaData(false,         false,         true,       false,    ResultSetMetaData.columnNoNulls,  false,  3,                 "origin",     "origin",   "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         false,         true,       false,    ResultSetMetaData.columnNoNulls,  false,  1,                 "gender",     "gender",   "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" )
		);
	}
	
	/**
	 * This static method creates a stored metadata for a relation "casts" of
	 * a movie database.
	 * 
	 * @return a stored metadata for a relation "casts" of a movie database.
	 */
	public static ColumnMetaDataResultSetMetaData casts() {
		return new ColumnMetaDataResultSetMetaData(
			//  StoredColumnMetaData(autoIncrement, caseSensitive, searchable, currency, nullable,                         signed, columnDisplaySize, columnLabel,  columnName, schemaName, precision, scale, tableName, catalogName, columnType,              columnTypeName, readOnly, writable, definitelyWritable, columnClassName    ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  6,                 "film ID",    "filmid",   "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  40,                "film title", "title",    "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  40,                "actor",      "actor",    "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" )
		);
	}
	
	/**
	 * This static method creates a stored metadata for a relation "movies" of
	 * a movie database.
	 * 
	 * @return a stored metadata for a relation "movies" of a movie database.
	 */
	public static ColumnMetaDataResultSetMetaData movies() {
		return new ColumnMetaDataResultSetMetaData(
			//  StoredColumnMetaData(autoIncrement, caseSensitive, searchable, currency, nullable,                         signed, columnDisplaySize, columnLabel,  columnName, schemaName, precision, scale, tableName, catalogName, columnType,              columnTypeName, readOnly, writable, definitelyWritable, columnClassName    ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  6,                 "film ID",    "filmid",   "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  40,                "film title", "title",    "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" ),
			new StoredColumnMetaData(false,         false,         true,       false,    ResultSetMetaData.columnNoNulls,  false,  4,                 "year",       "year",     "",         4,         0,     "actors",  "",          java.sql.Types.SMALLINT, "SMALLINT",     true,     false,    false,              "java.lang.Integer"),
			new StoredColumnMetaData(false,         true,          true,       false,    ResultSetMetaData.columnNoNulls,  false,  40,                "director",   "director", "",         0,         0,     "actors",  "",          java.sql.Types.VARCHAR,  "VARCHAR",      true,     false,    false,              "java.lang.String" )
		);
	}
	
}
