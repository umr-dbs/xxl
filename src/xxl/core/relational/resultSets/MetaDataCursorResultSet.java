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

package xxl.core.relational.resultSets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.SerializableConverter;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * This class wraps a metadata cursor to a result set.
 * 
 * <p>Result sets and metadata cursors are equivalent concepts. Both handle
 * sets of objects with metadata. The different direction is done by the class
 * {@link xxl.core.relational.cursors.ResultSetMetaDataCursor}.</p>
 */
public class MetaDataCursorResultSet extends AbstractResultSet {

	/**
	 * The metadata cursor that is wrapped into a result set.
	 */
	protected MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> metaDataCursor;
	
	/**
	 * The result set's metadata provided by the metadata cursor.
	 */
	protected ResultSetMetaData metaData;
	
	/**
	 * The next tuple of the metadata cursor.
	 */
	protected Tuple next;
	
	/**
	 * The index of the last column that became accessed.
	 */
	protected int lastColumnIndex = 0;

	/**
	 * Constructs a new result set based on the given metadata cursor.
	 * 
	 * @param metaDataCursor the metadata cursor that should be wrapped to a
	 *        result set.
	 */
	public MetaDataCursorResultSet(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> metaDataCursor) {
		this.metaDataCursor = metaDataCursor;
		this.metaData = ResultSetMetaDatas.getResultSetMetaData(metaDataCursor);
	}

	/**
	 * Moves the cursor down one row from its current position. A
	 * <code>ResultSet</code> cursor is initially positioned before the first
	 * row; the first call to the method <code>next</code> makes the first row
	 * the current row; the second call makes the second row the current row,
	 * and so on.
	 * 
	 * <p>If an input stream is open for the current row, a call to the method
	 * <code>next</code> will implicitly close it. A <code>ResultSet</code>
	 * object's warning chain is cleared when a new row is read.</p>
	 * 
	 * @return <code>true</code> if the new current row is valid;
	 *         <code>false</code> if there are no more rows.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public boolean next() throws SQLException {
		if (metaDataCursor.hasNext()) {
			next = metaDataCursor.next();
			return true;
		}
		return false;
	}
	
	/**
	 * Releases this <code>ResultSet</code> object's database and JDBC
	 * resources immediately instead of waiting for this to happen when it is
	 * automatically closed.
	 * 
	 * <p><b>Note:</b> A <code>ResultSet</code> object is automatically closed
	 * by the <code>Statement</code> object that generated it when that
	 * <code>Statement</code> object is closed, re-executed, or is used to
	 * retrieve the next result from a sequence of multiple results. A
	 * <code>ResultSet</code> object is also automatically closed when it is
	 * garbage collected.</p>
	 * 
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public void close() throws SQLException {
		metaDataCursor.close();
	}
	
	/**
	 * Gets the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Object</code> in the Java
	 * programming language.
	 * 
	 * <p>This method will return the value of the given column as a Java
	 * object. The type of the Java object will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for
	 * built-in types specified in the JDBC specification. If the value is an
	 * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.</p>
	 * 
	 * <p>This method may also be used to read database-specific abstract data
	 * types. In the JDBC 2.0 API, the behavior of method
	 * <code>getObject</code> is extended to materialize data of SQL
	 * user-defined types. When a column contains a structured or distinct
	 * value, the behavior of this method is as if it were a call to:
	 * <code>getObject(columnIndex, this.getStatement().getConnection().getTypeMap())</code>.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a <code>java.lang.Object</code> holding the column value.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		lastColumnIndex = columnIndex;
		return next.getObject(columnIndex);
	}
	
	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> array in the Java
	 * programming language. The bytes represent the raw values returned by the
	 * driver.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return getBytes(columnIndex, Converters.getObjectConverter(SerializableConverter.DEFAULT_INSTANCE));
	}
	
	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> array in the Java
	 * programming language. The byte representation of the column is generated
	 * using the specified converter.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param converter the converter used for transforming the column into a
	 *        byte representation.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException if a database access error occurs.
	 */
	public byte[] getBytes(int columnIndex, Converter<Object> converter) throws SQLException {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			converter.write(oos, getObject(columnIndex));
			byte[] result = baos.toByteArray();
			oos.close();
			return result;
		}
		catch (IOException ioe) {
			throw new SQLException("I/O exception occured during convertion : " + ioe.getMessage());
		}
	}

	/**
	 * Reports whether the last column read had a value of SQL
	 * <code>NULL</code>. Note that you must first call one of the getter
	 * methods on a column to try to read its value and then call the method
	 * <code>wasNull</code> to see if the value read was SQL <code>NULL</code>.
	 * 
	 * @return <code>true</code> if last column read was SQL <code>NULL</code>
	 *         and <code>false</code> otherwise.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public boolean wasNull() throws SQLException {
		return next.getObject(lastColumnIndex) == null ? true : false;
	}

	/**
	 * Retrieves the number, types and properties of this
	 * <code>ResultSet</code> object's columns.
	 * 
	 * @return the description of this <code>ResultSet</code> object's columns.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metaData;
	}

	/**
	 * Maps the given <code>ResultSet</code> column name to its
	 * <code>ResultSet</code> column index.
	 * 
	 * @param columnName the name of the column.
	 * @return the column index of the given column name.
	 * @throws SQLException if the <code>ResultSet</code> object does not
	 *         contain <code>columnName</code> or a database access error
	 *         occurs.
	 */
	@Override
	public int findColumn(String columnName) throws SQLException {
		for (int index = 1; index <= metaData.getColumnCount(); index++)
			if (metaData.getColumnName(index).equals(columnName))
				return index;
		throw new SQLException("a column with  vthe specified name cannot be found");
	}

	/**
	 * Updates the designated column with an <code>Object</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName the name of the column.
	 * @param x the new column value.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public void updateObject(String columnName, Object x) throws SQLException {
		updateObject(findColumn(columnName), x);
	}

	/**
	 * Deletes the current row from this <code>ResultSet</code> object and
	 * from the underlying database. This method cannot be called when the
	 * cursor is on the insert row.
	 * 
	 * @throws SQLException if a database access error occurs or if this method
	 *         is called when the cursor is on the insert row.
	 */
	@Override
	public void deleteRow() throws SQLException {
		metaDataCursor.remove();
	}
}
