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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * This class is a skeleton for writing non-abstract classes that implement the
 * result set interface. This class is very useful to integrate new sources into
 * XXL.
 * 
 * <p>
 * Only JDBC 1.0 is supported. All methods of JDBC 2.0 throw an
 * <code>UnsupportedOperationException</code>.
 * </p>
 * 
 * <p>
 * Only seven methods have to be implemented to get a non-abstract result set:
 * <ul>
 * <li>ResultSetMetaData <b>getMetaData</b>()</li>
 * <li>int <b>findColumn</b>(String columnName)</li>
 * <li>Object <b>getObject</b>(int columnIndex)</li>
 * <li>byte[] <b>getBytes</b>(int columnIndex)</li>
 * <li>boolean <b>next</b>()</li>
 * <li>void <b>close</b>()</li>
 * <li>boolean <b>wasNull</b>()</li>
 * </ul>
 * The <code>getXXX</code> methods call <code>getObject</code> and cast the
 * result properly.
 * </p>
 * 
 * <p>
 * For a complete description of the methods read the documentation of
 * {@link java.sql.ResultSet}.
 * </p>
 * 
 * <p>
 * An example for a non-abstract subclass of <code>AbstractResultSet</code>:
 * <code><pre>
 *   class MySelection extends AbstractResultSet {
 *       protected ResultSet resultSet;
 *       protected Predicate&lt;? extends ResultSet&gt; predicate;
 *       
 *       public MySelection(Predicate&lt;? extends ResultSet&gt; predicate, ResultSet resultSet) {
 *           this.resultSet = resultSet;
 *           this.predicate = predicate;
 *       }
 *       
 *       public boolean next() throws SQLException {
 *           boolean next = resultSet.next();
 *           while (next && !predicate.invoke(resultSet))
 *               next = resultSet.next();
 *           return next;
 *       }
 *       
 *       public ResultSetMetaData getMetaData() throws SQLException {
 *           return resultSet.getMetaData();
 *       }
 *       
 *       public int findColumn(String columnName) throws SQLException {
 *           return resultSet.findColumn(columnName);
 *       }
 *       
 *       public Object getObject(int columnIndex) throws SQLException {
 *           return resultSet.getObject(columnIndex);
 *       }
 *       
 *       public byte[] getBytes(int columnIndex) throws SQLException {
 *           return resultSet.getBytes(columnIndex);
 *       }
 *       
 *       public boolean wasNull() throws SQLException {
 *           return resultSet.wasNull();
 *       }
 *       
 *       public void close() throws SQLException {
 *           resultSet.close();
 *       }
 *   }
 * </pre></code>
 * </p>
 */
public abstract class AbstractResultSet implements ResultSet {

	// abstract methods

	/**
	 * Moves the cursor down one row from its current position. A
	 * <code>ResultSet</code> cursor is initially positioned before the first
	 * row; the first call to the method <code>next</code> makes the first row
	 * the current row; the second call makes the second row the current row,
	 * and so on.
	 * 
	 * <p>
	 * If an input stream is open for the current row, a call to the method
	 * <code>next</code> will implicitly close it. A <code>ResultSet</code>
	 * object's warning chain is cleared when a new row is read.
	 * </p>
	 * 
	 * @return <code>true</code> if the new current row is valid;
	 *         <code>false</code> if there are no more rows.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract boolean next() throws SQLException;

	/**
	 * Releases this <code>ResultSet</code> object's database and JDBC resources
	 * immediately instead of waiting for this to happen when it is
	 * automatically closed.
	 * 
	 * <p>
	 * <b>Note:</b> A <code>ResultSet</code> object is automatically closed by
	 * the <code>Statement</code> object that generated it when that
	 * <code>Statement</code> object is closed, re-executed, or is used to
	 * retrieve the next result from a sequence of multiple results. A
	 * <code>ResultSet</code> object is also automatically closed when it is
	 * garbage collected.
	 * </p>
	 * 
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract void close() throws SQLException;

	/**
	 * Gets the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Object</code> in the Java
	 * programming language.
	 * 
	 * <p>
	 * This method will return the value of the given column as a Java object.
	 * The type of the Java object will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for
	 * built-in types specified in the JDBC specification. If the value is an
	 * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
	 * </p>
	 * 
	 * <p>
	 * This method may also be used to read database-specific abstract data
	 * types. In the JDBC 2.0 API, the behavior of method <code>getObject</code>
	 * is extended to materialize data of SQL user-defined types. When a column
	 * contains a structured or distinct value, the behavior of this method is
	 * as if it were a call to:
	 * <code>getObject(columnIndex, this.getStatement().getConnection().getTypeMap())</code>.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>java.lang.Object</code> holding the column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract Object getObject(int columnIndex) throws SQLException;

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> array in the Java
	 * programming language. The bytes represent the raw values returned by the
	 * driver.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract byte[] getBytes(int columnIndex) throws SQLException;

	/**
	 * Reports whether the last column read had a value of SQL <code>NULL</code>
	 * . Note that you must first call one of the getter methods on a column to
	 * try to read its value and then call the method <code>wasNull</code> to
	 * see if the value read was SQL <code>NULL</code>.
	 * 
	 * @return <code>true</code> if last column read was SQL <code>NULL</code>
	 *         and <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract boolean wasNull() throws SQLException;

	/**
	 * Retrieves the number, types and properties of this <code>ResultSet</code>
	 * object's columns.
	 * 
	 * @return the description of this <code>ResultSet</code> object's columns.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public abstract ResultSetMetaData getMetaData() throws SQLException;

	/**
	 * Maps the given <code>ResultSet</code> column name to its
	 * <code>ResultSet</code> column index.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @return the column index of the given column name.
	 * @throws SQLException
	 *             if the <code>ResultSet</code> object does not contain
	 *             <code>columnName</code> or a database access error occurs.
	 */
	public abstract int findColumn(String columnName) throws SQLException;

	// methods based on abstract methods

	/**
	 * Gets the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Object</code> in the Java
	 * programming language.
	 * 
	 * <p>
	 * This method will return the value of the given column as a Java object.
	 * The type of the Java object will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for
	 * built-in types specified in the JDBC specification. If the value is an
	 * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
	 * </p>
	 * 
	 * <p>
	 * This method may also be used to read database-specific abstract data
	 * types.
	 * </p>
	 * 
	 * <p>
	 * In the JDBC 2.0 API, the behavior of the method <code>getObject</code> is
	 * extended to materialize data of SQL user-defined types. When a column
	 * contains a structured or distinct value, the behavior of this method is
	 * as if it were a call to:
	 * <code>getObject(columnIndex, this.getStatement().getConnection().getTypeMap())</code>
	 * .
	 * </p>
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return a <code>java.lang.Object</code> holding the column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Object getObject(String columnName) throws SQLException {
		return getObject(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> array in the Java
	 * programming language. The bytes represent the raw values returned by the
	 * driver.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public byte[] getBytes(String columnName) throws SQLException {
		return getBytes(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a stream of ASCII characters. The value
	 * can then be read in chunks from the stream. This method is particularly
	 * suitable for retrieving large LONGVARCHAR values. The JDBC driver will do
	 * any necessary conversion from the database format into ASCII.
	 * 
	 * <p>
	 * <b>Note:
	 * </p>
	 * All the data in the returned stream must be read prior to getting the
	 * value of any other column. The next call to a getter method implicitly
	 * closes the stream. Also, a stream may return <code>0</code> when the
	 * method <code>InputStream.available</code> is called whether there is data
	 * available or not.</p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of one-byte ASCII characters; if the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code> .
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return new ByteArrayInputStream(getBytes(columnIndex));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a stream of ASCII characters. The value
	 * can then be read in chunks from the stream. This method is particularly
	 * suitable for retrieving large <code>LONGVARCHAR</code> values. The JDBC
	 * driver will do any necessary conversion from the database format into
	 * ASCII.
	 * 
	 * <p>
	 * <b>Note:</b> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a getter method
	 * implicitly closes the stream. Also, a stream may return <code>0</code>
	 * when the method <code>available</code> is called whether there is data
	 * available or not.
	 * </p>
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of one-byte ASCII characters. If the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public InputStream getAsciiStream(String columnName) throws SQLException {
		return getAsciiStream(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a binary stream of uninterpreted bytes.
	 * The value can then be read in chunks from the stream. This method is
	 * particularly suitable for retrieving large <code>LONGVARBINARY</code>
	 * values.
	 * 
	 * <p>
	 * <b>Note:</b> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a getter method
	 * implicitly closes the stream. Also, a stream may return <code>0</code>
	 * when the method <code>InputStream.available</code> is called whether
	 * there is data available or not.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of uninterpreted bytes; if the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return new ByteArrayInputStream(getBytes(columnIndex));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a stream of uninterpreted
	 * <code>byte</code>s. The value can then be read in chunks from the stream.
	 * This method is particularly suitable for retrieving large
	 * <code>LONGVARBINARY</code> values.
	 * 
	 * <p>
	 * <b>Note:</b> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a getter method
	 * implicitly closes the stream. Also, a stream may return <code>0</code>
	 * when the method <code>available</code> is called whether there is data
	 * available or not.
	 * </p>
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of uninterpreted bytes; if the value is SQL
	 *         <code>NULL</code>, the result is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public InputStream getBinaryStream(String columnName) throws SQLException {
		return getBinaryStream(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>boolean</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>false</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? false : o instanceof Boolean ? ((Boolean) o)
				.booleanValue() : o instanceof Number ? ((Number) o)
				.doubleValue() != 0
				: o instanceof BigDecimal ? ((BigDecimal) o).signum() != 0
						: Boolean.parseBoolean(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>boolean</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>false</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean getBoolean(String columnName) throws SQLException {
		return getBoolean(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public byte getByte(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Byte ? ((Byte) o).byteValue()
				: Byte.parseByte(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>byte</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public byte getByte(String columnName) throws SQLException {
		return getByte(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Date</code> object in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Date getDate(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? null : o instanceof Date ? (Date) o : Date.valueOf(o
				.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row * of this
	 * <code>ResultSet</code> object as a <code>java.sql.Date</code> object in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public Date getDate(String columnName) throws SQLException {
		return getDate(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>double</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public double getDouble(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Double ? ((Double) o).doubleValue()
				: Double.parseDouble(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>double</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public double getDouble(String columnName) throws SQLException {
		return getDouble(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>float</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public float getFloat(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Float ? ((Float) o).floatValue()
				: Float.parseFloat(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>float</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public float getFloat(String columnName) throws SQLException {
		return getFloat(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>int</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public int getInt(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Integer ? ((Integer) o).intValue()
				: Integer.parseInt(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>int</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public int getInt(String columnName) throws SQLException {
		return getInt(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>long</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public long getLong(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Long ? ((Long) o).longValue()
				: Long.parseLong(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>long</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public long getLong(String columnName) throws SQLException {
		return getLong(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>short</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public short getShort(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? 0 : o instanceof Short ? ((Short) o).shortValue()
				: Short.parseShort(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>short</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>0</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public short getShort(String columnName) throws SQLException {
		return getShort(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public String getString(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? null : o.toString();
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public String getString(String columnName) throws SQLException {
		return getString(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Time</code> object in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Time getTime(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return (o == null) ? null : (o instanceof Time) ? (Time) o : Time
				.valueOf(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Time</code> object in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Time getTime(String columnName) throws SQLException {
		return getTime(findColumn(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code> object
	 * in the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return o == null ? null : o instanceof Timestamp ? (Timestamp) o
				: Timestamp.valueOf(o.toString());
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code>
	 * object.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return getTimestamp(findColumn(columnName));
	}

	// unimplemented methods

	/**
	 * Clears all warnings reported on this <code>ResultSet</code> object. After
	 * this method is called, the method <code>getWarnings</code> returns
	 * <code>null</code> until a new warning is reported for this
	 * <code>ResultSet</code> object.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the first warning reported by calls on this
	 * <code>ResultSet</code> object. Subsequent warnings on this
	 * <code>ResultSet</code> object will be chained to the
	 * <code>SQLWarning</code> object that this method returns.
	 * <p>
	 * The warning chain is automatically cleared each time a new row is read.
	 * This method may not be called on a <code>ResultSet</code> object that has
	 * been closed; doing so will cause an <code>SQLException</code> to be
	 * thrown.
	 * </p>
	 * <p>
	 * <b>Note:</b> This warning chain only covers warnings caused by
	 * <code>ResultSet</code> methods. Any warning caused by
	 * <code>Statement</code> methods (such as reading OUT parameters) will be
	 * chained on the <code>Statement</code> object.
	 * </p>
	 * 
	 * @return the first <code>SQLWarning</code> object reported or
	 *         <code>null</code> if there are none.
	 * @throws SQLException
	 *             if a database access error occurs or this method is called on
	 *             a closed result set.
	 */
	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the name of the SQL cursor used by this <code>ResultSet</code>
	 * object.
	 * 
	 * <p>
	 * In SQL, a result table is retrieved through a cursor that is named. The
	 * current row of a result set can be updated or deleted using a positioned
	 * update/delete statement that references the cursor name. To insure that
	 * the cursor has the proper isolation level to support update, the cursor's
	 * <code>SELECT</code> statement should be of the form
	 * <code>SELECT FOR UPDATE</code>. If <code>FOR UPDATE</code> is omitted,
	 * the positioned updates may fail.
	 * <p>
	 * 
	 * <p>
	 * The JDBC API supports this SQL feature by providing the name of the SQL
	 * cursor used by a <code>ResultSet</code> object. The current row of a
	 * <code>ResultSet</code> object is also the current row of this SQL cursor.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> If positioned update is not supported, a
	 * <code>SQLException</code> is thrown.
	 * </p>
	 * 
	 * @return the SQL name for this <code>ResultSet</code> object's cursor.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public String getCursorName() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	// unimplemented methods from Java 1.2

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Date</code> object in
	 * the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the date if the underlying
	 * database does not store timezone information.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the date.
	 * @return the column value as a <code>java.sql.Date</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Date</code> object in
	 * the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the date if the underlying
	 * database does not store timezone information.
	 * 
	 * @param columnName
	 *            the SQL name of the column from which to retrieve the value.
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the date.
	 * @return the column value as a <code>java.sql.Date</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Date getDate(String columnName, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Time</code> object in
	 * the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the time if the underlying
	 * database does not store timezone information.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the time.
	 * @return the column value as a <code>java.sql.Time</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Time</code> object in
	 * the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the time if the underlying
	 * database does not store timezone information.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the time.
	 * @return the column value as a <code>java.sql.Time</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Time getTime(String columnName, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code> object
	 * in the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the timestamp if the
	 * underlying database does not store timezone information.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the timestamp.
	 * @return the column value as a <code>java.sql.Timestamp</code> object; if
	 *         the value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.Timestamp</code> object
	 * in the Java programming language. This method uses the given calendar to
	 * construct an appropriate millisecond value for the timestamp if the
	 * underlying database does not store timezone information.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @param cal
	 *            the <code>java.util.Calendar</code> object to use in
	 *            constructing the date.
	 * @return the column value as a <code>java.sql.Timestamp</code> object; if
	 *         the value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Timestamp getTimestamp(String columnName, Calendar cal)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the first row in this <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if there are no rows in the result set.
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public boolean first() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the given row number in this <code>ResultSet</code>
	 * object.
	 * <p>
	 * If the row number is positive, the cursor moves to the given row number
	 * with respect to the beginning of the result set. The first row is row 1,
	 * the second is row 2, and so on.
	 * </p>
	 * <p>
	 * If the given row number is negative, the cursor moves to an absolute row
	 * position with respect to the end of the result set. For example, calling
	 * the method <code>absolute(-1)</code> positions the cursor on the last
	 * row; calling the method <code>absolute(-2)</code> moves the cursor to the
	 * next-to-last row, and so on.
	 * </p>
	 * <p>
	 * An attempt to position the cursor beyond the first/last row in the result
	 * set leaves the cursor before the first row or after the last row.
	 * </p>
	 * <p>
	 * <b>Note:</b> Calling <code>absolute(1)</code> is the same as calling
	 * <code>first()</code>. Calling <code>absolute(-1)</code> is the same as
	 * calling <code>last()</code>.
	 * </p>
	 * 
	 * @param row
	 *            the number of the row to which the cursor should move. A
	 *            positive number indicates the row number counting from the
	 *            beginning of the result set; a negative number indicates the
	 *            row number counting from the end of the result set.
	 * @return <code>true</code> if the cursor is on the result set;
	 *         <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs, or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public boolean absolute(int row) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the end of this <code>ResultSet</code> object, just
	 * after the last row. This method has no effect if the result set contains
	 * no rows.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public void afterLast() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the front of this <code>ResultSet</code> object, just
	 * before the first row. This method has no effect if the result set
	 * contains no rows.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public void beforeFirst() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Cancels the updates made to the current row in this
	 * <code>ResultSet</code> object. This method may be called after calling an
	 * updater method(s) and before calling the method <code>updateRow</code> to
	 * roll back the updates made to a row. If no updates have been made or
	 * <code>updateRow</code> has already been called, this method has no
	 * effect.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or if this method is called
	 *             when the cursor is on the insert row.
	 */
	public void cancelRowUpdates() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Deletes the current row from this <code>ResultSet</code> object and from
	 * the underlying database. This method cannot be called when the cursor is
	 * on the insert row.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or if this method is called
	 *             when the cursor is on the insert row.
	 */
	public void deleteRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Array</code> object in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return an <code>Array</code> object representing the SQL
	 *         <code>ARRAY</code> value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Array getArray(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Array</code> object in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @return an <code>Array</code> object representing the SQL
	 *         <code>ARRAY</code> value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Array getArray(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code> with
	 * full precision.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value (full precision); if the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code> in the
	 *         Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.BigDecimal</code> in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param scale
	 *            the number of digits to the right of the decimal point.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @deprecated
	 */
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code> with
	 * full precision.
	 * 
	 * @param columnName
	 *            the column name.
	 * @return the column value (full precision); if the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code> in the
	 *         Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.math.BigDecimal</code> in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @param scale
	 *            the number of digits to the right of the decimal point
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @deprecated
	 */
	@Deprecated
	public BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Blob</code> object in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>Blob</code> object representing the SQL <code>BLOB</code>
	 *         value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Blob</code> object in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @return a <code>Blob</code> object representing the SQL <code>BLOB</code>
	 *         value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Blob getBlob(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.io.Reader</code> object.
	 * 
	 * @return a <code>java.io.Reader</code> object that contains the column
	 *         value; if the value is SQL <code>NULL</code>, the value returned
	 *         is <code>null</code> in the Java programming language.
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.io.Reader</code> object.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @return a <code>java.io.Reader</code> object that contains the column
	 *         value; if the value is SQL <code>NULL</code>, the value returned
	 *         is <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Reader getCharacterStream(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Clob</code> object in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>Clob</code> object representing the SQL <code>CLOB</code>
	 *         value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Clob getClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Clob</code> object in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @return a <code>Clob</code> object representing the SQL <code>CLOB</code>
	 *         value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Clob getClob(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the concurrency mode of this <code>ResultSet</code> object. The
	 * concurrency used is determined by the <code>Statement</code> object that
	 * created the result set.
	 * 
	 * @return the concurrency type, either
	 *         <code>ResultSet.CONCUR_READ_ONLY</code> or
	 *         <code>ResultSet.CONCUR_UPDATABLE</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public int getConcurrency() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the fetch direction for this <code>ResultSet</code> object.
	 * 
	 * @return the current fetch direction for this <code>ResultSet</code>
	 *         object.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @see #setFetchDirection
	 */
	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the fetch size for this <code>ResultSet</code> object.
	 * 
	 * @return the current fetch size for this <code>ResultSet</code> object.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @see #setFetchSize
	 */
	public int getFetchSize() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Object</code> in the Java
	 * programming language. If the value is an SQL <code>NULL</code>, the
	 * driver returns a Java <code>null</code>. This method uses the given
	 * <code>Map</code> object for the custom mapping of the SQL structured or
	 * distinct type that is being retrieved.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param map
	 *            a <code>java.util.Map</code> object that contains the mapping
	 *            from SQL type names to classes in the Java programming
	 *            language.
	 * @return an <code>Object</code> in the Java programming language
	 *         representing the SQL value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as an <code>Object</code> in the Java
	 * programming language. If the value is an SQL <code>NULL</code>, the
	 * driver returns a Java <code>null</code>. This method uses the specified
	 * <code>Map</code> object for custom mapping if appropriate.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @param map
	 *            a <code>java.util.Map</code> object that contains the mapping
	 *            from SQL type names to classes in the Java programming
	 *            language.
	 * @return an <code>Object</code> representing the SQL value in the
	 *         specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Object getObject(String columnName, Map<String, Class<?>> map)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	//@Override
	//TODO: Java 7 
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		Object result = getObject(columnIndex);
		try {
			return type.cast(result);
		} catch (ClassCastException e) {
			throw new SQLException("Column " + columnIndex
					+ " has not the desired type " + type);
		}
	}

	//@Override
	//TODO: Java 7 
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		int columnIndex = findColumn(columnLabel);
		return getObject(columnIndex, type);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Ref</code> object in the Java
	 * programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>Ref</code> object representing an SQL <code>REF</code>
	 *         value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Ref getRef(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Ref</code> object in the Java
	 * programming language.
	 * 
	 * @param columnName
	 *            the column name.
	 * @return a <code>Ref</code> object representing the SQL <code>REF</code>
	 *         value in the specified column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Ref getRef(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the current row number. The first row is number 1, the second
	 * number 2, and so on.
	 * 
	 * @return the current row number; <code>0</code> if there is no current
	 *         row.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public int getRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the <code>Statement</code> object that produced this
	 * <code>ResultSet</code> object. If the result set was generated some other
	 * way, such as by a <code>DatabaseMetaData</code> method, this method
	 * returns <code>null</code>.
	 * 
	 * @return the <code>Statment</code> object that produced this
	 *         <code>ResultSet</code> object or <code>null</code> if the result
	 *         set was produced some other way.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public Statement getStatement() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the type of this <code>ResultSet</code> object. The type is
	 * determined by the <code>Statement</code> object that created the result
	 * set.
	 * 
	 * @return <code>ResultSet.TYPE_FORWARD_ONLY</code>,
	 *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
	 *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public int getType() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as as a stream of two-byte Unicode
	 * characters. The first byte is the high byte; the second byte is the low
	 * byte. The value can then be read in chunks from the stream. This method
	 * is particularly suitable for retrieving large <code>LONGVARCHAR</code>
	 * values. The JDBC driver will do any necessary conversion from the
	 * database format into Unicode.
	 * 
	 * <p>
	 * <b>Note:</b> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a getter method
	 * implicitly closes the stream. Also, a stream may return <code>0</code>
	 * when the method <code>InputStream.available</code> is called, whether
	 * there is data available or not.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of two-byte Unicode characters; if the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @deprecated use <code>getCharacterStream</code> in place of
	 *             <code>getUnicodeStream</code>.
	 */
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a stream of two-byte Unicode characters.
	 * The first byte is the high byte; the second byte is the low byte. The
	 * value can then be read in chunks from the stream. This method is
	 * particularly suitable for retrieving large <code>LONGVARCHAR</code>
	 * values. The JDBC technology-enabled driver will do any necessary
	 * conversion from the database format into Unicode.
	 * 
	 * <p>
	 * <b>Note:</b> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a getter method
	 * implicitly closes the stream. Also, a stream may return <code>0</code>
	 * when the method <code>InputStream.available</code> is called, whether
	 * there is data available or not.
	 * </p>
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of two-byte Unicode characters. If the value is SQL
	 *         <code>NULL</code>, the value returned is <code>null</code>.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @deprecated use <code>getCharacterStream</code> instead.
	 */
	@Deprecated
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Inserts the contents of the insert row into this <code>ResultSet</code>
	 * object and into the database. The cursor must be on the insert row when
	 * this method is called.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs, if this method is called
	 *             when the cursor is not on the insert row, or if not all of
	 *             non-nullable columns in the insert row have been given a
	 *             value.
	 */
	public void insertRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the cursor is after the last row in this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is after the last row;
	 *         <code>false</code> if the cursor is at any other position or the
	 *         result set contains no rows.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean isAfterLast() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the cursor is before the first row in this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is before the first row;
	 *         <code>false</code> if the cursor is at any other position or the
	 *         result set contains no rows.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean isBeforeFirst() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the cursor is on the first row of this
	 * <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on the first row;
	 *         <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean isFirst() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the cursor is on the last row of this
	 * <code>ResultSet</code> object. Note: Calling the method
	 * <code>isLast</code> may be expensive because the JDBC driver might need
	 * to fetch ahead one row in order to determine whether the current row is
	 * the last row in the result set.
	 * 
	 * @return <code>true</code> if the cursor is on the last row;
	 *         <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean isLast() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the last row in this <code>ResultSet</code> object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if there are no rows in the result set.
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public boolean last() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the remembered cursor position, usually the current
	 * row. This method has no effect if the cursor is not on the insert row.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or the result set is not
	 *             updatable.
	 */
	public void moveToCurrentRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the insert row. The current cursor position is
	 * remembered while the cursor is positioned on the insert row. The insert
	 * row is a special row associated with an updatable result set. It is
	 * essentially a buffer where a new row may be constructed by calling the
	 * updater methods prior to inserting the row into the result set. Only the
	 * updater, getter, and <code>insertRow</code> methods may be called when
	 * the cursor is on the insert row. All of the columns in a result set must
	 * be given a value each time this method is called before calling
	 * <code>insertRow</code>. An updater method must be called before a getter
	 * method can be called on a column value.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or the result set is not
	 *             updatable.
	 */
	public void moveToInsertRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor to the previous row in this <code>ResultSet</code>
	 * object.
	 * 
	 * @return <code>true</code> if the cursor is on a valid row;
	 *         <code>false</code> if it is off the result set.
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code>.
	 */
	public boolean previous() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Refreshes the current row with its most recent value in the database.
	 * This method cannot be called when the cursor is on the insert row.
	 * <p>
	 * The <code>refreshRow</code> method provides a way for an application to
	 * explicitly tell the JDBC driver to refetch a row(s) from the database. An
	 * application may want to call <code>refreshRow</code> when caching or
	 * prefetching is being done by the JDBC driver to fetch the latest value of
	 * a row from the database. The JDBC driver may actually refresh multiple
	 * rows at once if the fetch size is greater than one.
	 * </p>
	 * <p>
	 * All values are refetched subject to the transaction isolation level and
	 * cursor sensitivity. If <code>refreshRow</code> is called after calling an
	 * updater method, but before calling the method <code>updateRow</code>,
	 * then the updates made to the row are lost. Calling the method
	 * <code>refreshRow</code> frequently will likely slow performance.
	 * </p>
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or if this method is called
	 *             when the cursor is on the insert row.
	 */
	public void refreshRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Moves the cursor a relative number of rows, either positive or negative.
	 * Attempting to move beyond the first/last row in the result set positions
	 * the cursor before/after the the first/last row. Calling
	 * <code>relative(0)</code> is valid, but does not change the cursor
	 * position.
	 * <p>
	 * Note: Calling the method <code>relative(1)</code> is identical to calling
	 * the method <code>next()</code> and calling the method
	 * <code>relative(-1)</code> is identical to calling the method
	 * <code>previous()</code>.
	 * </p>
	 * 
	 * @param rows
	 *            an <code>int</code> specifying the number of rows to move from
	 *            the current row; a positive number moves the cursor forward; a
	 *            negative number moves the cursor backward.
	 * @return <code>true</code> if the cursor is on a row; <code>false</code>
	 *         otherwise.
	 * @throws SQLException
	 *             if a database access error occurs, there is no current row,
	 *             or the result set type is <code>TYPE_FORWARD_ONLY</code>.
	 */
	public boolean relative(int rows) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether a row has been deleted. A deleted row may leave a
	 * visible "hole" in a result set. This method can be used to detect holes
	 * in a result set. The value returned depends on whether or not this
	 * <code>ResultSet</code> object can detect deletions.
	 * 
	 * @return <code>true</code> if a row was deleted and deletions are
	 *         detected; <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @see DatabaseMetaData#deletesAreDetected
	 */
	public boolean rowDeleted() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the current row has had an insertion. The value
	 * returned depends on whether or not this <code>ResultSet</code> object can
	 * detect visible inserts.
	 * 
	 * @return <code>true</code> if a row has had an insertion and insertions
	 *         are detected; <code>false</code> otherwise.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @see DatabaseMetaData#insertsAreDetected
	 */
	public boolean rowInserted() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether the current row has been updated. The value returned
	 * depends on whether or not the result set can detect updates.
	 * 
	 * @return <code>true</code> if both (1) the row has been visibly updated by
	 *         the owner or another and (2) updates are detected.
	 * @throws SQLException
	 *             if a database access error occurs.
	 * @see DatabaseMetaData#updatesAreDetected
	 */
	public boolean rowUpdated() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Gives a hint as to the direction in which the rows in this
	 * <code>ResultSet</code> object will be processed. The initial value is
	 * determined by the <code>Statement</code> object that produced this
	 * <code>ResultSet</code> object. The fetch direction may be changed at any
	 * time.
	 * 
	 * @param direction
	 *            an <code>int</code> specifying the suggested fetch direction;
	 *            one of <code>ResultSet.FETCH_FORWARD</code>,
	 *            <code>ResultSet.FETCH_REVERSE</code>, or
	 *            <code>ResultSet.FETCH_UNKNOWN</code>.
	 * @throws SQLException
	 *             if a database access error occurs or the result set type is
	 *             <code>TYPE_FORWARD_ONLY</code> and the fetch direction is not
	 *             <code>FETCH_FORWARD</code>.
	 * @see Statement#setFetchDirection
	 * @see #getFetchDirection
	 */
	public void setFetchDirection(int direction) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Gives the JDBC driver a hint as to the number of rows that should be
	 * fetched from the database when more rows are needed for this
	 * <code>ResultSet</code> object. If the fetch size specified is zero, the
	 * JDBC driver ignores the value and is free to make its own best guess as
	 * to what the fetch size should be. The default value is set by the
	 * <code>Statement</code> object that created the result set. The fetch size
	 * may be changed at any time.
	 * 
	 * @param rows
	 *            the number of rows to fetch.
	 * @throws SQLException
	 *             if a database access error occurs or the condition
	 *             <code>0 &le; rows &le; Statement.getMaxRows()</code> is not
	 *             satisfied.
	 * @see #getFetchSize
	 */
	public void setFetchSize(int rows) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateAsciiStream(String columnName, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.math.BigDecimal</code>
	 * value. The updater methods are used to update column values in the
	 * current row or the insert row. The updater methods do not update the
	 * underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.BigDecimal</code>
	 * value. The updater methods are used to update column values in the
	 * current row or the insert row. The updater methods do not update the
	 * underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBigDecimal(String columnName, BigDecimal x)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBinaryStream(String columnName, InputStream x, int length)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>boolean</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>boolean</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>byte</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>byte</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateByte(String columnName, byte x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>byte</code> array value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a byte array value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBytes(String columnName, byte[] x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateCharacterStream(String columnName, Reader reader,
			int length) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Date</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Date</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateDate(String columnName, Date x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>double</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>double</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateDouble(String columnName, double x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>float</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>float	</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateFloat(String columnName, float x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>int</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>int</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateInt(String columnName, int x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>long</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>long</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateLong(String columnName, long x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Gives a nullable column a null value. The updater methods are used to
	 * update column values in the current row or the insert row. The updater
	 * methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to
	 * update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateNull(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>null</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the <code>updateRow</code> or <code>insertRow</code> methods are called
	 * to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateNull(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>Object</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>Object</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @param scale
	 *            for <code>java.sql.Types.DECIMA</code> or
	 *            <code>java.sql.Types.NUMERIC</code> types, this is the number
	 *            of digits after the decimal point. For all other types this
	 *            value will be ignored.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateObject(int columnIndex, Object x, int scale)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>Object</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateObject(String columnName, Object x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an <code>Object</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @param scale
	 *            for <code>java.sql.Types.DECIMAL</code> or
	 *            <code>java.sql.Types.NUMERIC</code> types, this is the number
	 *            of digits after the decimal point. For all other types this
	 *            value will be ignored.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateObject(String columnName, Object x, int scale)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the underlying database with the new contents of the current row
	 * of this <code>ResultSet</code> object. This method cannot be called when
	 * the cursor is on the insert row.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs or if this method is called
	 *             when the cursor is on the insert row.
	 */
	public void updateRow() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>short</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>short</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateShort(String columnName, short x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>String</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>String</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateString(String columnName, String x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Time</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Time</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateTime(String columnName, Time x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Timestamp</code>
	 * value. The updater methods are used to update column values in the
	 * current row or the insert row. The updater methods do not update the
	 * underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Timestamp</code>
	 * value. The updater methods are used to update column values in the
	 * current row or the insert row. The updater methods do not update the
	 * underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateTimestamp(String columnName, Timestamp x)
			throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	// unimplemented methods from Java 1.4

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.net.URL</code> object in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the index of the column 1 is the first, 2 is the second,...
	 * @return the column value as a <code>java.net.URL</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs, or if a URL is malformed.
	 */
	public URL getURL(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.net.URL</code> object in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the SQL name of the column
	 * @return the column value as a <code>java.net.URL</code> object; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if a database access error occurs or if a URL is malformed.
	 */
	public URL getURL(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Array</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Array</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateArray(String columnName, Array x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Blob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Blob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateBlob(String columnName, Blob x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Clob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Clob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateClob(String columnName, Clob x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Ref</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.Ref</code> value. The
	 * updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public void updateRef(String columnName, Ref x) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	// unimplemented methods from Java 1.6 (JDBC 4.0 and Wrapper interface)

	/**
	 * Retrieves the holdability of this <code>ResultSet</code> object.
	 * 
	 * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
	 *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>.
	 * @throws SQLException
	 *             if a database access error occurs or this method is called on
	 *             a closed result set.
	 */
	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.io.Reader</code> object. It
	 * is intended for use when accessing <code>NCHAR</code>,
	 * <code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
	 * 
	 * @return a <code>java.io.Reader</code> object that contains the column
	 *         value; if the value is SQL <code>NULL</code>, the value returned
	 *         is <code>null</code> in the Java programming language.
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.io.Reader</code> object. It
	 * is intended for use when accessing <code>NCHAR</code>,
	 * <code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @return a <code>java.io.Reader</code> object that contains the column
	 *         value; if the value is SQL <code>NULL</code>, the value returned
	 *         is <code>null</code> in the Java programming language.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public Reader getNCharacterStream(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.NClob</code> object in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>java.sql.NClob</code> object representing the SQL
	 *         <code>NCLOB</code> value in the specified column.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.NClob</code> object in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @return a <code>java.sql.NClob</code> object representing the SQL
	 *         <code>NCLOB</code> value in the specified column.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public NClob getNClob(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language. It is intended for use when accessing
	 * <code>NCHAR</code>, <code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language. It is intended for use when accessing
	 * <code>NCHAR</code>, <code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 * 
	 * @param columnName
	 *            the SQL name of the column.
	 * @return the column value; if the value is SQL <code>NULL</code>, the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public String getNString(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.RowId</code> object in
	 * the Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @return the column value; if the value is a SQL <code>NULL</code> the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>java.sql.RowId</code> object in
	 * the Java programming language.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @return the column value; if the value is a SQL <code>NULL</code> the
	 *         value returned is <code>null</code>.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public RowId getRowId(String columnName) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> as a <code>java.sql.SQLXML</code> object in the
	 * Java programming language.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @return a <code>java.sql.SQLXML</code> object that maps a
	 *         <code>SQL XML</code> value.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> as a <code>java.sql.SQLXML</code> object in the
	 * Java programming language.
	 * 
	 * @param columnName
	 *            the name of the column from which to retrieve the value.
	 * @return a <code>java.sql.SQLXML</code> object that maps a
	 *         <code>SQL XML</code> value.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs or this method is called on a closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public SQLXML getSQLXML(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Retrieves whether this <code>ResultSet</code> object has been closed. A
	 * <code>ResultSet</code> is closed if the method close has been called on
	 * it, or if it is automatically closed.
	 * 
	 * @return true if this <code>ResultSet</code> object is closed; false if it
	 *         is still open.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public boolean isClosed() throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateAsciiStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateAsciiStream(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateAsciiStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateAsciiStream(String columnName, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value, which will have
	 * the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateAsciiStream(int columnIndex, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with an ascii stream value, which will have
	 * the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateAsciiStream(String columnName, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateBinaryStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBinaryStream(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateBinaryStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBinaryStream(String columnName, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value, which will have
	 * the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBinaryStream(int columnIndex, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a binary stream value, which will have
	 * the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBinaryStream(String columnName, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given input stream. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateBlob</code> which
	 * takes a length parameter.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given input stream. The data will
	 * be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateBlob</code> which
	 * takes a length parameter.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBlob(String columnName, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given input stream, which will
	 * have the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given input stream, which will
	 * have the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param inputStream
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateBlob(String columnName, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The data
	 * will be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateCharacterStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateCharacterStream(int columnIndex, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The data
	 * will be read from the stream as needed until end-of-stream is reached.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of
	 * <code>updateCharacterStream</code> which takes a length parameter.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateCharacterStream(String columnName, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value, which will
	 * have the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateCharacterStream(int columnIndex, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value, which will
	 * have the specified number of bytes.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateCharacterStream(String columnName, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * The data will be read from the stream as needed until end-of-stream is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to
	 * the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateClob</code> which
	 * takes a length parameter.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * The data will be read from the stream as needed until end-of-stream is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to
	 * the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateClob</code> which
	 * takes a length parameter.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateClob(String columnName, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateClob(String columnName, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The data
	 * will be read from the stream as needed until end-of-stream is reached.
	 * The driver does the necessary conversion from Java character format to
	 * the national character set in the database. It is intended for use when
	 * updating <code>NCHAR</code>, <code>NVARCHAR</code> and
	 * <code>LONGNVARCHAR</code> columns.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNCharacterStream(int columnIndex, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value. The data
	 * will be read from the stream as needed until end-of-stream is reached.
	 * The driver does the necessary conversion from Java character format to
	 * the national character set in the database. It is intended for use when
	 * updating <code>NCHAR</code>, <code>NVARCHAR</code> and
	 * <code>LONGNVARCHAR</code> columns.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNCharacterStream(String columnName, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value, which will
	 * have the specified number of bytes. The driver does the necessary
	 * conversion from Java character format to the national character set in
	 * the database. It is intended for use when updating <code>NCHAR</code>,
	 * <code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNCharacterStream(int columnIndex, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a character stream value, which will
	 * have the specified number of bytes. The driver does the necessary
	 * conversion from Java character format to the national character set in
	 * the database. It is intended for use when updating <code>NCHAR</code>,
	 * <code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param reader
	 *            the <code>java.io.Reader</code> object containing the new
	 *            column value.
	 * @param length
	 *            the length of the stream.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNCharacterStream(String columnName, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.NClob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param nClob
	 *            the value for the column to be updated.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set; if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.NClob</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            name of the column.
	 * @param nClob
	 *            the value for the column to be updated.
	 * @throws SQLException
	 *             if the columnName is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set; if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(String columnName, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code>. The
	 * data will be read from the stream as needed until end-of-stream is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to
	 * the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateNClob</code>
	 * which takes a length parameter.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set, if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code>. The
	 * data will be read from the stream as needed until end-of-stream is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to
	 * the database char format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * <p>
	 * <b>Note:</b> Consult your JDBC driver documentation to determine if it
	 * might be more efficient to use a version of <code>updateNClob</code>
	 * which takes a length parameter.
	 * 
	 * @param columnName
	 *            the first column is 1, the second 2, ...
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @throws SQLException
	 *             if the columnName is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set, if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(String columnName, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code>object,
	 * which is the given number of characters long. When a very large UNICODE
	 * value is input to a <code>LONGVARCHAR</code> parameter, it may be more
	 * practical to send it via a <code>Reader</code> object. The JDBC driver
	 * will do any necessary conversion from UNICODE to the database char
	 * format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the number of characters in the parameter data.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set, if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column using the given <code>Reader</code>object,
	 * which is the given number of characters long. When a very large UNICODE
	 * value is input to a <code>LONGVARCHAR</code> parameter, it may be more
	 * practical to send it via a <code>Reader</code> object. The JDBC driver
	 * will do any necessary conversion from UNICODE to the database char
	 * format.
	 * 
	 * <p>
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * </p>
	 * 
	 * @param columnName
	 *            name of the column.
	 * @param reader
	 *            an object that contains the data to set the parameter value
	 *            to.
	 * @param length
	 *            the number of characters in the parameter data.
	 * @throws SQLException
	 *             if the columnName is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set, if a database access error
	 *             occurs or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNClob(String columnName, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>String</code> value. It is
	 * intended for use when updating <code>NCHAR</code>, <code>NVARCHAR</code>
	 * and <code>LONGNVARCHAR</code> columns. The updater methods are used to
	 * update column values in the current row or the insert row. The updater
	 * methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to
	 * update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param nString
	 *            the value for the column to be updated.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or if a database access error
	 *             occurs.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>String</code> value. It is
	 * intended for use when updating <code>NCHAR</code>, <code>NVARCHAR</code>
	 * and <code>LONGNVARCHAR</code> columns. The updater methods are used to
	 * update column values in the current row or the insert row. The updater
	 * methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to
	 * update the database.
	 * 
	 * @param columnName
	 *            name of the column.
	 * @param nString
	 *            the value for the column to be updated.
	 * @throws SQLException
	 *             if the columnName is not valid; if the driver does not
	 *             support national character sets; if the driver can detect
	 *             that a data conversion error could occur; this method is
	 *             called on a closed result set; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or if a database access error
	 *             occurs.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateNString(String columnName, String nString)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.RowId</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param x
	 *            the column value.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.RowId</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param x
	 *            the column value.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code> or this method is called on a
	 *             closed result set.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateRowId(String columnName, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.SQLXML</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * <p>
	 * If the <code>javax.xl.stream.XMLStreamWriter</code> for the
	 * <code>java.sql.SQLXML</code> object has not been closed prior to calling
	 * <code>updateSQLXML</code>, a <code>SQLException</code> will be thrown.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second 2, ...
	 * @param xmlObject
	 *            the value for the column to be updated.
	 * @throws SQLException
	 *             if the columnIndex is not valid; if a database access error
	 *             occurs; this method is called on a closed result set; the
	 *             <code>java.xml.transform.Result</code>, <code>Writer</code>
	 *             or <code>OutputStream</code> has not been closed for the
	 *             <code>SQLXML</code> object; if there is an error processing
	 *             the XML value or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>. The <code>getCause</code>
	 *             method of the exception may provide a more detailed
	 *             exception, for example, if the stream does not contain valid
	 *             XML.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Updates the designated column with a <code>java.sql.SQLXML</code> value.
	 * The updater methods are used to update column values in the current row
	 * or the insert row. The updater methods do not update the underlying
	 * database; instead the <code>updateRow</code> or <code>insertRow</code>
	 * methods are called to update the database.
	 * 
	 * <p>
	 * If the <code>javax.xml.stream.XMLStreamWriter</code> for the
	 * <code>java.sql.SQLXML</code> object has not been closed prior to calling
	 * <code>updateSQLXML</code>, a <code>SQLException</code> will be thrown.
	 * 
	 * @param columnName
	 *            the name of the column.
	 * @param xmlObject
	 *            the column value.
	 * @throws SQLException
	 *             if the columnName is not valid; if a database access error
	 *             occurs; this method is called on a closed result set; the
	 *             <code>java.xml.transform.Result</code>, <code>Writer</code>
	 *             or <code>OutputStream</code> has not been closed for the
	 *             <code>SQLXML</code> object; if there is an error processing
	 *             the XML value or the result set concurrency is
	 *             <code>CONCUR_READ_ONLY</code>. The <code>getCause</code>
	 *             method of the exception may provide a more detailed
	 *             exception, for example, if the stream does not contain valid
	 *             XML.
	 * @throws SQLFeatureNotSupportedException
	 *             if the JDBC driver does not support this method.
	 */
	public void updateSQLXML(String columnName, SQLXML xmlObject)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(
				"this method is not implemented yet.");
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. The
	 * result may be either the object found to implement the interface or a
	 * proxy for that object. If the receiver implements the interface then that
	 * is the object. If the receiver is a wrapper and the wrapped object
	 * implements the interface then that is the object. Otherwise the object is
	 * the result of calling <code>unwrap</code> recursively on the wrapped
	 * object. If the receiver is not a wrapper and does not implement the
	 * interface, then an <code>SQLException</code> is thrown.
	 * 
	 * @param iface
	 *            a class defining an interface that the result must implement.
	 * @return an object that implements the interface. May be a proxy for the
	 *         actual implementing object.
	 * @throws SQLException
	 *             if no object found that implements the interface.
	 */
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
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
	 * expensive <code>unwrap</code> calls that may fail. If this method returns
	 * true then calling <code>unwrap</code> with the same argument should
	 * succeed.
	 * 
	 * @param iface
	 *            a class defining an interface.
	 * @return true if this implements the interface or directly or indirectly
	 *         wraps an object that does.
	 * @throws SQLException
	 *             if an error occurs while determining whether this is a
	 *             wrapper for an object with the given interface.
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException(
				"this method is not implemented yet.");
	}

}
