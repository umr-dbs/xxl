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
import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.MetaDataException;

/** 
 * This abstract class is a ResultSetMetaData skeleton that wraps a number of
 * given ResultSetMetaData objects to a single one.
 * 
 * This is useful especially for Joins. There are three abstract methods:
 * <ul>
 *     <li><code>int <B>getColumnCount</B>()</code></li>
 *     <li><code>Cursor <B>originalMetaDataIndices</B>(int column)</code></li>
 *     <li><code>int <B>originalColumnIndex</B>(int originalMetaData, int column)</code></li>
 * </ul>
 * 
 * These functions define a mapping between the original metadata objects and
 * an object of this class.
 * 
 * For each call to <code>getXXX(int column)</code> it is first called
 * <code>originalMetaDataIndices(column)</code> to determine the indices of the
 * ResultSetMetaData objects thie column belongs to.
 * 
 * Thereafter the <code>getXXX(int column)</code>-call is redirected to
 * <code>getXXX(originalColumnIndex(originalMetaData, column))</code> where
 * <code>originalMetaData</code> is the index obtained from the iteration
 * returned by the <code>originalMetaDataIndices(column)</code> method.<br />
 * For merged columns the behaviour of a method depends on the kind of
 * underlying column. For example <code>columnDisplaySize</code> is the maximum
 * of the <code>columnDisplaySizes</code> of the corresponding columns from all
 * underlying MetaData objects.
 */
public abstract class MergedResultSetMetaData implements ResultSetMetaData {
	
	/**
	 * An array holding the ResultSetMetaData objects to be merged.
	 */
	protected ResultSetMetaData[] metaData;
	
	/**
	 * Constructs a MergedResultSetMetaData object that wraps the given
	 * ResultSetMetaData objects. 
	 *
	 * @param metaData an array holding the ResultSetMetaData objects to be
	 *        merged.
	 */
	public MergedResultSetMetaData(ResultSetMetaData... metaData) {
		this.metaData = metaData;
	}

	/**
	 * Constructs a MergedResultSetMetaData object that wraps the
	 * ResultSetMetaData objects contained by the given iteration. 
	 *
	 * @param metaData an iteration holding the ResultSetMetaData objects to be
	 *        merged.
	 */
	public MergedResultSetMetaData(Iterator<? extends ResultSetMetaData> metaData) {
		this(Cursors.toFittingArray(metaData, new ResultSetMetaData[0]));
	}

	/**
	 * Returns the number of columns of this ResultSetMetaData object.
	 *
	 * @return the number of columns of this ResultSetMetaData object
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int getColumnCount() throws SQLException;

	/**
	 * Returns an iteration over the indices of the underlying
	 * ResultSetMetaData objects the given column is originated in. If you
	 * derive a class from this class, this method defines your mapping from
	 * the columns of the ResultSetMetaData objects to the
	 * MergedResultSetMetaData object.
	 *
	 * @param column number of the column: the first column is 1, the second is
	 *        2, ...
	 * @return returns an iteration over the indices of the underlying
	 *         ResultSetMetaData objects the given column is originated in.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract Cursor<Integer> originalMetaDataIndices(int column) throws SQLException;
	
	/**
	 * Determines the original column index from the underlying
	 * ResultSetMetaData object with the given index, on which the specified
	 * column of this object is based.
	 *
	 * @param originalMetaData the index of the underlying ResultSetMetaData
	 *        object that should be tested for being the origin of the
	 *        specified column.
	 * @param column number of the column: the first column is 1, the second is
	 *        2, ... If the given column does not originate in the specified
	 *        ResultSetMetaData object, the return value has to be 0.
	 * @return the original column number from the underlying ResultSetMetaData
	 *         object with the given index, on which the specified column of
	 *         this object is based.
	 * @throws SQLException if a database access error occurs.
	 */
	public abstract int originalColumnIndex(int originalMetaData, int column) throws SQLException;
	
	/**
	 * Determines the original ResultSetMetaData objects and column indices, on
	 * which the specified column of this object is based.
	 *
	 * @param column number of the column: the first column is 1, the second is
	 *        2, ... If the given column does not originate in the specified
	 *        ResultSetMetaData object, the return value has to be 0.
	 * @return the original ResultSetMetaData objects and column indices, on
	 *         which the specified column of this object is based. The returned
	 *         iteration contains two-dimensional <code>int</code>-arrays
	 *         holding the index of the original ResultSetMetaData object and
	 *         the index of the original column.
	 * @throws SQLException if a database access error occurs.
	 */
	public Cursor<int[]> originalColumnIndices(final int column) throws SQLException {
		return new Mapper<Integer, int[]>(
			new AbstractFunction<Integer, int[]>() {
				@Override
				public int[] invoke(Integer metaDataIndex) {
					try {
						return new int[] {
							metaDataIndex,
							originalColumnIndex(
								metaDataIndex,
								column
							)
						};
					}
					catch (SQLException sqle) {
						throw new WrappingRuntimeException(sqle);
					}
				}
			},
			originalMetaDataIndices(column)
		);
	}
	
	/**
	 * Returns the name the catalog. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the name of the catalog.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getCatalogName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getCatalogName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the name the Java class that is associated with the column
	 * type. The call is redirected to the ResultSetMetaData object the given
	 * column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the name of the column class.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getColumnClassName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getColumnClassName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the display size of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in and the
	 * maximum of all column display sizes is returned.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the display size of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public int getColumnDisplaySize(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int columnDiplaySize = Integer.MIN_VALUE;
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				columnDiplaySize = Math.max(columnDiplaySize, metaData[originalColumnIndex[0]].getColumnDisplaySize(originalColumnIndex[1]));
			}
			return columnDiplaySize;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the column label. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the name of the column label.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getColumnLabel(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getColumnLabel(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the name of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the name of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getColumnName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getColumnName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the type of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the type of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public int getColumnType(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getColumnType(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the type name of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the type name of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getColumnTypeName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getColumnTypeName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the precision of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the precision of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public int getPrecision(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getPrecision(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the scale of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the scale of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public int getScale(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getScale(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the schema name of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the schema name of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getSchemaName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getSchemaName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns the table name of the column. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return the table name of the column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public String getTableName(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return metaData[originalColumnIndex[0]].getTableName(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is an auto increment column. The call is
	 * redirected to the ResultSetMetaData object the given column is
	 * originated in. If the column is originated in more than one
	 * ResultSetMetaData object it cannot be an auto increment column.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is an auto increment column.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isAutoIncrement(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int[] originalColumnIndex = originalColumnIndices.next();
			return !originalColumnIndices.hasNext() && metaData[originalColumnIndex[0]].isAutoIncrement(originalColumnIndex[1]);
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is case sensitive. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata object, it is case
	 * sensitive, if one of the original columns is case sensitive.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is case sensitive.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isCaseSensitive(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (metaData[originalColumnIndex[0]].isCaseSensitive(originalColumnIndex[1]))
					return true;
			}
			return false;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column contains a currency value. The call is redirected
	 * to the ResultSetMetaData object the given column is originated in. If
	 * the column is originated in more than one metadata object, it is a
	 * currency value if all original values are currency values.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column contains a currency value.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isCurrency(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (!metaData[originalColumnIndex[0]].isCurrency(originalColumnIndex[1]))
					return false;
			}
			return true;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is definitively writable. The call is redirected
	 * to the ResultSetMetaData object the given column is originated in. If
	 * the column is originated in more than one metadata object, it is
	 * definitively writable if all original columns are definitively writable.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is definitively writable.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isDefinitelyWritable(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (!metaData[originalColumnIndex[0]].isDefinitelyWritable(originalColumnIndex[1]))
					return false;
			}
			return true;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is nullable. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata object, the column is
	 * nullabe if one of the underlying columns is nullable. If all underlying
	 * columns do not allow null values (<code>columnNoNulls</code>), the
	 * column also does not allow null values. In any other case, the return
	 * value is <code>columnNullableUnknown</code>.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return one of the constants defined in
	 *         {@link java.sql.ResultSetMetaData}: <code>columnNoNulls</code>,
	 *         <code>columnNullable</code> or
	 *         <code>columnNullableUnknown</code>.
	 * @throws SQLException if a database access error occurs.
	 */	
	public int isNullable(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			int isNullable = ResultSetMetaData.columnNullableUnknown;
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				int isNextNullable = metaData[originalColumnIndex[0]].isNullable(originalColumnIndex[1]);
				if (isNextNullable == ResultSetMetaData.columnNullable)
					return ResultSetMetaData.columnNullable;
				isNullable = (isNullable == ResultSetMetaData.columnNoNulls && isNextNullable == ResultSetMetaData.columnNoNulls) ?
					ResultSetMetaData.columnNoNulls :
					ResultSetMetaData.columnNullableUnknown;
			}
			return isNullable;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is read only. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata object, it is read only,
	 * if one of the original columns is read only.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is read only.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isReadOnly(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (metaData[originalColumnIndex[0]].isReadOnly(originalColumnIndex[1]))
					return true;
			}
			return false;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is searchable. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata objects, it is searchable
	 * if all original columns are searchable.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is searchable.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isSearchable(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (!metaData[originalColumnIndex[0]].isSearchable(originalColumnIndex[1]))
					return false;
			}
			return true;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is signed. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata object, it is signed, if
	 * one of the original columns is signed.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column signed.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isSigned(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (metaData[originalColumnIndex[0]].isSigned(originalColumnIndex[1]))
					return true;
			}
			return false;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}

	/**
	 * Returns if the column is writable. The call is redirected to the
	 * ResultSetMetaData object the given column is originated in. If the
	 * column is originated in more than one metadata object, it is writable if
	 * all original columns are writable.
	 * 
     * @param column number of the column: the first column is 1, the second is
     *        2, ...
	 * @return true if the column is writable.
	 * @throws SQLException if a database access error occurs.
	 */	
	public boolean isWritable(int column) throws SQLException {
		Cursor<int[]> originalColumnIndices = originalColumnIndices(column);
		if (originalColumnIndices.hasNext()) {
			while (originalColumnIndices.hasNext()) {
				int[] originalColumnIndex = originalColumnIndices.next();
				if (!metaData[originalColumnIndex[0]].isWritable(originalColumnIndex[1]))
					return false;
			}
			return true;
		}
		throw new SQLException("the specified column " + column + " cannot be identified");
	}
	
	/**
	 * Returns the metadata set that is specified by the given index.
	 * 
	 * @param input the number of the metadata set: the first metadata set is
	 *        1, the second is 2, ...
	 * @return the metadata object. 
	 */	
	protected ResultSetMetaData getMetaData(int input) {
		return metaData[input];
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
	
}
