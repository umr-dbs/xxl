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
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.util.metaData.MetaDataException;

/**
 * Appends a number of given ResultSetMetaData objects to each other and
 * returns a new ResultSetMetaData object. 
 */
public class AppendedResultSetMetaData extends MergedResultSetMetaData {
	
	/**
	 * An array of int value holding the column counts from the underlying
	 * wrapped ResultSetMetaData objects.
	 */
	protected int[] columnCounts;
	
	/**
	 * An int value holding the column count for the AppendedResultSetMetaData
	 * objectt.
	 */
	protected int columnCount;
	
	/**
	 * Constructs an AppendedResultSetMetaData object that wraps the given
	 * ResultSetMetaData objects. 
	 *
	 * @param metaData an array holding the ResultSetMetaData objects to be
	 *        appended to one ResultSetMetaData object.
	 */
	public AppendedResultSetMetaData(ResultSetMetaData... metaData) {
		super(metaData);
		
		try {
			columnCounts = new int[this.metaData.length];
			for (int i = 0; i < this.metaData.length; i++)
				columnCount += columnCounts[i] = metaData[i].getColumnCount();
		}
		catch (SQLException sqle) {
			throw new MetaDataException("meta data cannot be constructed due to the following sql exception: " + sqle.getMessage());
		}
	}
	
	/**
	 * Constructs an AppendedResultSetMetaData object that wraps the
	 * ResultSetMetaData objects contained by the given iteration. 
	 *
	 * @param metaData an iteration holding the ResultSetMetaData objects to be
	 *        appended to one ResultSetMetaData object.
	 */
	public AppendedResultSetMetaData(Iterator<? extends ResultSetMetaData> metaData) {
		this(Cursors.toFittingArray(metaData, new ResultSetMetaData[0]));
	}
	
	/**
	 * Returns the number of columns. The number of columns of an
	 * AppendedResultSetMetaData object is equal to the sum of the number of
	 * columns of the appended ResultSetMetaData objects.
	 *
	 * @return the number of columns.
	 * @throws SQLException if a database access error occurs.
	 */	
	@Override
	public int getColumnCount() throws SQLException {
		return columnCount;
	}
	
	/**
	 * Returns an iteration over the indices of the underlying
	 * ResultSetMetaData objects the given column is originated in. Iterates
	 * over the ResultSetMetaData objects wrapped by this
	 * AppendedResultSetMetaData object and subtracts the number of columns
	 * from the given column as long as the number of columns are greater than
	 * the given (modified) column. When the (modified) column is smaller than
	 * or equal to the number of columns the column is located in the actual
	 * ResultSetMetaData object and has the (modified) column index.
	 *
	 * @param column number of the column: the first column is 1, the second is
	 *        2, ...
	 * @return returns an iteration over the indices of the underlying
	 *         ResultSetMetaData objects the given column is originated in.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public Cursor<Integer> originalMetaDataIndices(int column) throws SQLException {
		if (1 <= column)
			for (int i = 0; i < metaData.length; i++)
				if (column > columnCounts[i])
					column -= columnCounts[i];
				else
					return new SingleObjectCursor<Integer>(column);
		throw new SQLException("the specified column " + column + " cannot be identified");
	}
	
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
	@Override
	public int originalColumnIndex(int originalMetaData, int column) throws SQLException {
		if (0 <= originalMetaData && originalMetaData < metaData.length) {
			for (int i = 0; i < originalMetaData; column -= columnCounts[i], i++);
			return 1 <= column && column <= columnCounts[originalMetaData] ? column : 0;
		}
		throw new SQLException("the specified metadata index " + originalMetaData + " cannot be identified");
	}
	
	/**
	 * Determines the original ResultSetMetaData objects and column indices, on
	 * which the specified column of this object is based. Iterates over the
	 * ResultSetMetaData objects wrapped by this AppendedResultSetMetaData
	 * object and subtracts the number of columns from the given column as long
	 * as the number of columns are greater than the given (modified) column.
	 * When the (modified) column is smaller than or equal to the number of
	 * columns the column is located in the actual ResultSetMetaData object and
	 * has the (modified) column index.
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
	@Override
	public Cursor<int[]> originalColumnIndices(int column) throws SQLException {
		if (1 <= column)
			for (int i = 0; i < metaData.length; i++)
				if (column > columnCounts[i])
					column -= columnCounts[i];
				else
					return new SingleObjectCursor<int[]>(new int[] {i, column});
		throw new SQLException("the specified column " + column + " cannot be identified");
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
	
}
