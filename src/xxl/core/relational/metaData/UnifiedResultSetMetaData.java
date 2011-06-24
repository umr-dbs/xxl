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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import xxl.core.collections.MapEntry;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.util.metaData.MetaDataException;

/**
 * Appends a number of given ResultSetMetaData objects to each other and
 * returns a new unified ResultSetMetaData object such that all column names
 * are unique. That is when appending two ResultSetMetaData objects having
 * columns with equal names the resulting UnifiedResultSetMetaData object will
 * unify these columns and merge them to one column.
 */
public class UnifiedResultSetMetaData extends MergedResultSetMetaData {

	/**
	 * An array representing the columns of this object. Every entry consists
	 * of a list, holding the occurrences of the affected column in the wrapped
	 * ResultSetMetaData objects.
	 */
	protected List<List<int[]>> columns;
	
	/**
	 * Constructs an UnifiedResultSetMetaData object that wraps the given
	 * ResultSetMetaData objects. The constructor already performs the
	 * calculation of the UnifiedResultSetMetaData object's columns by
	 * determining he multiple occurrance of column names.
	 *
	 * @param metaData an array holding the ResultSetMetaData objects to be
	 *        appended to one ResultSetMetaData object.
	 */
	public UnifiedResultSetMetaData(ResultSetMetaData... metaData) {
		super(metaData);
		
		try {
			HashMap<String, Entry<ColumnMetaData, List<int[]>>> hashMap = new HashMap<String, Entry<ColumnMetaData, List<int[]>>>();
			for (int i = 0; i < this.metaData.length; i++)
				for (int j = 1; j <= this.metaData[i].getColumnCount(); j++) {
					String columnName = this.metaData[i].getColumnName(j).toUpperCase();
					ColumnMetaData columnMetaData = new ResultSetMetaDataColumnMetaData(this.metaData[i], j);
					Entry<ColumnMetaData, List<int[]>> list = hashMap.get(columnName);
					if (list == null)
						hashMap.put(columnName, list = new MapEntry<ColumnMetaData, List<int[]>>(columnMetaData, new ArrayList<int[]>()));
					else
						if (ColumnMetaDatas.COLUMN_METADATA_COMPARATOR.compare(list.getKey(), columnMetaData) != 0)
							throw new MetaDataException("columns named by " + columnName + " cannot be unified because of different types");
					list.getValue().add(new int[] {i, j});
				}
			columns = new ArrayList<List<int[]>>(hashMap.size());
			for (Entry<ColumnMetaData, List<int[]>> list : hashMap.values())
				columns.add(list.getValue());
			Collections.sort(columns, new Comparator<List<int[]>>() {
				public int compare(List<int[]> list1, List<int[]> list2) {
					int[] intArray1 = list1.get(0), intArray2 = list2.get(0);
					int index = intArray1[0] == intArray2[0] ? 1 : 0;
					return intArray1[index]-intArray2[index];
				}
			});
		}
		catch (SQLException sqle) {
			throw new MetaDataException("meta data cannot be constructed due to the following sql exception: " + sqle.getMessage());
		}
	}
	
	/**
	 * Constructs an UnifiedResultSetMetaData object that wraps the
	 * ResultSetMetaData objects contained by the given iteration. 
	 *
	 * @param metaData an iteration holding the ResultSetMetaData objects to be
	 *        appended to one ResultSetMetaData object.
	 */
	public UnifiedResultSetMetaData(Iterator<ResultSetMetaData> metaData) {
		this(Cursors.toFittingArray(metaData, new ResultSetMetaData[0]));
	}

	/**
	 * Returns the number of columns. The number of columns of an
	 * UnifiedResultSetMetaData object is equal to the number of the unique
	 * column names of the appended ResultSetMetaData objects.
	 *
	 * @return the number of columns.
	 * @throws SQLException if a database access error occurs.
	 */	
	@Override
	public int getColumnCount() throws SQLException {
		return columns.size();
	}

	/**
	 * Returns an iteration over the indices of the underlying
	 * ResultSetMetaData objects the given column is originated in.
	 *
	 * @param column number of the column: the first column is 1, the second is
	 *        2, ...
	 * @return returns an iteration over the indices of the underlying
	 *         ResultSetMetaData objects the given column is originated in.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public Cursor<Integer> originalMetaDataIndices(int column) throws SQLException {
		return new Mapper<int[], Integer>(
			new AbstractFunction<int[], Integer>() {
				@Override
				public Integer invoke(int[] intArray) {
					return intArray[0];
				}
			},
			columns.get(column-1).iterator()
		);
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
		for (int[] i : columns.get(column-1))
			if (i[0] == originalMetaData)
				return i[1];
		throw new MetaDataException("column " + column + " is not based on original mata data " + originalMetaData);
	}

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
	@Override
	public Cursor<int[]> originalColumnIndices(int column) throws SQLException {
		return new IteratorCursor<int[]>(columns.get(column-1).iterator());
	}
}
