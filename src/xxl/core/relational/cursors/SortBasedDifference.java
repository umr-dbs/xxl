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

package xxl.core.relational.cursors;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Comparator;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * Operator computing the difference of two sorted metadata cursors. This class
 * uses the algorithm of
 * {@link xxl.core.cursors.differences.SortBasedDifference} and additionally
 * computes the metadata. Relational metadata is checked whether it fits.
 *
 * <p><b>Caution</b>: the input iterations have to be sorted! To sort a
 * metadata cursor use a {@link MergeSorter merge-sorter}.</p>
 */
public class SortBasedDifference extends xxl.core.cursors.differences.SortBasedDifference<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {
	
	/**
	 * The metadata provided by the sort-based difference.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;
	
	/**
	 * Constructs a sort-based difference operator (metadata cursor) that
	 * computes the difference between two sorted input metadata cursors. The
	 * parameters are the same compared to
	 * {@link xxl.core.cursors.differences.SortBasedDifference}.
	 *
	 * <p>To get a correct result, the input relations have to be sorted. To
	 * sort a metadata cursor use a {@link MergeSorter merge-sorter}.</p>
	 *
	 * <p>See also {@link xxl.core.relational.tuples.Tuples} for static methods
	 * delivering ready to use tuple comparators.</p>
	 *
	 * @param sortedInput1 the first input metadata cursor.
	 * @param sortedInput2 the second input metadata cursor.
	 * @param comparator a comparator used to compare two tuples. To get a
	 *        comparator that compares tuples after some specified columns use
	 *        one of the <code>getTupleComparator</code> in
	 *        {@link xxl.core.relational.tuples.Tuples}.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @param asc a flag showing if the input cursors have been sorted
	 *        ascending or not.
	 */
	public SortBasedDifference(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> sortedInput1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedInput2, Comparator<? super Tuple> comparator, boolean all, boolean asc) {
		super(sortedInput1, sortedInput2, comparator, all, asc);
		
		ResultSetMetaData metaData1 = ResultSetMetaDatas.getResultSetMetaData(sortedInput1);
		ResultSetMetaData metaData2 = ResultSetMetaDatas.getResultSetMetaData(sortedInput2);

		if (ResultSetMetaDatas.RESULTSET_METADATA_COMPARATOR.compare(metaData1, metaData2) != 0)
			throw new MetaDataException("the difference of the given cursors cannot be computed because they differ in their relational metadata");
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, metaData1);
	}

	/**
	 * Constructs a sort-based difference operator (metadata cursor) that
	 * computes the difference between two sorted input result sets. The result
	 * sets are wrapped to metadata cursors using
	 * {@link ResultSetMetaDataCursor}.
	 *
	 * <p>To get a correct result, the input relations have to be sorted.</p>
	 *
	 * <p>See also {@link xxl.core.relational.tuples.Tuples} for static methods
	 * delivering ready to use tuple comparators.</p>
	 *
	 * @param sortedResultSet1 the first input result set.
	 * @param sortedResultSet2 the second input result set.
	 * @param comparator a comparator used to compare two tuples. To get a
	 *        comparator that compares tuples after some specified columns use
	 *        one of the <code>getTupleComparator</code> in
	 *        {@link xxl.core.relational.tuples.Tuples}.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @param asc a flag showing if the input cursors have been sorted
	 *        ascending or not.
	 */
	public SortBasedDifference(ResultSet sortedResultSet1, ResultSet sortedResultSet2, Comparator<? super Tuple> comparator, boolean all, boolean asc) {
		this(new ResultSetMetaDataCursor(sortedResultSet1), new ResultSetMetaDataCursor(sortedResultSet2), comparator, all, asc);
	}

	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
