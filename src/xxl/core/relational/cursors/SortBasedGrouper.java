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

import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.ComparatorBasedEqual;
import xxl.core.predicates.Not;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.relational.tuples.Tuples;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * The sort-based grouper is an implementation of the group operator. It is
 * based on {@link xxl.core.cursors.groupers.SortBasedGrouper}. The input
 * relation has to be sorted so that all elements belonging to the same group
 * have to be in sequence. Then, a group is defined by a predicate that returns
 * false at the end of such a sequence.
 * 
 * <p>A call to the <code>next</code> method returns a group (cursor)
 * containing all elements of a group.</p>
 * 
 * <p>Usually, a
 * {@link xxl.core.relational.cursors.GroupAggregator group-aggregator} is
 * applied on the output of a grouper.</p>
 */
public class SortBasedGrouper extends xxl.core.cursors.groupers.SortBasedGrouper<Tuple> implements MetaDataCursor<Cursor<Tuple>, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the sort-based grouper.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs an instance of the sort-based grouper.
	 *
	 * @param sortedCursor the sorted metadata cursor containing elements.
	 * @param predicate the predicate that determines the borders of the groups.
	 */
	public SortBasedGrouper(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> sortedCursor, Predicate<? super Tuple> predicate) {
		super(sortedCursor, predicate);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(sortedCursor));
	}

	/**
	 * Constructs an instance of the sort-based grouper.
	 *
	 * @param sortedCursor the sorted metadata cursor containing elements.
	 * @param columns if the values in the passed column numbers differ, a new
	 *        group is created. The first column is 1, the second is 2, ...
	 */
	public SortBasedGrouper(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> sortedCursor, int[] columns) {
		this(
			sortedCursor,
			new Not<Tuple>(
				new ComparatorBasedEqual<Tuple>(
					Tuples.getTupleComparator(columns)
				)
			)
		);
	}

	/**
	 * Constructs an instance of the sort-based grouper.
	 *
	 * @param sortedResultSet the sorted result set containing elements.
	 * @param createTuple a function that maps a (row of the) result set to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. If <code>null</code> is passed, the factory method of
	 *        array-tuple is used. It is forbidden to call the
	 *        <code>next</code>, <code>update</code> and similar methods of the
	 *        result set from inside the function!
	 * @param predicate the predicate that determines the borders of the
	 *        groups.
	 */
	public SortBasedGrouper(ResultSet sortedResultSet, Function<? super ResultSet, ? extends Tuple> createTuple, Predicate<? super Tuple> predicate) {
		this(
			createTuple == null ?
				new ResultSetMetaDataCursor(sortedResultSet) :
				new ResultSetMetaDataCursor(sortedResultSet, createTuple),
			predicate
		);
	}

	/**
	 * Constructs an instance of the sort-based grouper.
	 *
	 * @param sortedResultSet the sorted result set containing elements.
	 * @param createTuple a function that maps a (row of the) result set to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. If <code>null</code> is passed, the factory method of
	 *        array-tuple is used. It is forbidden to call the
	 *        <code>next</code>, <code>update</code> and similar methods of the
	 *        result set from inside the function!
	 * @param columns if the values in the passed column numbers differ, a new
	 *        group is created. The first column is 1, the second is 2, ...
	 */
	public SortBasedGrouper(ResultSet sortedResultSet, Function<? super ResultSet, ? extends Tuple> createTuple, int[] columns) {
		this(
			sortedResultSet,
			createTuple,
			new Not<Tuple>(
					new ComparatorBasedEqual<Tuple>(
						Tuples.getTupleComparator(columns)
					)
				)
			);
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
