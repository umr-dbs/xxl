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

import xxl.core.collections.sweepAreas.SweepAreaImplementor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.relational.JoinUtils;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * An operator that computes an intersection using the sort-merge paradigm. The
 * input iterations have to be sorted according to the comparator that is
 * turned over.
 */
public class SortBasedIntersection extends xxl.core.cursors.intersections.SortBasedIntersection<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {
	
	/**
	 * The metadata provided by the sort-based intersection.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;
	
	/**
	 * Creates a new sort-based intersection operator backed on two sorted
	 * input iterations using the given sweep-area to store the elements of the
	 * first input iteration and probe with the elements of the second one for
	 * matchings.
	 * 
	 * @param sortedCursor0 the first sorted metadata cursor to be intersected.
	 * @param sortedCursor1 the second sorted metadata cursor to be
	 *        intersected.
	 * @param impl the sweep-area implementor used for storing elements of the
	 *        first sorted input iteration (<code>sortedInput0</code>).
	 * @param comparator the comparator that is used for comparing elements of
	 *        the two sorted input iterations.
	 */
	public SortBasedIntersection(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor0, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, SweepAreaImplementor<Tuple> impl, Comparator<? super Tuple> comparator) {
		super(sortedCursor0, sortedCursor1, impl, comparator, JoinUtils.naturalJoinMetaDataPredicate(ResultSetMetaDatas.getResultSetMetaData(sortedCursor0), ResultSetMetaDatas.getResultSetMetaData(sortedCursor1)));
		
		ResultSetMetaData metaData0 = ResultSetMetaDatas.getResultSetMetaData(sortedCursor0);
		ResultSetMetaData metaData1 = ResultSetMetaDatas.getResultSetMetaData(sortedCursor1);

		if (ResultSetMetaDatas.RESULTSET_METADATA_COMPARATOR.compare(metaData0, metaData1) != 0)
			throw new MetaDataException("the difference of the given cursors cannot be computed because they differ in their relational metadata");
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, metaData0);
	}
	
	/**
	 * Creates a new sort-based intersection operator backed on two sorted
	 * input result sets using the given sweep-area to store the elements of
	 * the first input result set and probe with the elements of the second one
	 * for matchings.
	 * 
	 * @param sortedResultSet0 the first sorted result set to be intersected.
	 * @param sortedResultSet1 the second sorted result set to be intersected.
	 * @param impl the sweep-area implementor used for storing elements of the
	 *        first sorted input result set (<code>sortedInput0</code>).
	 * @param comparator the comparator that is used for comparing elements of
	 *        the two sorted input result set.
	 */
	public SortBasedIntersection(ResultSet sortedResultSet0, ResultSet sortedResultSet1, SweepAreaImplementor<Tuple> impl, Comparator<? super Tuple> comparator) {
		this(new ResultSetMetaDataCursor(sortedResultSet0), new ResultSetMetaDataCursor(sortedResultSet1), impl, comparator);
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
