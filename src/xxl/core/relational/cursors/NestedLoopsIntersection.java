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

import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.relational.JoinUtils;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * Operator that computes an intersection using the nested-loops paradigm. 
 */
public class NestedLoopsIntersection extends xxl.core.cursors.intersections.NestedLoopsIntersection<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * An internal variable used for storing the metadata information of this
	 * intersect operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs a new nested-loops intersection.
	 * 
	 * @param input0 the input metadata cursor that is traversed in the "outer"
	 *        loop.
	 * @param input1 the input metadata cursor that is traversed in the "inner"
	 *        loop.
	 * @param newCursor a function without parameters that delivers a new
	 *        metadata cursor, when the cursor <code>input1</code> cannot be
	 *        reset, i.e., <code>input1.reset()</code> will cause an
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param equals the binary predicate evaluated for each tuple of elements
	 *        backed on one element of each input iteration in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        intersection operation.
	 */
	public NestedLoopsIntersection(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input0, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input1, Function<?, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newCursor, Predicate<? super Tuple> equals) {
		super(input0, input1, newCursor, equals);
		
		ResultSetMetaData metaData0 = ResultSetMetaDatas.getResultSetMetaData(input0);
		ResultSetMetaData metaData1 = ResultSetMetaDatas.getResultSetMetaData(input1);

		if (ResultSetMetaDatas.RESULTSET_METADATA_COMPARATOR.compare(metaData0, metaData1) != 0)
			throw new MetaDataException("the difference of the given cursors cannot be computed because they differ in their relational metadata");
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, metaData0);
	}
	
	/**
	 * Constructs a new nested-loops intersection.
	 * 
	 * @param input0 the input metadata cursor that is traversed in the "outer"
	 *        loop.
	 * @param input1 the input metadata cursor that is traversed in the "inner"
	 *        loop.
	 * @param newCursor a function without parameters that delivers a new
	 *        metadata cursor, when the cursor <code>input1</code> cannot be
	 *        reset, i.e., <code>input1.reset()</code> will cause an
	 *        {@link java.lang.UnsupportedOperationException}.
	 */
	public NestedLoopsIntersection(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input0, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input1, Function<?, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newCursor) {
		this(input0, input1, newCursor, JoinUtils.naturalJoinMetaDataPredicate(ResultSetMetaDatas.getResultSetMetaData(input0), ResultSetMetaDatas.getResultSetMetaData(input1)));
	}
	
	/**
	 * Constructs a new nested-loops intersection.
	 * 
	 * @param resultSet0 the input result set that is traversed in the "outer"
	 *        loop.
	 * @param resultSet1 the input result set that is traversed in the "inner"
	 *        loop.
	 * @param newResultSet a function without parameters that delivers a new
	 *        result set, when the iterating over the "inner" result set
	 *        <code>resultSet1</code> cannot be reset, i.e.,
	 *        <code>input1.reset()</code> will cause an
	 *        {@link java.lang.UnsupportedOperationException}.
	 */
	public NestedLoopsIntersection(ResultSet resultSet0, ResultSet resultSet1, final Function<?, ? extends ResultSet> newResultSet) {
		this(
			new ResultSetMetaDataCursor(resultSet0),
			new ResultSetMetaDataCursor(resultSet1),
			new AbstractFunction<Object, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>>>() {
				@Override
				public MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> invoke() {
					return new ResultSetMetaDataCursor(newResultSet.invoke());
				}
			}
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
