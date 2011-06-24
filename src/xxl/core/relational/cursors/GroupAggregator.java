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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.functions.MetaDataAggregationFunction;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.ColumnMetaDatas;
import xxl.core.relational.metaData.ProjectedResultSetMetaData;
import xxl.core.relational.metaData.RenamedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * The aggregator computes aggregates like SUM, COUNT and VARIANCE for a
 * grouped input metadata cursor, i.e., an input cursor delivering groups
 * (cursors) of input elements. This aggregator is based on the implementation
 * of {@link xxl.core.cursors.mappers.Aggregator}.
 * 
 * <p>Some standard aggregates are defined in the class {@link Aggregator}, but
 * new functionality can simply be included by using your own
 * {@link MetaDataFunction}.</p>
 *
 * <p>Important: a group-aggregator executes no group- or sort-operations!
 * Normally, a {@link SortBasedGrouper} or
 * {@link xxl.core.cursors.groupers.SortBasedGrouper} is applied before the
 * aggregator is used.</p>
 */
public class GroupAggregator extends xxl.core.cursors.mappers.Aggregator<Cursor<? extends Tuple>, Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the aggregator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new aggregator.
	 *
	 * @param cursor the metadata cursor delivering the groups (cursors) of
	 *        input elements.
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param aggregateColumnNames the column names of the aggregate columns.
	 *        This array must have the same length as the number of given
	 *        aggregates. If an entry equals <code>null</code>, the name
	 *        determined by the aggregate function is taken.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public GroupAggregator(MetaDataCursor<? extends Cursor<? extends Tuple>, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		super(
			cursor,
			new AggregationFunction<Cursor<? extends Tuple>, Tuple>() {
				protected boolean[] initialized;

				@Override
				public Tuple invoke(Tuple aggregate, Cursor<? extends Tuple> cursor) {
					initialized = new boolean[aggregates.length];
					while (cursor.hasNext()) {
						Tuple tuple = cursor.next();
						ArrayList<Object> result = new ArrayList<Object>(projectedColumns.length + aggregates.length);
						for (int projectedColumn : projectedColumns)
							result.add(tuple.getObject(projectedColumn));
						for (int i = 0; i < aggregates.length; i++) {
							Object agg = initialized[i] ? aggregate.getObject(projectedColumns.length + i + 1) : null;
							result.add(
								aggregates[i].invoke(
									agg,
									tuple
								)
							);
							if (!initialized[i] && result.get(projectedColumns.length + i) != null)
								initialized[i] = true;
						}
						aggregate = createOutputTuple.invoke(result);
					}
					return aggregate;
				}
			}
		);
		
		try {
			if (aggregates.length != aggregateColumnNames.length)
				throw new MetaDataException("the number of specified aggregate functions and the new column names does not match");
			
			ResultSetMetaData resultSetMetaData = ResultSetMetaDatas.getResultSetMetaData(cursor);

			ColumnMetaData[] columnMetaDatas = new ColumnMetaData[aggregates.length];
			for (int i = 0; i < aggregates.length; i++) {
				columnMetaDatas[i] = ColumnMetaDatas.getColumnMetaData(aggregates[i]);
				if (aggregateColumnNames[i] == null)
					aggregateColumnNames[i] = columnMetaDatas[i].getColumnName();
			}

			globalMetaData = new CompositeMetaData<Object, Object>();
			globalMetaData.add(
				ResultSetMetaDatas.RESULTSET_METADATA_TYPE,
				new AppendedResultSetMetaData(
					new ProjectedResultSetMetaData(resultSetMetaData, projectedColumns),
					new RenamedResultSetMetaData(
						new ColumnMetaDataResultSetMetaData(
							columnMetaDatas
						),
						aggregateColumnNames
					)
				)
			);
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Creates a new aggregator.
	 *
	 * @param cursor the metadata cursor delivering the groups (cursors) of
	 *        input elements.
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param aggregatedColumns the column indices determining which aggregate
	 *        function is applied on which column. This array must have the
	 *        same length as the number of given aggregates.
	 * @param aggregateColumnNames the column names of the aggregate columns.
	 *        This array must have the same length as the number of given
	 *        aggregates. If an entry equals <code>null</code>, the name
	 *        determined by the aggregate function is taken.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public GroupAggregator(MetaDataCursor<? extends Cursor<? extends Tuple>, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(
			cursor,
			Aggregator.getTupleAggregationFunctions(aggregates, aggregatedColumns),
			aggregateColumnNames,
			projectedColumns,
			createOutputTuple
		);
		
		try {
			ResultSetMetaData resultSetMetaData = ResultSetMetaDatas.getResultSetMetaData(cursor);

			int columnCount = resultSetMetaData.getColumnCount();
			for (int i = 0; i < aggregatedColumns.length; i++)
				if (aggregatedColumns[i] < 1 || columnCount < aggregatedColumns[i])
					throw new MetaDataException("the specified column " + aggregatedColumns[i] + " cannot be aggregated because it does not exist in the underlying meta data");
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param cursor the metadata cursor delivering the groups (cursors) of
	 *        input elements.
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public GroupAggregator(MetaDataCursor<? extends Cursor<? extends Tuple>, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(cursor, aggregates, new String[aggregates.length], projectedColumns, createOutputTuple);
	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param cursor the metadata cursor delivering the groups (cursors) of
	 *        input elements.
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param aggregatedColumns the column indices determining which aggregate
	 *        function is applied on which column. This array must have the
	 *        same length as the number of given aggregates.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public GroupAggregator(MetaDataCursor<? extends Cursor<? extends Tuple>, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(cursor, aggregates, aggregatedColumns, new String[aggregatedColumns.length], projectedColumns, createOutputTuple);
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
