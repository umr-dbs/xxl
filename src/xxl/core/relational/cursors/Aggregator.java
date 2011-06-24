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
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.functions.MetaDataAggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.StatefulAverage;
import xxl.core.math.statistics.parametric.aggregates.Count;
import xxl.core.math.statistics.parametric.aggregates.CountAll;
import xxl.core.math.statistics.parametric.aggregates.Maximum;
import xxl.core.math.statistics.parametric.aggregates.Minimum;
import xxl.core.math.statistics.parametric.aggregates.StatefulStandardDeviation;
import xxl.core.math.statistics.parametric.aggregates.Sum;
import xxl.core.math.statistics.parametric.aggregates.StatefulVariance;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.ColumnMetaDatas;
import xxl.core.relational.metaData.ProjectedResultSetMetaData;
import xxl.core.relational.metaData.RenamedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.relational.tuples.Tuple;
import xxl.core.relational.tuples.Tuples;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * The aggregator computes aggregates like SUM, COUNT and VARIANCE for an input
 * metadata cursor. This aggregator is based on the implementation of
 * {@link xxl.core.cursors.mappers.Aggregator}.
 * 
 * <p>Some standard aggregates are defined in this class, but new functionality
 * can simply be included by using your own {@link MetaDataFunction}.</p>
 *
 * <p>Important: an aggregator executes no group- or sort-operations! For this
 * task a {@link GroupAggregator group-aggreagtion} should be used.</p>
 * 
 * @see GroupAggregator
 */
public class Aggregator extends xxl.core.cursors.mappers.Aggregator<Tuple, Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the count-aggregate. The column name is set to <code>COUNT</code> and
	 * its precision is 20.
	 */
	public static final MetaDataAggregationFunction<Object, Long, CompositeMetaData<Object, Object>> COUNT = new MetaDataAggregationFunction<Object, Long, CompositeMetaData<Object, Object>>(new Count()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, 20, "COUNT", "COUNT", "", 20, 0, "", "", Types.BIGINT, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the countAll-aggregate. The column name is set to <code>COUNT_ALL</code>
	 * and its precision is 20.
	 */
	public static final MetaDataAggregationFunction<Object, Long, CompositeMetaData<Object, Object>> COUNT_ALL = new MetaDataAggregationFunction<Object, Long, CompositeMetaData<Object, Object>>(new CountAll()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, 20, "COUNT_ALL", "COUNT_ALL", "", 20, 0, "", "", Types.BIGINT, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the sum-aggregate. The column name is set to <code>SUM</code> and its
	 * precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> SUM = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new Sum()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "SUM", "SUM", "", 52, 0, "", "", Types.DOUBLE, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the min-aggregate. The column name is set to <code>MIN</code> and its
	 * precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> MIN = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new Minimum()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "MIN", "MIN", "", 52, 0, "", "", Types.NUMERIC, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the max-aggregate. The column name is set to <code>MAX</code> and its
	 * precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> MAX = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new Maximum()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "MAX", "MAX", "", 52, 0, "", "", Types.NUMERIC, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the average-aggregate. The column name is set to <code>AVG</code> and
	 * its precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> AVG = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new StatefulAverage()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "AVG", "AVG", "", 52, 0, "", "", Types.DOUBLE, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggreagte that can be used as aggregate function to compute
	 * the standard-deviation-aggregate. The column name is set to
	 * <code>STDDEV</code> and its precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> STDDEV = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new StatefulStandardDeviation()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "STDDEV", "STDDEV", "", 52, 0, "", "", Types.DOUBLE, true, false, false));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * A metadata aggregate that can be used as aggregate function to compute
	 * the variance-aggregate. The column name is set to <code>VAR</code> and
	 * its precision is 52.
	 */
	public static final MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>> VAR = new MetaDataAggregationFunction<Number, Number, CompositeMetaData<Object, Object>>(new StatefulVariance()) {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
 		{
			globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 52, "VAR", "VAR", "", 52, 0, "", "", Types.DOUBLE, true, false, false));
		}
		
 		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * Composes every aggregation function specified in the given array with a
	 * function that projects an input tuple to the value of the column
	 * specified by the given <tt>int</tt> array.
	 * 
	 * @param aggregates an array of aggregation functions that are not able to
	 *        process tuples.
	 * @param aggregatedColumns an array holding the indices of the columns
	 *        that should be used as arguments of specified aggregation
	 *        functions.
	 * @return an array of aggregation functions that are able to process
	 *         tuples. When the <tt>n</tt>-th returned aggregation function is
	 *         invoked with an input tuple, the tuple is projected to its
	 *         <tt>n</tt>-th specified column and the <tt>n</tt>-th specified
	 *         aggregation function is invoked with it.
	 */
	@SuppressWarnings("unchecked")
	protected static MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] getTupleAggregationFunctions(MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, int[] aggregatedColumns) {
		if (aggregates.length != aggregatedColumns.length)
			throw new MetaDataException("the number of specified aggregate functions and the column number does not match");
		
		MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] composedAggregates = new MetaDataAggregationFunction[aggregates.length];
		for (int i = 0; i < aggregates.length; i++)
			composedAggregates[i] = aggregates[i].compose(Tuples.getObjectFunction(aggregatedColumns[i]));
		return composedAggregates;
	}
	
	/**
	 * The metadata provided by the aggregator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new aggregator.
	 *
	 * @param cursor the metadata cursor delivering the input elements.
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
	public Aggregator(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		super(
			cursor,
			new AggregationFunction<Tuple, Tuple>() {
				protected boolean[] initialized = new boolean[aggregates.length];

				@Override
				public Tuple invoke(Tuple aggregate, Tuple tuple) {
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
					return createOutputTuple.invoke(result);
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
		catch (SQLException sqle) {
			throw new MetaDataException("sql exception occured during meta data construction: \'" + sqle.getMessage() + "\'");
		}
	}

	/**
	 * Creates a new aggregator.
	 *
	 * @param cursor the metadata cursor delivering the input elements.
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
	public Aggregator(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(
			cursor,
			getTupleAggregationFunctions(aggregates, aggregatedColumns),
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
		catch (SQLException sqle) {
			throw new MetaDataException("sql exception occured during meta data construction: \'" + sqle.getMessage() + "\'");
		}
	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param cursor the metadata cursor delivering the input elements.
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public Aggregator(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(cursor, aggregates, new String[aggregates.length], projectedColumns, createOutputTuple);
	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param cursor the metadata cursor delivering the input elements.
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
	public Aggregator(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(cursor, aggregates, aggregatedColumns, new String[aggregatedColumns.length], projectedColumns, createOutputTuple);
	}

	/**
	 * Creates a new aggregator.
	 *
	 * @param resultSet the result set delivering the input elements. It is
	 *        wrapped internally to a metadata cursor
	 *        ({@link ResultSetMetaDataCursor}).
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
	public Aggregator(ResultSet resultSet, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(new ResultSetMetaDataCursor(resultSet), aggregates, aggregateColumnNames, projectedColumns, createOutputTuple);
 	}

	/**
	 * Creates a new aggregator.
	 *
	 * @param resultSet the result set delivering the input elements. It is
	 *        wrapped internally to a metadata cursor
	 *        ({@link ResultSetMetaDataCursor}).
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
	public Aggregator(ResultSet resultSet, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, String[] aggregateColumnNames, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(new ResultSetMetaDataCursor(resultSet), aggregates, aggregatedColumns, aggregateColumnNames, projectedColumns, createOutputTuple);
 	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param resultSet the result set delivering the input elements. It is
	 *        wrapped internally to a metadata cursor
	 *        ({@link ResultSetMetaDataCursor}).
	 * @param aggregates an array of metadata aggregates that each computes an
	 *        aggregate on the input data.
	 * @param projectedColumns the columns to which the input metadata cursor
	 *        is projected.
	 * @param createOutputTuple a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 */
	public Aggregator(ResultSet resultSet, final MetaDataAggregationFunction<Tuple, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(resultSet, aggregates, new String[aggregates.length], projectedColumns, createOutputTuple);
	}

	/**
	 * Creates a new aggregator. The column names of the aggregates are
	 * determined by the aggregate functions.
	 *
	 * @param resultSet the result set delivering the input elements. It is
	 *        wrapped internally to a metadata cursor
	 *        ({@link ResultSetMetaDataCursor}).
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
	public Aggregator(ResultSet resultSet, final MetaDataAggregationFunction<Object, Object, CompositeMetaData<Object, Object>>[] aggregates, final int[] aggregatedColumns, final int[] projectedColumns, final Function<Object, ? extends Tuple> createOutputTuple) {
		this(resultSet, aggregates, aggregatedColumns, new String[aggregatedColumns.length], projectedColumns, createOutputTuple);
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
