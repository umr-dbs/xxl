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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.ProjectedResultSetMetaData;
import xxl.core.relational.metaData.RenamedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * This class computes the cross table of an input metadata cursor. All data of
 * the table is stored inside maps in main memory. The best way to describe the
 * function will be an example:<br />
 * the dataset
 * <table border>
 *   <tr><th>A</th><th>B</th><th>C</th></tr>
 *   <tr><td>a<sub>0</sub></td><td>b<sub>0</sub></td><td>c<sub>0</sub></td></tr>
 *   <tr><td>a<sub>0</sub></td><td>b<sub>1</sub></td><td>c<sub>1</sub></td></tr>
 *   <tr><td>a<sub>1</sub></td><td>b<sub>0</sub></td><td>c<sub>2</sub></td></tr>
 *   <tr><td>a<sub>1</sub></td><td>b<sub>1</sub></td><td>c<sub>3</sub></td></tr>
 * </table>
 * can be transformed into (first dimension is given by column "A", second
 * dimension is given by column "B" and tuples are reduced to the value of
 * column "C")
 * <table border>
 *   <tr><th><sub>A</sub>\<sup>B</sup></th><th>b<sub>0</sub></th><th>b<sub>1</sub></th></tr>
 *   <tr><td>a<sub>0</sub></td></td><td>c<sub>0</sub></td><td>c<sub>1</sub></td></tr>
 *   <tr><td>a<sub>1</sub></td></td><td>c<sub>2</sub></td><td>c<sub>3</sub></td></tr>
 * </table>
 * 
 * @param <D> the type of the data structure that is internally used for
 *        storing the aggregated tuples that have identical values for the
 *        columns defining the dimensions of the cross table.
 */
public class CrossTableCursor<D> extends AbstractCursor<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {
	
	/**
	 * A function that maps an instance of the internally used data structure
	 * to an object representing it. The function must be able to provide
	 * relational metadata that is used as relational metadata for the columns
	 * defined by the second dimension.
	 */
	protected MetaDataFunction<? super D, ? extends Object, CompositeMetaData<Object, Object>> aggregate;

	/**
	 * A function that constructs a tuple out of a list of objects.
	 */
	protected Function<Object, ? extends Tuple> tupleFactory;

	/**
	 * An internally used data structure storing the different values defining
	 * the cross table's second dimension.
	 */
	protected Set<Object> secondDimension;

	/**
	 * The metadata provided by the cross table.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * An iteration containing the tuples of the cross table.
	 */
	protected Iterator<Entry<Object, Map<Object, D>>> result;

	/**
	 * Constructs a new cross table that transforms the cursor's data using the
	 * given column names as table dimensions. The given tuple aggregate is
	 * used for reducing the tuples to an object representing their features of
	 * interest an storing it in an instance of the specified data structure
	 * (acting as aggregation value). The given aggregate transforms an
	 * instance of the specified datastructure storing objects having the same
	 * values in the specified columns to a single object. This cursor has to
	 * perform nearly all of its computations inside the constructor (and not
	 * inside <code>open</code>), because the computation of the metadata
	 * already needs nearly the whole computation (the number of columns
	 * depends on the data).
	 * 
	 * @param cursor the metadata cursor providing the input to be processed.
	 * @param firstDimensionColumnName the name of the column providing the
	 *        first dimension of the cross table (the rows).
	 * @param secondDimensionColumnName the name of the column providing the
	 *        second dimension of the cross table (from which the column values
	 *        are taken).
	 * @param tupleAggregate an aggregation function that is called for every
	 *        tuple of the input cursor. The aggregation function has to reduce
	 *        the given tuple to an object representing the features of
	 *        interest and returns an instance of the specified data structure
	 *        storing this objects as aggregation value.
	 * @param aggregate a metadata function mapping instances of the specified
	 *        datastructure storing objects having the same values in the
	 *        specified columns to a single object. The relational metadata
	 *        provided by this function defines the relational metadata of the
	 *        columns given by the second dimension.
	 * @param tupleFactory a function that constructs a tuple out of a list of
	 *        objects.
	 */
	public CrossTableCursor(
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor,
		String firstDimensionColumnName,
		String secondDimensionColumnName,
		AggregationFunction<? super Tuple, D> tupleAggregate,
		MetaDataFunction<? super D, ? extends Object, CompositeMetaData<Object, Object>> aggregate,
		Function<Object, ? extends Tuple> tupleFactory
	) {
		this.aggregate = aggregate;
		this.tupleFactory = tupleFactory;
		
		// Some initializations
		ResultSetMetaData resultSetMetaData = ResultSetMetaDatas.getResultSetMetaData(cursor);
		int firstDimensionColumnIndex, secondDimensionColumnIndex;
		try {
			firstDimensionColumnIndex = ResultSetMetaDatas.getColumnIndex(resultSetMetaData, firstDimensionColumnName);
			secondDimensionColumnIndex = ResultSetMetaDatas.getColumnIndex(resultSetMetaData, secondDimensionColumnName);
			
		}
		catch (SQLException sqle) {
			throw new MetaDataException("the metadata of the underlying cursor cannot be accessed because of the following reason : " + sqle.getMessage());
		}
		
		// Computing the cross table
		Map<Object, Map<Object, D>> firstDimensionMap = new HashMap<Object, Map<Object, D>>();
		secondDimension = new TreeSet<Object>();
		while (cursor.hasNext()) {
			Tuple tuple = cursor.next();
			Object firstDimensionValue = tuple.getObject(firstDimensionColumnIndex);
			Object secondDimensionValue = tuple.getObject(secondDimensionColumnIndex);
			
			secondDimension.add(secondDimensionValue);
			
			Map<Object, D> secondDimensionMap = firstDimensionMap.get(firstDimensionValue);
			if (secondDimensionMap == null) {
				secondDimensionMap = new HashMap<Object, D>();
				firstDimensionMap.put(firstDimensionValue, secondDimensionMap);
			}
			
			D d = secondDimensionMap.get(secondDimensionValue);
			secondDimensionMap.put(secondDimensionValue, tupleAggregate.invoke(d, tuple));
		}
		cursor.close();
		
		// Computing the metadata
		ResultSetMetaData[] resultSetMetaDatas = new ResultSetMetaData[secondDimension.size() + 1];
		resultSetMetaDatas[0] = new ProjectedResultSetMetaData(resultSetMetaData, firstDimensionColumnIndex);
		Iterator<Object> objects = secondDimension.iterator();
		for (int i = 1; i < resultSetMetaDatas.length; i++)
			resultSetMetaDatas[i] = new RenamedResultSetMetaData(ResultSetMetaDatas.getResultSetMetaData(aggregate), (String)null, String.valueOf(objects.next()));
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, new AppendedResultSetMetaData(resultSetMetaDatas));
		result = firstDimensionMap.entrySet().iterator();
	}

	/**
	 * Determines if there is a next object in the iteration.
	 * 
	 * @see AbstractCursor#hasNextObject()
	 */
	@Override
	protected boolean hasNextObject() {
		return result.hasNext();
	}

	/**
	 * Retrieves the next object of the iteration.
	 * 
	 * @see AbstractCursor#nextObject()
	 */
	@Override
	protected Tuple nextObject() {
		Entry<Object, Map<Object, D>> entry = result.next();
		Map<Object, D> secondDimensionMap = entry.getValue();
		List<Object> newTuple = new ArrayList<Object>();
		newTuple.add(entry.getKey());
		for (Object value : secondDimension)
			newTuple.add(aggregate.invoke(secondDimensionMap.get(value)));
		return tupleFactory.invoke(newTuple);
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
