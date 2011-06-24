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

import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * Straight forward implementation of the operator selection.
 * 
 * <p>Example:
 * <code><pre>
 *   CompositeMetaData&lt;Object, Object&gt; metadata = new CompositeMetaData&lt;Object, Object&gt;();
 *   metadata.add(
 *       ResultSetMetaDatas.RESULTSET_METADATA_TYPE,
 *       new StoredResultSetMetaData(
 *           new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, 9, "", "", "", 9, 0, "", "", INTEGER, true, false, false)
 *       )
 *   );
 *   
 *   MetaDataCursor&lt;Tuple, CompositeMetaData&lt;Object, Object&gt;&gt; cursor = Cursors.wrapToMetaDataCursor(
 *       Tuples.mapObjectsToTuples(
 *           new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(1000), 100)
 *       ),
 *       metadata
 *   );
 *   
 *   cursor = new Selection(
 *       cursor,
 *       new Predicate&lt;Tuple&gt;() {
 *           public boolean invoke(Tuple tuple) {
 *               int value = tuple.getInt(1);
 *               return value &ge; 100 && value &le; 200;
 *           }
 *       }
 *   );
 *   
 *   cursor.open();
 *   
 *   Cursors.println(cursor);
 *   
 *   cursor.close();
 * </pre></code>
 * Wraps a RandomIntegers cursor (100 integers up to 1000) to a metadata cursor
 * using
 * {@link xxl.core.cursors.Cursors#wrapToMetaDataCursor(java.util.Iterator, Object)}. 
 * Then, all integers between 100 and 200 are selected.</p>
 */
public class Selection extends Filter<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the selection.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;
	
	/**
	 * Creates a new instance of selection.
	 *
	 * @param cursor the input metadata cursor delivering the input element. 
	 * @param predicate an unary predicate that has to determine if an element
	 *        qualifies for the result.
	 */
	public Selection(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, Predicate<? super Tuple> predicate) {
		super(cursor, predicate);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new instance of selection.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createTuple a function that maps a (row of the) result set to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. If <code>null</code> is passed, the factory method of
	 *        {@link xxl.core.relational.tuples.ArrayTuple} is used. It is forbidden
	 *        to call the <code>next</code>, <code>update</code> and similar
	 *        methods of the result set from inside the function!
	 * @param predicate an unary predicate that has to determine if an element
	 *        qualifies for the result.
	 */
	public Selection(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createTuple, Predicate<? super Tuple> predicate) {
		this(new ResultSetMetaDataCursor(resultSet, createTuple), predicate);
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
