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
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A Mapper invokes a given metadata mapping function on an input metadata
 * cursor array. <br />
 * The mapping function is applied to <code>n</code> input cursors at the same
 * time, that means a n-dimensional function is called and its arguments are
 * one element of each input cursor. The result is an object that is returned
 * by the mapper.<br />
 * Also a partial input is allowed with the intention to apply the mapping
 * function on less than <code>n</code> arguments (see allowPartialInput).
 * 
 * <p><b>IMPORTANT:</b> The function of the mapper is called with a list of
 * arguments.</p>
 * 
 * @see xxl.core.functions.MetaDataFunction
 */
public class Mapper extends xxl.core.cursors.mappers.Mapper<Tuple, Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the mapper.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs a Mapper object that invokes a given metadata mapping
	 * function on an input metadata cursor array. <br />
	 * The mapping function is applied to <code>n</code> input cursors at the
	 * same time, that means a n-dimensional function is called and its
	 * arguments are one element of each input cursor. The result is an object
	 * that is returned by the mapper.<br />
	 * Also a partial input is allowed with the intention to apply the mapping
	 * function on less than <code>n</code> arguments (see allowPartialInput).
	 * 
	 * <p><b>IMPORTANT:</b> The function of the mapper is called with a list of
	 * arguments.</p>
	 *
	 * @param mapping a metadata function that maps a list of objects (one
	 *        object per input cursor) to a new object (output). The function
	 *        also has to deliver the metadata for the output.
	 * @param allowPartialInput <code>true</code> if the function can be
	 *        applied if not every cursor delivers an input object.
	 * @param cursors the metadata cursors delivering the input.
	 */	
	public Mapper(MetaDataFunction<? super Tuple, ? extends Tuple, CompositeMetaData<Object, Object>> mapping, boolean allowPartialInput, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>... cursors) {
		super(mapping, allowPartialInput, cursors);
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(mapping));
	}

	/**
	 * Constructs a Mapper object that invokes a given metadata mapping
	 * function on an input metadata cursor array. <br />
	 * The mapping function is applied to <code>n</code> input cursors at the
	 * same time, that means a n-dimensional function is called and its
	 * arguments are one element of each input cursor. The result is an object
	 * that is returned by the mapper.<br />
	 * Partial input is not allowed.
	 * 
	 * <p><b>IMPORTANT:</b> The function of the mapper is called with a list of
	 * arguments.</p>
	 *
	 * @param mapping a metadata function that maps a list of objects (one
	 *        object per input cursor) to a new object (output). The function
	 *        also has to deliver the metadata for the output.
	 * @param cursors the metadata cursors delivering the input.
	 */	
	public Mapper(MetaDataFunction<? super Tuple, ? extends Tuple, CompositeMetaData<Object, Object>> mapping, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>... cursors) {
		this(mapping, false, cursors);
	}

	/**
	 * Constructs a Mapper object that invokes a given metadata mapping
	 * function on an input ResultSet and returns an iteration containing the
	 * resulting objects.
	 * 
	 * <p><b>IMPORTANT:</b> The function of the mapper is called with a list of
	 * arguments.</p>
	 *
	 * @param mapping a metadata function that maps a list of objects (one
	 *        object per input cursor) to a new object (output). The function
	 *        also has to deliver the metadata for the output.
	 * @param createTuple a function that maps a (row of the) result set to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the next, update and similar
	 *        methods of the result set!
	 * @param resultSet the result set that delivers the input relation.
	 */	
	public Mapper(MetaDataFunction<? super Tuple, ? extends Tuple, CompositeMetaData<Object, Object>> mapping, Function<? super ResultSet, ? extends Tuple> createTuple, ResultSet resultSet) {
		this(mapping, false, new ResultSetMetaDataCursor(resultSet, createTuple));
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
