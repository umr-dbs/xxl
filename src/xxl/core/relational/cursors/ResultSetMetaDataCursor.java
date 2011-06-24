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
import java.sql.SQLException;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * This class wraps a result set to a metadata cursor. ResultSet and
 * MetaDataCursor are equivalent concepts. Both handle sets of objects with
 * metadata. The different direction is done by the class
 * {@link xxl.core.relational.resultSets.MetaDataCursorResultSet}.
 */
public class ResultSetMetaDataCursor extends AbstractCursor<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The result set that is wrapped into a metadata cursor.
	 */
	protected ResultSet resultSet = null;
	
	/**
	 * The metadata for this cursor.
	 */
	protected CompositeMetaData<Object, Object> globalMetadata;
	
	/** 
	 * The function that maps a (row of the) result set to a tuple. The
	 * function gets a result set and maps the current row to a tuple. It is
	 * forbidden to call the next, update and similar methods. 
	 */
	protected Function<? super ResultSet, ? extends Tuple> createTuple;

	/**
	 * Constructs a Wrapper that wraps a result set into a metadata cursor.
	 * Therefore, a function can be passed that translates a row into an
	 * object.
	 *
	 * @param resultSet the result set that is wrapped into a metadata cursor.
	 * @param createTuple the function that maps a (row of the) result set to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the next, update and similar
	 *        methods of the result set!
	 */
	public ResultSetMetaDataCursor(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createTuple) {
		this.resultSet = resultSet;
		globalMetadata = new CompositeMetaData<Object, Object>();
		try {
			globalMetadata.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, resultSet.getMetaData());
		}
		catch (SQLException sqle) {
			throw new MetaDataException("sql exception occured during meta data construction: \'" + sqle.getMessage() + "\'");
		}
		this.createTuple = createTuple;
	}

	/**
	 * Constructs a Wrapper that wraps a result set into a metadata cursor. The
	 * rows of the result set are represented by array-tuples.
	 *
	 * @param resultSet the result set that is wrapped into a metadata cursor.
	 */
	public ResultSetMetaDataCursor(ResultSet resultSet) {
		this(resultSet, ArrayTuple.FACTORY_METHOD);
	}

	/**
	 * Returns <code>true</code> if the underlying result set has more
	 * elements. (In other words, returns <code>true</code> if
	 * <code>next</code> or <code>peek</code> would return an element rather
	 * than throwing an exception.)
	 *
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws WrappingRuntimeException if a SQLException is reported by the
	 *         underlying result set.
	 */
	@Override
	public boolean hasNextObject() {
		try {
			return resultSet.next();
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Returns the next element of the underlying result set.
	 *
	 * @return the next element of the underlying result set. The element is
	 *         mapped to an object using the createTuple function (see the
	 *         constructors).
	 * @throws java.util.NoSuchElementException if the iteration has no more
	 *         elements.
	 */
	@Override
	public Tuple nextObject() {
		return createTuple.invoke(resultSet); //Abb.: ResultSet -> Tuple
	}

	/**
	 * Closes the underlying result set. It is important to call this method
	 * when the cursor is not used any longer.
	 *
	 * @throws WrappingRuntimeException if the underlying result set reports a
	 *         SQLException.
	 */
	@Override
	public void close() {
		if (isClosed) return;
		super.close();
		try {
			resultSet.close();
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Returns the underlying result set. Be careful: calling methods of the
	 * result set may influence the metadata cursor.
	 *
	 * @return the underlying result set.
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}

	/**
	 * Removes from the underlying collection the last element returned by the
	 * cursor. This call does only work if the underlying result set supports
	 * the deleteRow-call of JDBC 2.0. This method can be called only once per
	 * call to <code>next</code> or <code>peek</code>. The behavior of a cursor
	 * is unspecified if the underlying result set is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 *
	 * @throws WrappingRuntimeException if the underlying result set reports a
	 *         SQLException.
	 */
	@Override
	public void remove() throws WrappingRuntimeException {
		super.remove();
		try {
			resultSet.deleteRow();
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Returns true if remove is supported. This method always returns
	 * <code>true</code> because this cursor supports remove. 
	 * 
	 * @return <code>true</code> if remove is supported.
	 */
	@Override
	public boolean supportsRemove() {
		return true;
	}

	/**
	 * Resets the cursor to its initial state. So the caller is able to
	 * traverse the underlying collection again. The modifications, removes and
	 * updates concerning the underlying collection, are still persistent. This
	 * call does only work if the underlying result set supports the
	 * beforeFirst-call of JDBC 2.0.
	 *
	 * @throws WrappingRuntimeException if the underlying result set reports a
	 *         SQLException.
	 */
	@Override
	public void reset() throws WrappingRuntimeException {
		super.reset();
		try {
			resultSet.beforeFirst();
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Returns <code>true</code> if reset is supported. This method always
	 * returns <code>true</code> because this cursor is considered as
	 * resetable. 
	 * 
	 * @return <code>true</code> if reset is supported.
	 */
	@Override
	public boolean supportsReset() {
		return true;
	}

	/**
	 * Replaces the object that was returned by the last call to
	 * <code>next</code> or <code>peek</code>. This operation must not be
	 * called after a call to <code>hasNext</code>. It should follow a call to
	 * <code>next</code> or <code>peek</code>. This method should be called
	 * only once per call to <code>next</code> or <code>peek</code>. The
	 * behavior is unspecified if the underlying result set is modified while
	 * the iteration is in progress in any way other than by calling this
	 * method. This call does only work if the underlying result set supports
	 * the updateObject-call of JDBC 2.0.
	 *
	 * @param tuple the object that replaces the object returned by the last
	 *        call to <code>next</code> or <code>peek</code>.
	 * @throws WrappingRuntimeException if the underlying result set reports a
	 *         SQLException.
	 */
	@Override
	public void update(Tuple tuple) throws WrappingRuntimeException {
		super.update(tuple);
		try {
			for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
				resultSet.updateObject(i, tuple.getObject(i));
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Returns <code>true</code> if update is supported. This method always
	 * returns <code>true</code> because this cursor supports update. 
	 * 
	 * @return <code>true</code> if update is supported.
	 */
	@Override
	public boolean supportsUpdate() {
		return true;
	}

	/**
	 * Returns the metadata information for this metadata cursor (the
	 * underlying result set).
	 *
	 * @return an object of the type ResultSetMetaData representing the
	 *         metadata information for this cursor.
	 * @throws WrappingRuntimeException if the underlying result set reports a
	 *         SQLException.
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetadata;
	}
}
