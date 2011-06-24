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

package xxl.core.relational.tuples;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * This class provides an extended decorator for tuples. Calls to the methods
 * of this class (<code>getXXX(int column)</code>) are redirected to the
 * wrapped instance (<code>getXXX(<b>originalColumnIndex(column)</b>)</code>).
 * Note, that the given column index of the original method call is substituded
 * by a mapped column index. This mapping, performed by the
 * <code>originalColumnIndex</code> method, can be easily used to create
 * smaller or even reordered tuples without creating a new one and storing the
 * interesting values a second time. (In the case of smaller tuples, the
 * <code>getColumnCount</code> method must also be overwritten to return the
 * correct number of remaining columns.)
 */
public class WrappedTuple extends AbstractTuple {
	
	/**
	 * The wrapped tuple calls to this class' methods are redirected to.
	 */
	protected Tuple tuple;
	
	/**
	 * Constructs a new tuple that wraps the given tuple.
	 * 
	 * @param tuple the tuple to be wrapped.
	 */
	public WrappedTuple(Tuple tuple) {
		this.tuple = tuple;
	}
	
	/**
	 * Returns the column number of the original tuple that has been mapped to
	 * the column number value that is passed to the call.
	 *
	 * @param columnIndex the column number of the mapped tuple.
	 * @return the column number of the original tuple.
	 */
	protected int originalColumnIndex(int columnIndex) {
		return columnIndex;
	}
	
	/**
	 * Returns the number of columns in this tuple.
	 *
	 * @return the number of columns.
	 */
	@Override
	public int getColumnCount() {
		return tuple.getColumnCount();
	}

	/**
	 * Column access method that corresponds to <code>getBoolean</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a boolean representation of the column object.
	 */
	@Override
	public boolean getBoolean(int columnIndex) {
		return tuple.getBoolean(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getByte</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a byte representation of the column object.
	 */
	@Override
	public byte getByte(int columnIndex) {
		return tuple.getByte(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getDate</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Date representation of the column object.
	 */
	@Override
	public Date getDate(int columnIndex) {
		return tuple.getDate(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getDouble</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a double representation of the column object.
	 */
	@Override
	public double getDouble(int columnIndex) {
		return tuple.getDouble(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getFloat</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a float representation of the column object.
	 */
	@Override
	public float getFloat(int columnIndex) {
		return tuple.getFloat(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getInt</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return an int representation of the column object.
	 */
	@Override
	public int getInt(int columnIndex) {
		return tuple.getInt(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getLong</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a long representation of the column object.
	 */
	@Override
	public long getLong(int columnIndex) {
		return tuple.getLong(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getObject</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return the column object.
	 */
	@Override
	public Object getObject(int columnIndex) {
		return tuple.getObject(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getShort</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a short representation of the column object.
	 */
	@Override
	public short getShort(int columnIndex) {
		return tuple.getShort(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getString</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a String representation of the column object.
	 */
	@Override
	public String getString(int columnIndex) {
		return tuple.getString(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getTime</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Time representation of the column object.
	 */
	@Override
	public Time getTime(int columnIndex) {
		return tuple.getTime(originalColumnIndex(columnIndex));
	}

	/**
	 * Column access method that corresponds to <code>getTimestamp</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Timestamp representation of the column object.
	 */
	@Override
	public Timestamp getTimestamp(int columnIndex) {
		return tuple.getTimestamp(originalColumnIndex(columnIndex));
	}

	/**
	 * Compares the column object to <code>null</code>.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return <code>true</code> if the column object is equal to
	 *         <code>null</code>.
	 */
	@Override
	public boolean isNull(int columnIndex) {
		return tuple.isNull(originalColumnIndex(columnIndex));
	}

}
