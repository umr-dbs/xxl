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

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A tuple is a hard copy (materialization) of an (existing) tuple of a
 * result set.
 * 
 * <p>The <code>getXXX</code> methods retrieve the column values like result
 * set does. You only can retrieve values using the index number of the column.
 * Columns are numbered from 1.</p>
 * 
 * <p>A tuple throws only <code>RuntimeExceptions</code>. If a column is
 * requested, that does not exist, an <code>IndexOutOfBoundsException</code> is
 * thrown.</p>
 * 
 * <p>Empty tuples are not allowed. By creating a tuple from a result set, it
 * must be ensured, that the result set is not empty. If so, the constructor of
 * the Tuple-class throws a <code>RuntimeException</code>.</p>
 */
public interface Tuple extends Cloneable, Serializable {

	/**
	 * Returns the number of columns in this tuple.
	 *
	 * @return the number of columns.
	 */
	public int getColumnCount();
	
	/**
	 * Column access method that corresponds to <code>getBoolean</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a boolean representation of the column object.
	 */
	public boolean getBoolean(int columnIndex);
	
	/**
	 * Column access method that corresponds to <code>getByte</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a byte representation of the column object.
	 */
	public byte getByte(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getDate</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Date representation of the column object.
	 */
	public Date getDate(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getDouble</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a double representation of the column object.
	 */
	public double getDouble(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getFloat</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a float representation of the column object.
	 */
	public float getFloat(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getInt</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return an int representation of the column object.
	 */
	public int getInt(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getLong</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a long representation of the column object.
	 */
	public long getLong(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getObject</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return the column object.
	 */
	public Object getObject(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getShort</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a short representation of the column object.
	 */
	public short getShort(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getString</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a String representation of the column object.
	 */
	public String getString(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getTime</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Time representation of the column object.
	 */
	public Time getTime(int columnIndex);
	
	/**
	 * Column access method that corresponds to <code>getTimestamp</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Timestamp representation of the column object.
	 */
	public Timestamp getTimestamp(int columnIndex);

	/**
	 * Compares the column object to <code>null</code>.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return <code>true</code> if the column object is equal to
	 *         <code>null</code>.
	 */
	public boolean isNull(int columnIndex);
	
	/**
	 * Copies the objects of the tuple into a new object array.
	 *
	 * @return array containing the objects of the tuple
	 */
	public Object[] toArray();
	
}
