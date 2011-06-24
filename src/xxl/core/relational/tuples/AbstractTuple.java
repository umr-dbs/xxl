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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * This class is a skeleton-implementation for a tuple.
 * 
 * <p>Tuples are equal, if they have the same number of columns and they are
 * equal on each column value.</p>
 * 
 * <p>Two column values <code>x</code> and <code>y</code> are equal, if they
 * are both <code>null</code> or they are both not <code>null</code> and
 * <code>x.equals(y)</code> holds.</p>
 * 
 * <p>The <code>getXXX</code> methods retrieve the values with the given index
 * from the internal list, and return the casted result.</p>
 */
public abstract class AbstractTuple implements Tuple {

	/**
	 * Returns the number of columns in this tuple.
	 *
	 * @return the number of columns.
	 */
	public abstract int getColumnCount();
	
	/**
	 * Provides access to the packed objects in the tuple. An abstract method
	 * that has to be overwritten to construct a non-abstract tuple.
	 *
 	 * @param columnIndex the first column is 1, the second is 2, ...
 	 * @return column object
	 * @see xxl.core.relational.tuples.ArrayTuple
	 * @see xxl.core.relational.tuples.ListTuple
	 */
	public abstract Object getObject(int columnIndex);

	/**
	 * Column access method that corresponds to <code>getBoolean</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a boolean representation of the column object.
	 */
	public boolean getBoolean(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			false :
			o instanceof Boolean ?
				((Boolean)o).booleanValue() :
				o instanceof Number ?
					((Number)o).doubleValue() != 0 :
					o instanceof BigDecimal ?
						((BigDecimal)o).signum() != 0 :
						Boolean.parseBoolean(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getByte</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a byte representation of the column object.
	 */
	public byte getByte(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Byte ?
				((Byte)o).byteValue() :
				Byte.parseByte(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getDate</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Date representation of the column object.
	 */
	public Date getDate(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			null :
			o instanceof Date ?
				(Date)o :
				Date.valueOf(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getDouble</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a double representation of the column object.
	 */
	public double getDouble(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Double ?
				((Double)o).doubleValue() :
				Double.parseDouble(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getFloat</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a float representation of the column object.
	 */
	public float getFloat(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Float ?
				((Float)o).floatValue() :
				Float.parseFloat(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getInt</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return an int representation of the column object.
	 */
	public int getInt(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Integer ?
				((Integer)o).intValue() :
				Integer.parseInt(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getLong</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a long representation of the column object.
	 */
	public long getLong(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Long ?
				((Long)o).longValue() :
				Long.parseLong(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getShort</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a short representation of the column object.
	 */
	public short getShort(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			0 :
			o instanceof Short ?
				((Short)o).shortValue() :
				Short.parseShort(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getString</code> in
	 * {@link java.sql.ResultSet java.sql.ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a String representation of the column object.
	 */
	public String getString(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			null :
			o.toString();
	}

	/**
	 * Column access method that corresponds to <code>getTime</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Time representation of the column object.
	 */
	public Time getTime(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			null :
			o instanceof Time ?
				(Time)o :
				Time.valueOf(o.toString());
	}

	/**
	 * Column access method that corresponds to <code>getTimestamp</code> in
	 * {@link java.sql.ResultSet ResultSet}.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a Timestamp representation of the column object.
	 */
	public Timestamp getTimestamp(int columnIndex) {
		Object o = getObject(columnIndex);
		return o == null ?
			null :
			o instanceof Timestamp ?
				(Timestamp)o :
				Timestamp.valueOf(o.toString());
	}

	/**
	 * Compares the column object to <code>null</code>.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return <code>true</code> if the column object is equal to
	 *         <code>null</code>.
	 */
	public boolean isNull(int columnIndex) {
		return getObject(columnIndex) == null;
	}

	/**
	 * Compares two tuples.
	 * 
	 * <p>Tuples are equal, if they have the same number of columns and they
	 * are equal on each column value.</p>
	 *
	 * <p>Two column values <code>x</code> and <code>y</code> are equal, if
	 * they are both <code>null</code> or they are both not <code>null</code>
	 * and <code>x.equals(y)</code> holds.</p>
 	 *
 	 * @param object the object with which the current object is compared
 	 * @return <code>true</code> if the two objects are equal in the sense
 	 *         explained above.
 	 */
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof Tuple))
			return false;
		Tuple tuple = (Tuple)object;
		if (getColumnCount() != tuple.getColumnCount())
			return false;
		Object o1, o2;
		for (int i = 1; i <= getColumnCount(); i++) {
			o1 = getObject(i);
			o2 = tuple.getObject(i);
			if (o1 == null ^ o2 == null || o1 != null && !o1.equals(o2))
				return false;
		}
		return true;
	}

	/**
	 * Returns the hash code value for this tuple.
	 * 
	 * @return the hash code value for this tuple.
	 */
	@Override
	public int hashCode() {
		int hashCode = 1;
		for (int i = 0; i < getColumnCount(); i++) {
			Object object = getObject(i+1);
			hashCode = 31*hashCode + (object == null ? 0 : object.hashCode());
		}
		return hashCode;
	}
	
	/**
	 * Copies the objects of the tuple into a new object array.
	 *
	 * @return array containing the objects of the tuple
	 */
	public Object[] toArray() {
		Object[] result = new Object[getColumnCount()];
		for (int i = 0; i < getColumnCount(); i++)
			result[i] = getObject(i+1);
		return result;
	}

	/**
	 * Outputs the content of the tuple.
	 *
	 * @return a string representation of a tuple.
	 */
	@Override
	public String toString() {
		if (getColumnCount() == 0)
			return "[]";
		
		StringBuilder string = new StringBuilder().append('[').append(getObject(1));
		
		for (int i = 2; i <= getColumnCount(); i++)
			string.append(", ").append(getObject(i));
		
		return string.append(']').toString();
	}
}
