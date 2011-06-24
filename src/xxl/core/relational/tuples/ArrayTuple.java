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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.util.WrappingRuntimeException;

/**
 * Implements an array-tuple where the column objects are packed together in an
 * object array. This is an efficient implementation of the tuple interface
 * using arrays. For convenience, the implementation extends
 * <code>AbstractTuple</code>.
 * <code><pre>
 *   Tuple tuple = new ArrayTuple(resultSet);
 * </code></pre>
 * creates a tuple for the given result set. An object array is used to store
 * the elements.
 * <code><pre>
 *   ArrayTuple.FACTORY_METHOD.invoke(resultSet)
 * </code></pre>
 * does the same.
 */
public class ArrayTuple extends AbstractTuple implements Serializable {

	/** 
	 * A factory method that constructs objects of the class ArrayTuple. The
	 * function can be called with one or a list of parameters (corresponding
	 * to the two constructors of the class):
	 * <ul>
	 *   <li>
	 *     resultSet: a {@link java.sql.ResultSet result set} from which data
	 *     and metadata is taken.
	 *     <code><pre>
	 *       ResultSet resultSet = ...;
	 *       Tuple t = ArrayTuple.FACTORY_METHOD.invoke(resultSet);
	 *     </pre></code>
	 *   </li>
	 *   <li>
	 *     objects: a list of arguments containing the objects.<br />
	 *     Caution: tuple is linked (not copied). So, changes to the original
	 *     variables will cause changes to the tuple!
	 *     <code></pre>
	 *       List<Object> objects = ...;
	 *       Tuple t = ArrayTuple.FACTORY_METHOD.invoke(objects);
	 *     </pre></code>
	 *   </li>
	 * </ul>
	 * Exactly
	 * <ul>
	 *   <li>
	 *     <pre>ResultSet --> Tuple</pre>
	 *   </li>
	 *   <li>
	 *     <pre>List<? extends Object> --> Tuple</pre>
	 *   </li>
	 * <ul>
	 * This function can always be used when a createTuple-Function is needed.
	 */
	public static final Function<Object, ArrayTuple> FACTORY_METHOD = new AbstractFunction<Object, ArrayTuple>() {

		@Override
		public ArrayTuple invoke(Object resultSet) {
			return new ArrayTuple((ResultSet)resultSet);
		}

		@Override
		public ArrayTuple invoke(List<? extends Object> objects) {
			return new ArrayTuple(objects.toArray());
		}

	};

	/**
	 * An array containing column objects.
	 */
	protected Object[] tuple;

	/**
	 * Constructs an array-tuple containing the column objects of the current
	 * row of the result set. The metadata is also taken from the result set.
	 *
	 * @param resultSet the underlying result set.
	 * @throws WrappingRuntimeException when accessing the result set fails.
	 */
	public ArrayTuple(ResultSet resultSet) {
		try {
			ResultSetMetaData metadata = resultSet.getMetaData();
			tuple = new Object[metadata.getColumnCount()];
			for (int i = 0; i < tuple.length; i++)
				tuple[i] = resultSet.getObject(i+1);
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Constructs an array-tuple containing the column objects of an object
	 * array. The metadata is taken from the passed result set metadata object.
	 *
	 * @param tuple an object array containing column objects. Caution: the
	 *        tuple is linked (not copied). So, changes to tuple will cause
	 *        changes in the tuple!
	 */
	public ArrayTuple(Object... tuple) {
		this.tuple = tuple;
	}

	/**
	 * Returns the number of columns in this tuple.
	 *
	 * @return the number of columns.
	 */
	@Override
	public int getColumnCount() {
		return tuple.length;
	}
	
	/** 
	 * Returns the object of the given column.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return the object of the column.
	 */
	@Override
	public Object getObject(int columnIndex) {
		return tuple[columnIndex-1];
	}
	
	/**
	 * Creates and returns a copy of this object. The precise meaning of
	 * "copy" may depend on the class of the object. The general intent is
	 * that, for any object <tt>x</tt>, the expression:
	 * <pre>
	 *   x.clone() != x
	 * </pre>
	 * will be <code>true</code>, and that the expression:
	 * <pre>
	 *   x.clone().getClass() == x.getClass()
	 * </pre>
	 * will be <code>true</code>, but these are not absolute requirements.
	 * While it is typically the case that:
	 * <pre>
	 *   x.clone().equals(x)
	 * </pre>
	 * will be <code>true</code>, this is not an absolute requirement.
	 * 
	 * <p>By convention, the returned object should be obtained by calling
	 * <code>super.clone</code>. If a class and all of its superclasses (except
	 * <code>Object</code>) obey this convention, it will be the case that
	 * <code>x.clone().getClass() == x.getClass()</code>.</p>
	 * 
	 * <p>By convention, the object returned by this method should be
	 * independent of this object (which is being cloned). To achieve this
	 * independence, it may be necessary to modify one or more fields of the
	 * object returned by <tt>super.clone</tt> before returning it. Typically,
	 * this means copying any mutable objects that comprise the internal "deep
	 * structure" of the object being cloned and replacing the references to
	 * these objects with references to the copies. If a class contains only
	 * primitive fields or references to immutable objects, then it is usually
	 * the case that no fields in the object returned by
	 * <code>super.clone</code> need to be modified.</p>
	 * 
	 * <p>The method <code>clone</code> for class <code>Object</code> performs
	 * a specific cloning operation. First, if the class of this object does
	 * not implement the interface <code>Cloneable</code>, then a 
	 * <code>CloneNotSupportedException</code> is thrown. Note that all arrays 
	 * are considered to implement the interface <code>Cloneable</code>. 
	 * Otherwise, this method creates a new instance of the class of this 
	 * object and initializes all its fields with exactly the contents of the
	 * corresponding fields of this object, as if by assignment; the contents
	 * of the fields are not themselves cloned. Thus, this method performs a
	 * "shallow copy" of this object, not a "deep copy" operation.</p>
	 * 
	 * <p>The class <code>Object</code> does not itself implement the
	 * interface <code>Cloneable</code>, so calling the <code>clone</code>
	 * method on an object whose class is <code>Object</code> will result in
	 * throwing an exception at run time.</p>
	 *
	 * @return a clone of this instance.
	 * @throws CloneNotSupportedException if the object's class does not
	 *         support the <code>Cloneable</code> interface. Subclasses that
	 *         override the <code>clone</code> method can also throw this
	 *         exception to indicate that an instance cannot be cloned.
	 * @see java.lang.Cloneable
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		ArrayTuple clone = (ArrayTuple)super.clone();
		clone.tuple = tuple.clone();
		return clone;
	}
	
	/**
	 * Copies the objects of the tuple into a new object array.
	 *
	 * @return array containing the objects of the tuple
	 */
	@Override
	public Object[] toArray() {
		return tuple.clone();
	}

	/**
	 * Outputs the content of the tuple.
	 *
	 * @return a string representation of a tuple.
	 */
	@Override
	public String toString() {
		return Arrays.toString(tuple);
	}
}
