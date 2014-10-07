/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
 * of Mathematics and Computer Science University of Marburg Germany
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * http://code.google.com/p/xxl/
 */

package xxl.core.relational.tuples;

import java.util.Arrays;

public class ColumnComparableArrayTuple extends ArrayTuple
    implements
      ColumnComparableTuple {

  /**
   * An array containing column objects.
   */
  protected Object[] tuple;

  /**
   * Constructs an array-tuple containing the column objects of an object array. The meta data is
   * taken from the passed result set meta data object. <b>Please note</b>: If you want to use a
   * single column or a set of columns you have to <b>ensure</b> that the containing objects
   * implements {@link java.lang.Comparable}. Otherwise an <code>RuntimeException</code> will be
   * thrown.
   * 
   * 
   * @param array an object array containing column objects. Caution: the tuple is linked (not
   *        copied). So, changes to tuple will cause changes in the tuple!
   */
  public ColumnComparableArrayTuple(Object... array) {
    for (Object c : array)
      if (c instanceof ColumnComparableArrayTuple) {
        this.tuple = ((ColumnComparableArrayTuple) c).toComparableArray();
        return;
      } else if (c instanceof Tuple) {
        this.tuple = (Comparable[]) c;
        return;
      }
    this.tuple = array;
  }

  /**
   * Constructs an array-tuple containing the column objects of an object array. The meta data is
   * taken from the passed result set meta data object.
   * 
   * @param template an object array containing column objects. Caution: the tuple is linked (not
   *        copied). So, changes to tuple will cause changes in the tuple!
   */
  public ColumnComparableArrayTuple(Tuple template) {
    Comparable destination[] = new Comparable[template.getColumnCount()];
    Object source[] = template.toArray();

    for (int i = 0; i < source.length; i++) {
      destination[i] = (Comparable) source[i];
    }
    this.tuple = destination;
  }

  /**
   * Creates and returns a copy of this object. The precise meaning of "copy" may depend on the
   * class of the object. The general intent is that, for any object <tt>x</tt>, the expression:
   * 
   * <pre>
	 * x.clone() != x
	 * </pre>
   * 
   * will be <code>true</code>, and that the expression:
   * 
   * <pre>
	 * x.clone().getClass() == x.getClass()
	 * </pre>
   * 
   * will be <code>true</code>, but these are not absolute requirements. While it is typically the
   * case that:
   * 
   * <pre>
	 * x.clone().equals(x)
	 * </pre>
   * 
   * will be <code>true</code>, this is not an absolute requirement.
   * 
   * <p>
   * By convention, the returned object should be obtained by calling <code>super.clone</code>. If a
   * class and all of its superclasses (except <code>Object</code>) obey this convention, it will be
   * the case that <code>x.clone().getClass() == x.getClass()</code>.
   * </p>
   * 
   * <p>
   * By convention, the object returned by this method should be independent of this object (which
   * is being cloned). To achieve this independence, it may be necessary to modify one or more
   * fields of the object returned by <tt>super.clone</tt> before returning it. Typically, this
   * means copying any mutable objects that comprise the internal "deep structure" of the object
   * being cloned and replacing the references to these objects with references to the copies. If a
   * class contains only primitive fields or references to immutable objects, then it is usually the
   * case that no fields in the object returned by <code>super.clone</code> need to be modified.
   * </p>
   * 
   * <p>
   * The method <code>clone</code> for class <code>Object</code> performs a specific cloning
   * operation. First, if the class of this object does not implement the interface
   * <code>Cloneable</code>, then a <code>CloneNotSupportedException</code> is thrown. Note that all
   * arrays are considered to implement the interface <code>Cloneable</code>. Otherwise, this method
   * creates a new instance of the class of this object and initializes all its fields with exactly
   * the contents of the corresponding fields of this object, as if by assignment; the contents of
   * the fields are not themselves cloned. Thus, this method performs a "shallow copy" of this
   * object, not a "deep copy" operation.
   * </p>
   * 
   * <p>
   * The class <code>Object</code> does not itself implement the interface <code>Cloneable</code>,
   * so calling the <code>clone</code> method on an object whose class is <code>Object</code> will
   * result in throwing an exception at run time.
   * </p>
   * 
   * @return a clone of this instance.
   * @throws CloneNotSupportedException if the object's class does not support the
   *         <code>Cloneable</code> interface. Subclasses that override the <code>clone</code>
   *         method can also throw this exception to indicate that an instance cannot be cloned.
   * @see java.lang.Cloneable
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    ColumnComparableArrayTuple clone =
        (ColumnComparableArrayTuple) super.clone();
    clone.tuple = tuple.clone();
    return clone;
  }

  /**
   * Compares this tuple to another by comparing each component, see {@link java.lang.Comparable
   * Comparable}. <br/>
   * <b>Note</b> If the dimension of this tuple an <b>other</b> does not match and both are equal in
   * their shared components, than this tuple is greater than <b>other</b> if the length of
   * <b>other</b> is greater than the length of this tuple. Otherwise <b>other</b> is greater. <br/>
   * <br/>
   * <b>Example</b>
   * 
   * <pre>
	 * ComparableTuple t1 = new ComparableArrayTuple(new Integer(50), new Integer(30), new Integer(10));
	 * ComparableTuple t2 = new ComparableArrayTuple(new Integer(50), new Integer(30));
	 * System.out.println(t1.compareTo(t2)); // outputs &quot;1&quot;
	 * </pre>
   * 
   * @param other the second <b>ComparableTuple</b>
   */
  @SuppressWarnings("rawtypes")
  @Override
  public int compareTo(Object other) {

    Comparable[] first =
        new ColumnComparableArrayTuple(tuple).toComparableArray();
    Comparable[] second =
        ((ColumnComparableArrayTuple) other).toComparableArray();

    for (int i = 0; i < Math.min(first.length, second.length); i++) {
      @SuppressWarnings("unchecked")
      int compare = first[i].compareTo(second[i]);
      if (compare != 0) return compare;
    }
    if (first.length < second.length)
      return -1;
    else if (first.length > second.length)
      return 1;
    else
      return 0;
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
    return tuple[columnIndex - 1];
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

  @Override
  public Comparable[] toComparableArray() {

    try {
      Comparable[] result = new Comparable[tuple.length];
      for (int i = 0; i < tuple.length; i++)
        result[i] = (Comparable) tuple[i];
      return result;

    } catch (Exception e) {
      throw new RuntimeException(
          "The given tuple does not contain comparable objects at all.");
    }
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
