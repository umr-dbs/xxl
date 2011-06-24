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

package xxl.core.comparators;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;

/**
 * The ComparableComparator uses the <code>compareTo</code> method
 * of a given Comparable object to compare two elements.
 * This means that objects that implement the Comparable interface
 * are wrapped by a ComparableComparator to be used as a Comparator.
 * 
 * @param <T> the type of the objects to be compared with this comparator (must
 *        be a subtype of <code>Comparable&lt;T&gt;</code>).
 * @see java.util.Comparator
 */
public class ComparableComparator<T extends Comparable<T>> implements Comparator<T>, Serializable {

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for boolean values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Boolean> BOOLEAN_COMPARATOR = new ComparableComparator<Boolean>();
	
	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for byte values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Byte> BYTE_COMPARATOR = new ComparableComparator<Byte>();
	
	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for character values and objects. It is similar to the <i>Singleton
	 * Design Pattern</i> (for further details see Creational Patterns,
	 * Prototype in <i>Design Patterns: Elements of Reusable Object-Oriented
	 * Software</i> by Erich Gamma, Richard Helm, Ralph Johnson, and John
	 * Vlissides) except that there are no mechanisms to avoid the creation of
	 * other instances of the comparator.
	 */
	public static final ComparableComparator<Character> CHARACTER_COMPARATOR = new ComparableComparator<Character>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for double values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Double> DOUBLE_COMPARATOR = new ComparableComparator<Double>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for float values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Float> FLOAT_COMPARATOR = new ComparableComparator<Float>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for integer values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Integer> INTEGER_COMPARATOR = new ComparableComparator<Integer>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for long values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Long> LONG_COMPARATOR = new ComparableComparator<Long>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for short values and objects. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * the comparator.
	 */
	public static final ComparableComparator<Short> SHORT_COMPARATOR = new ComparableComparator<Short>();

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for string objects. It is similar to the <i>Singleton Design Pattern</i>
	 * (for further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of the
	 * comparator.
	 */
	public static final ComparableComparator<String> STRING_COMPARATOR = new ComparableComparator<String>();

	/**
	 * This instance can be used for getting a default instance of a case
	 * insensitive comparator for string objects. It is similar to the
	 * <i>Singleton Design Pattern</i> (for further details see Creational
	 * Patterns, Prototype in <i>Design Patterns: Elements of Reusable
	 * Object-Oriented Software</i> by Erich Gamma, Richard Helm, Ralph
	 * Johnson, and John Vlissides) except that there are no mechanisms to
	 * avoid the creation of other instances of the comparator.
	 */
	public static final ComparableComparator<String> CASE_INSENSITIVE_STRING_COMPARATOR = new ComparableComparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			return o1.compareToIgnoreCase(o2);
		}
		
	};

	/**
	 * This instance can be used for getting a default instance of a comparator
	 * for {@link java.sql.Date date} objects. It is similar to the
	 * <i>Singleton Design Pattern</i> (for further details see Creational
	 * Patterns, Prototype in <i>Design Patterns: Elements of Reusable
	 * Object-Oriented Software</i> by Erich Gamma, Richard Helm, Ralph
	 * Johnson, and John Vlissides) except that there are no mechanisms to
	 * avoid the creation of other instances of the comparator.
	 */
	public static final ComparableComparator<Date> DATE_COMPARATOR = new ComparableComparator<Date>();

	/** 
	 * Compares its two arguments for order. If a <tt>null</tt> value is allowed 
	 * to be passed as argument, the user has to wrap this comparator by calling 
	 * {@link Comparators#newNullSensitiveComparator(java.util.Comparator,boolean)}.
	 * <br>
	 * The exact implementation of this method is:
	 * <code><pre>
	 *   return o1.compareTo(o2);
	 * </pre></code>
	 * @param o1 the first object
	 * @param o2 the second object
	 * @return the comparison result
	 */
	public int compare(T o1, T o2) {
		return o1.compareTo(o2);
	}
}
