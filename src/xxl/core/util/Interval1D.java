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

package xxl.core.util;

import java.util.Comparator;

import xxl.core.comparators.ComparableComparator;
import xxl.core.indexStructures.Descriptor;
import xxl.core.math.Maths;

/**
 * Class to implement one-dimensional intervals of any ordered data type. <p>
 *
 * The order of the basic data type is represented by means of comparators. If
 * no comparator is specified in a constructor a ComparableComparator.DEFAULT_INSTANCE
 * is used instead. <br>
 * The borders of an interval are represented in an Object-array called
 * <tt>border</tt> with <tt>length == 2</tt>.
 * That means the left border is contained in <tt>border[0]</tt> and the right
 * border is contained <tt>border[1]</tt>.
 * The borders of an interval may or may not belong to the interval, as desired.
 * This information is described in the boolean array <code>inclusive</code>.
 * If <tt>inclusive[0] == true</tt> the left border is contained in this interval. If it
 * is <tt>false</tt> the left border is not included. <tt>inclusive[1]</tt> contains
 * the same information for the right border of an interval.
 * This class provides methods to compare, union and intersect intervals, even to check
 * if an interval is contained in an interval.
 * Further it overrides the methods <tt>equals</tt>, <tt>clone</tt> and <tt>toString</tt>
 * of the superclass <tt>Object</tt>. <p>
 *
 * <b>Note:</b>
 * If an instance of this class is created in any constructor the condition if an interval
 * contains a minimum of one point is checked. An <tt>IllegalArgumentException</tt> is
 * thrown in the case when the interval does not contain any point.
 * <p>
 * <b>Example usage:</b>
 * <br><br>
 * <code><pre>
 * 	Interval1D interval1 = new Interval1D(new Integer(-20), true, new Integer(20), false);
 * 	Interval1D interval2 = new Interval1D(new Integer(1), new Integer(30));
 *
 * 	System.out.println("Interval1:");
 * 	System.out.println("\tleft border: " +interval1.border(false));
 * 	System.out.println("\tleft border included? " +interval1.includes(false));
 * 	System.out.println("\tright border: " +interval1.border(true));
 * 	System.out.println("\tright border included? " +interval1.includes(true));
 * 	System.out.println("Printed directly to output stream: " +interval1 +"\n");
 * 	System.out.println("Interval2:");
 * 	System.out.println("\tleft border: " +interval2.border(false));
 * 	System.out.println("\tleft border included? " +interval2.includes(true));
 * 	System.out.println("\tright border: " +interval2.border(true));
 * 	System.out.println("\tright border included? " +interval2.includes(true));
 * 	System.out.println("Printed directly to output stream: " +interval2 +"\n");
 * 	System.out.println("Are the intervals equal? " +interval1.equals(interval2));
 * 	System.out.println("Do the intervals overlap? " +interval1.overlaps(interval2));
 * 	System.out.println("Does interval1 contain interval2? " +interval1.contains(interval2));
 * 	System.out.println("Interval3 gets a clone of interval1.");
 * 	Interval1D interval3 = (Interval1D)interval1.clone();
 * 	System.out.println("Union of interval1 and interval2: " +interval1.union(interval2));
 * 	System.out.println("Intersection of interval3 and interval2: " +interval3.intersect(interval2));
 * </code></pre>
 * <p>
 * The following output was created:
 * <pre>
 * 	Interval1:
 * 		left border: -20
 * 		left border included? true
 * 		right border: 20
 * 		right border included? false
 * 	Printed directly to output stream: [-20,20[
 *
 * 	Interval2:
 * 		left border: 1
 * 		left border included? true
 * 		right border: 30
 * 		right border included? true
 * 	Printed directly to output stream: [1,30]
 *
 * 	Are the intervals equal? false
 * 	Do the intervals overlap? 0
 * 	Does interval1 contain interval2? false
 * 	Interval3 gets a clone of interval1.
 * 	Union of interval1 and interval2: [-20,30]
 * 	Intersection of interval3 and interval2: [1,20[
 * </pre>
 *
 * @see java.util.Comparator
 * @see xxl.core.comparators.ComparableComparator
 * @see xxl.core.indexStructures.Descriptor
 * @see java.lang.Cloneable
 */
public class Interval1D implements Descriptor {

    /**
	 * Object-array containing the left and right border.
	 * <b>Note:</b> border.length = 2
	 * border[0]: left border of the interval
	 * border[1]: right border of the interval
	 */
	protected Object [] border;

	/**
	 * The boolean array containing the information if the borders of an interval
	 * may or may not belong to this interval.
	 * <b>Note:</b> border.length = 2
	 * If inclusive[0] is <tt>true</tt> then the left border belongs to the interval,
	 * otherwise this border does not belong to this interval.
	 * If inclusive[1] is <tt>true</tt> then the right border belongs to the interval,
	 * otherwise this border does not  belong to this interval.
	 */
	protected boolean [] inclusive;

	/**
	 * The comparator defining the order of the basic data type.
	 * This comparator is used to compare two borders.
	 */
	public final Comparator comparator;

	/**
	 * Constructs a new interval by providing the left and right borders and a comparator.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param leftInclusive <tt>True</tt> iff the left border belongs to the interval.
	 * @param rightBorder The right border of the interval.
	 * @param rightInclusive <tt>True</tt> iff the right border belongs to the interval.
	 * @param comparator The comparator defining the order of the basic data type.
	 * @throws IllegalArgumentException if the interval does not contain any point.
	 */
	public Interval1D (Object leftBorder, boolean leftInclusive, Object rightBorder, boolean rightInclusive, Comparator comparator) throws IllegalArgumentException {
		int comparison = comparator.compare(leftBorder, rightBorder);

		if (comparison>0 || comparison==0 && !(leftInclusive && rightInclusive))
			throw new IllegalArgumentException("Interval does not contain any point.");
		this.border = new Object [] {leftBorder, rightBorder};
		this.inclusive = new boolean [] {leftInclusive, rightInclusive};
		this.comparator = comparator;
	}

	/**
	 * Constructs a new interval by providing the left and right borders based on a Comparable data type.
	 * A {@link xxl.core.comparators.ComparableComparator comparable comparator} is used to compare two borders.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param leftInclusive <tt>True</tt> iff the left border belongs to the interval.
	 * @param rightBorder The right border of the interval.
	 * @param rightInclusive <tt>True</tt> iff the right border belongs to the interval.
	 * @throws IllegalArgumentException if the interval does not contain any point.
	 */
	public Interval1D (Object leftBorder, boolean leftInclusive, Object rightBorder, boolean rightInclusive) throws IllegalArgumentException {
		this(leftBorder, leftInclusive, rightBorder, rightInclusive, new ComparableComparator());
	}

	/**
	 * Constructs a new closed interval by providing the left and right borders and a comparator.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param rightBorder the right border of the interval.
	 * @param comparator The comparator defining the order on the basic data type.
	 * @throws IllegalArgumentException if the interval does not contain any point.
	 */
	public Interval1D (Object leftBorder, Object rightBorder, Comparator comparator) throws IllegalArgumentException {
		this(leftBorder, true, rightBorder, true, comparator);
	}

	/**
	 * Constructs a new closed interval by providing the left and right borders based on a Comparable data type.
	 * A {@link xxl.core.comparators.ComparableComparator comparable comparator} is used to compare two borders.
	 *
	 * @param leftBorder The left border of the interval.
	 * @param rightBorder The right border of the interval.
	 * @throws IllegalArgumentException  in case if the interval does not contain any point.
	*/
	public Interval1D (Object leftBorder, Object rightBorder) throws IllegalArgumentException {
		this(leftBorder, true, rightBorder, true);
	}

	/**
	 * Constructs a new closed interval by providing a single point and a comparator.
	 * That means the interval is defined as follows: [point, point]
	 *
	 * @param point The only point the interval will contain.
	 * @param comparator The comparator defining the order on the basic data type.
	 */
	public Interval1D (Object point, Comparator comparator) {
		this(point, true, point, true, comparator);
	}

	/**
	 * Constructs a new interval by providing a single point based on a Comparable data type.
	 * A {@link xxl.core.comparators.ComparableComparator comparable comparator} is used to compare two borders.
	 *
	 * @param point The only point the interval will contain.
	 */
	public Interval1D (Object point) {
		this(point, new ComparableComparator());
	}

	/**
	 * Copy-constructor.
	 * The created new instance of <tt>Interval1D</tt> will be equal to the given interval.
	 * The clone of the interval is another interval that has exactly the
	 * same border properties and the same comparator as the current interval.
	 *
	 * @param interval The interval to be cloned.
	 */
	public Interval1D (Interval1D interval) {
		this(interval.border[0], interval.inclusive[0], interval.border[1], interval.inclusive[1], interval.comparator);
	}

	/**
	 * Constructs a new interval given a descriptor.
	 * The descriptor is casted to an Interval1D and then
	 * the copy-constructor is called.
	 * @param descriptor the given descriptor
	 */
	public Interval1D (Descriptor descriptor) {
		this((Interval1D)descriptor);
	}

	/** Returns the Comparator used by this interval to compare two	
	 *  borders.
	 * @return returns Comparator used by this interval
	 * 
	*/
	public Comparator comparator(){
		return comparator;
	}

	/**
	 * Clones this interval.
	 * The produced new <tt>Interval1D</tt> will be equal to this interval.
	 * The clone of the interval is another interval that has exactly the
	 * same border properties and the same comparator as the current interval.
	 * The copy-constructor is called.
	 * <p>Overrides the <code>clone</code> method of <code>Object</code>.
	 *
	 * @return a clone of this interval.
	 * @see #Interval1D (Interval1D interval)
	 */
	public Object clone () {
		return new Interval1D(this);
	}

	/**
	 * Returns a String representation of this interval.
	 * <p>Overrides the <code>toString</code> method of <code>Object</code>.
	 *
	 * @return the String representation of this interval.
	 */
	public String toString () {
		return (inclusive[0]?"[":"]")+border[0]+","+border[1]+((inclusive[1]?"]":"["));
	}

	/**
	 * Returns <tt>true</tt> iff the given object is an interval having the same
	 * border properties and comparator as this interval.
	 * Otherwise <tt>false</tt> is returned.
	 * <p>Overrides the <code>equals</code> method of <code>Object</code>.
	 *
	 * @param object The object, an interval, to be compared with this interval.
	 * @return Returns <tt>true</tt> if the given object is an interval having the same
	 * 		border properties and comparator as this interval.
	 *		Returns <tt>false</tt> if the given object is not an interval, the borders
	 * 		differ in any kind or the used comparators are not equal.
	 */
	public boolean equals (Object object) {
		try {
			Interval1D interval = (Interval1D)object;

			for (int i=0; i<2; i++)
				if (inclusive[i]!=interval.inclusive[i] ||
					border[i]!=interval.border[i] && !border[i].equals(interval.border[i]) && comparator.compare(border[i], interval.border[i])!=0)
					return false;
			return true;
		}
		catch (ClassCastException cce) {
			return false;
		}
		catch (NullPointerException npe) {
			return false;
		}
	}

	/**
	 * Returns the desired border of this interval.
	 *
	 * @param rightBorder Returns the right border if <tt>true</tt>.
	 * @return Returns the interval's right border is the specified parameter is <tt>true</tt>,
	 * 		otherwise (<tt>false</tt>) the interval's left border.
	 */
	public Object border (boolean rightBorder) {
		return border[rightBorder? 1: 0];
	}

	/**
	 * Checks if the specified border is included in this interval.
	 * Returns <tt>true</tt> if the desired border belongs to this interval
	 * otherwise <tt>false</tt>.
	 *
	 * @param rightBorder Examines the right border if <tt>true</tt>. If this parameter
	 * 		is <tt>false</tt> the left border is examined.
	 * @return Returns <tt>true</tt> if the specified border belongs to this interval,
	 * 		otherwise <tt>false</tt>.
	 */
	public boolean includes (boolean rightBorder) {
		return inclusive[rightBorder? 1: 0];
	}

	/**
	 * Checks whether a point is contained by this interval.
	 *
	 * @param point The point to be tested.
	 * @return Returns 0 if the point is contained by this interval,
	 * 		returns -1 if the point is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException  if the point can not be tested properly.
	 */ 
	public int contains (Object point) throws IllegalArgumentException {
		try {
			int result = 0;

			for(int i=0; i<2; i++) {
				int comparison = comparator.compare(border[i], point);

				result += (comparison!=0 || inclusive[i])? Maths.signum(comparison): 1-2*i;
			}
			return result/2;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether an interval is contained by this interval.
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for(int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0? !inclusive[i] && interval.inclusive[i]: comparison==1-2*i)
	 * 			return false;
	 * 	}
	 * 	return true;
	 * </code></pre>
	 * At first the left borders of the intervals are tested to be equal using this
	 * interval's comparator. If this is the case (<code>comparison == 0</code>)
	 * the inclusion of the left borders of intervals has to be checked.
	 * If <code>comparison == 1</code> <tt>false</tt> is returned, because the left
	 * border of the specified interval is less than the left border of this interval and
	 * so the specified interval is larger.
	 * If the left border properties are exactly equal, then the right border properties are
	 * checked in the same way.
	 *
	 * @param interval The interval to be tested.
	 * @return Returns <tt>true</tt> if this intervals contains the given interval,
	 * 		otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException if the interval can not be tested properly.
	 */
	public boolean contains (Interval1D interval) throws IllegalArgumentException {
		try {
			for(int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0? !inclusive[i] && interval.inclusive[i]: comparison==1-2*i)
					return false;
			}
			return true;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether an descriptor is contained by this interval.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #contains(Interval1D)} is called.
	 *
	 * @param descriptor The descriptor to be tested.
	 * @return Returns <tt>true</tt> if this intervals contains the given descriptor,
	 * 		otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException  if the descriptor	can not be tested properly.
	 * @see #contains(Interval1D)
	 */
	public boolean contains (Descriptor descriptor) throws IllegalArgumentException {
		return contains((Interval1D)descriptor);
	}

	/**
	 * Checks whether an interval and this interval do overlap.
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	int result = 0;
	 *
	 * 	for(int i=0; i<2; i++) {
	 * 		int comparison = comparator.compare(border[i], interval.border[1-i]);

	 * 		result += (comparison!=0 || inclusive[i] && interval.inclusive[1-i])? Math.signum(comparison): 1-2*i;
	 * 	}
	 * 	return result/2;
	 * </code></pre>
	 *
	 * @param interval The interval to be tested.
	 * @return Returns 0 if the interval and this interval do overlap,
	 * 		returns -1 if the interval is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException if the interval can not be tested properly.
	 */
	public int overlaps (Interval1D interval) throws IllegalArgumentException {
		try {
			int result = 0;

			for(int i=0; i<2; i++) {
				int comparison = comparator.compare(border[i], interval.border[1-i]);

				result += (comparison!=0 || inclusive[i] && interval.inclusive[1-i])? Maths.signum(comparison): 1-2*i;
			}
			return result/2;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether a descriptor and this interval do overlap.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #overlaps(Interval1D)} is called.
	 *
	 * @param descriptor The descriptor to be tested.
	 * @return Returns 0 if the descriptor and this interval do overlap,
	 * 		returns -1 if the descriptor is located to the right of this interval, else 1.
	 * @throws IllegalArgumentException if the descriptor can not be tested properly.
	 * @see #overlaps(Interval1D)
	 */
	public boolean overlaps (Descriptor descriptor) throws IllegalArgumentException {
		return overlaps((Interval1D)descriptor)==0;
	}

	/**
	 * Extends this interval to contain a given interval, too.
	 * The borders of this interval are changed in following way:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0)
	 * 			inclusive[i] |= interval.inclusive[i];
	 * 		else if (comparison==1-2*i) {
	 * 			inclusive[i] = interval.inclusive[i];
	 * 			border[i] = interval.border[i];
	 * 		}
	 * 	}
	 * 	return this;
	 * </code></pre>
	 * If the given interval is larger than this one, <code>border[i]</code> is set to
	 * <code>interval.border[i]</code> and <code>inclusive[i]</code> is set to
	 * <code>interval.inclusive[i]</code>. If the intervals are equal concerning their
	 * borders, i.e. <code>comparision == 0</code>, then only the inclusion of this
	 * interval's borders will possibly be set.
	 *
	 * @param interval The interval which defines the extension of this interval.
	 * @return Returns this interval, now containg the specified interval, too.
	 * @throws IllegalArgumentException if the union can not be performed properly.
	 */
	public Interval1D union (Interval1D interval) throws IllegalArgumentException {
		try {
			for (int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0)
					inclusive[i] |= interval.inclusive[i];
				else if (comparison==1-2*i) {
					inclusive[i] = interval.inclusive[i];
					border[i] = interval.border[i];
				}
			}
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Extends this interval to contain a given descriptor, too.
	 * <b>Note:</b The descriptor is casted to an Interval1D and
	 * the method {@link #union(Interval1D)} is called.
	 *
	 * @param descriptor The descriptor which defines the extension of this interval.
	 * @throws IllegalArgumentException if the union can not be performed properly.
	 * @see #union(Interval1D)
	 */
	public void union (Descriptor descriptor) throws IllegalArgumentException {
		union((Interval1D)descriptor);
	}

	/**
	 * Shrinks this interval to reflect the intersection with a given interval.
	 * An intersection can only be computed if the interval overlaps with the given interval,
	 * therefore an <tt>IllegalArgumentException</tt> is thrown, if the intervals
	 * do not overlap.
	 * If the intervals overlap, this interval is shrinked as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<2; i++) {
	 * 		int comparison = Math.signum(comparator.compare(border[i], interval.border[i]));
	 *
	 * 		if (comparison==0)
	 * 			inclusive[i] &= interval.inclusive[i];
	 * 		else if (comparison==i*2-1) {
	 * 			inclusive[i] = interval.inclusive[i];
	 * 			border[i] = interval.border[i];
	 * 		}
	 * 	}
	 * 	return this;
	 * </code></pre>
	 * If <code>comparsion == 0</code> the intervals have the same left (right) border and
	 * therefore only the inclusion of this interval's border have to be set.
	 * If this interval's left border (<tt>border[0]</tt>) is less than the given interval's
	 * left border (<tt>interval.border[0]</tt>), i.e. <code>comparsion == -1</code> the given
	 * interval's border properties are assumed by this interval.
	 * In the other case when this interval's right border (<tt>border[1]</tt>) is
	 * taller than the given interval's right border (<tt>interval.border[1]</tt>),
	 * i.e <code>comparsion == -1</code>, the given interval's borders are also
	 * assumed by this interval.
	 *
	 * @param interval The interval to be intersected with.
	 * @return This interval (shrinked).
	 * @throws IllegalArgumentException if the intersection can not be performed properly.
	 */
	public Interval1D intersect (Interval1D interval) throws IllegalArgumentException {
		if (overlaps(interval)!=0)
			throw new IllegalArgumentException("Intervals do not overlap");
		try {
			for (int i=0; i<2; i++) {
				int comparison = Maths.signum(comparator.compare(border[i], interval.border[i]));

				if (comparison==0)
					inclusive[i] &= interval.inclusive[i];
				else if (comparison==i*2-1) {
					inclusive[i] = interval.inclusive[i];
					border[i] = interval.border[i];
				}
			}
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Shrinks this interval to reflect the intersection with a given interval.
	 * An intersection can only be computed if the interval overlaps with the given interval,
	 * therefore an <tt>IllegalArgumentException</tt> is thrown, if the intervals
	 * do not overlap. <br>
	 * <b>Note:</b> The descriptor is casted to an Interval1D and
	 * the method {@link #intersect(Interval1D)} is called.
	 *
	 * @param descriptor The descriptor to be intersected with.
	 * @return This descriptor (shrinked).
	 * @throws IllegalArgumentException if the intersection cannot be performed properly.
	 * @see #intersect(Interval1D)
	 */
	public Descriptor intersect (Descriptor descriptor) throws IllegalArgumentException {
		return intersect((Interval1D)descriptor);
	}
}
