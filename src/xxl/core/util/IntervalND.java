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

/**
 * Class to implement multi-dimensional intervals based on
 * n intervals of class Interval1D. <p>
 * The order of the basic data type of each dimension
 * is represented by means of comparators. If
 * no comparator for a dimension is specified in a constructor
 * a ComparableComparator.DEFAULT_INSTANCE is used instead.
 * <p>
 * <b>IMPORTANT:</b> If two intervals should be checked for equality, overlapping,
 * intersection or containment these two intervals must be based on
 * the same dimension and also have to use exactly
 * the same co-ordinate system.
 * That means the ith dimension of the two intervals must have the
 * same basic data type. It is necessary, because these
 * operations work in terms
 * of dimensions, i.e. the appertaining method of the class
 * Interval1D will be called. If the basic type of one
 * dimension is not the same, that is the case if the two
 * intervals use different co-ordinate systems, then an
 * <tt>IllegalArgument</tt> exception will be thrown, because
 * the operation could not be performed properly.
 * <p>
 * <b>Example usage:</b>
 * <br><br>
 * <code><pre>
 * 	AbstractIntervalND interval1 = new AbstractIntervalND (new Interval1D[] {
 * 		new Interval1D (new Integer(-25), true, new Integer(25), false),
 * 		new Interval1D (new Character('a'), new Character('z')),
 * 		new Interval1D (new Float(0.75), new Float(13.125)),
 * 	});
 *
 * 	AbstractIntervalND interval2 = new AbstractIntervalND (new Interval1D[] {
 * 		new Interval1D (new Integer(-10), new Integer(10)),
 * 		new Interval1D (new Character('e'), false, new Character('u'), false),
 * 		new Interval1D (new Float(-1.25), true, new Float(7.625), false),
 * 	});
 *
 * 	System.out.println("interval1: " +interval1);
 * 	System.out.println("interval2: " +interval2);
 * 	System.out.println("Are the intervals equal? " +interval1.equals(interval2));
 * 	System.out.println("Do the intervals overlap? " +interval1.overlaps(interval2));
 * 	System.out.println("Does interval1 contain interval2? " +interval1.contains(interval2));
 * 	System.out.println("Interval3 gets a clone of interval1.");
 * 	AbstractIntervalND interval3 = (AbstractIntervalND)interval1.clone();
 * 	System.out.println("Union of interval1 and interval2: " +interval1.union(interval2));
 * 	System.out.println("Intersection of interval3 and interval2: " +interval3.intersect(interval2));
 * </code></pre>
 * <p>
 * The following output was created:
 * <pre>
 * 	interval1: {[-25,25[; [a,z]; [0.75,13.125]}
 * 	interval2: {[-10,10]; ]e,u[; [-1.25,7.625[}
 *
 * 	Are the intervals equal? false
 * 	Do the intervals overlap? true
 * 	Does interval1 contain interval2? false
 * 	Interval3 gets a clone of interval1.
 * 	Union of interval1 and interval2: {[-25,25[; [a,z]; [-1.25,13.125]}
 * 	Intersection of interval3 and interval2: {[-10,10]; ]e,u[; [0.75,7.625[}
 * </pre>
 *
 * @see xxl.core.util.Interval1D
 * @see java.lang.Cloneable
 */
public class IntervalND implements Cloneable {

	/** Array containing n one-dimensional intervals. */
	protected Interval1D [] intervals;

	/**
	 * Constructs a new n-dimensional interval using the given array
	 * of one-dimensional intervals.
	 * @param intervals array of one-dimensional intervals
	 */
	public IntervalND (Interval1D [] intervals) {
		this.intervals = intervals;
	}

	/**
	 * Returns a String representation of this interval.
	 * <p>Overrides the <code>toString</code> method of <code>Object</code>.
	 *
	 * @return the String representation of this interval.
	 */
	public String toString () {
		String result = "{";

		for (int i=0; i<dimensions(); i++) {
			result += intervals()[i].toString();
			result = i<dimensions()-1 ? result+"; " : result;
		}
		return result+"}";
	}

	/**
	 * Clones this interval.
	 * The produced new <tt>AbstractIntervalND</tt> will be equal to this interval.
	 * The clone of the interval is another interval that has exactly the
	 * same border properties and the same comparators as the current interval.
	 * <p>Overrides the <code>clone</code> method of <code>Object</code>.
	 * In particular, all one-dimensional intervals are cloned, too.
	 *
	 * @return a clone of this interval.
	 * @see xxl.core.util.Interval1D#clone()
	 */
	public Object clone () {
		try {
			IntervalND clone = (IntervalND)super.clone();

			clone.intervals = new Interval1D[intervals.length];
			for (int i=0; i<intervals.length; i++)
				clone.intervals[i] = (Interval1D)intervals[i].clone();
			return clone;
		}
		catch (CloneNotSupportedException cnse) {
		}
		return null;
	}

	/**
	 * Returns <tt>true</tt> iff the given object is a multi-dimensional interval
	 * of the same dimension and equal one-dimensional intervals. <br>
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	AbstractIntervalND interval = (AbstractIntervalND)object;
	 *
	 * 	if (dimensions()!=interval.dimensions())
	 * 		return false;
	 * 	for (int i=0; i<tt><</tt>dimensions(); i++)
	 * 		if (!intervals()[i].equals(interval.intervals()[i]))
	 * 			return false;
	 * 	return true;
	 * </code></pre>
	 * Becaues n-dimensional intervals (AbstractIntervalND) are designed as n one-dimensional intervals
	 * (Interval1D) the method <code>equals</code> in class Interval1D is called for every
	 * one-dimenonsial interval with the intention to compare each one-dimensional interval of
	 * this object with that of the given object.
	 *
	 * @param object The object, an n-dimensional interval, to be compared with this interval.
	 * @return Returns <tt>true</tt> if the given object is an n-dimensional interval
	 * 		having the same dimension as this interval and also each one-dimensional interval
	 * 		has to be equal to this one-dimensional intervals.
	 * 		Returns <tt>false</tt> if the given object is not an n-dimensional interval,
	 * 		or one of the basic one-dimensional intervals differs in one border property of
	 * 		the one-dimensional intervals of this interval.
	 * @see xxl.core.util.Interval1D#equals(Object)
	 */
	public boolean equals (Object object) {
		try {
			IntervalND interval = (IntervalND)object;

			if (dimensions()!=interval.dimensions())
				return false;
			for (int i=0; i<dimensions(); i++)
				if (!intervals()[i].equals(interval.intervals()[i]))
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
	 * Returns the internal array of one-dimensional intervals used to
	 * design an n-dimensional array.
	 *
	 * @return The internal array containing the one-dimensional intervals.
	 */
	public Interval1D [] intervals () {
		return intervals;
	}

	/**
	 * Returns the number of dimensions of this interval.
	 * That means <code>intervals.length</code> is returned.
	 *
	 * @return This interval's dimension.
	 */
	public int dimensions () {
		return intervals.length;
	}

	/**
	 * Checks whether a n-dimensional point is contained by this interval. <br>
	 * <b>Note:</b> The point to be checked has to be an object array with
	 * the same length (dimension) as this n-dimensional interval calling
	 * this method, otherwise an <tt>IllegalArgumentException</tt> will be thrown. <br>
	 *
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<tt><</tt>intervals.length; i++)
	 * 		if (intervals[i].contains(point[i])!=0)
	 * 			return false;
	 * 	return true;
	 * </code></pre>
	 * For each one-dimensional interval of this AbstractIntervalND the condition, if the
	 * component of the specified point (<tt>point[i]</tt>) is contained,
	 * is checked. So the result is only <tt>true</tt> if each dimension contains
	 * the appertaining component of the specified n-dimensional point.
	 *
	 * @param point The point to be tested.
	 * @return Returns <tt>true</tt> iff the point is contained by this interval,
	 * 		otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException if the point can not be tested properly.
	 * @see xxl.core.util.Interval1D#contains(Object point)
	 */
	public boolean contains (Object [] point) throws IllegalArgumentException {
		try {
			if (dimensions()!=point.length)
				throw new IllegalArgumentException("Point and interval do not have the same dimension.");
			for (int i=0; i<intervals.length; i++)
				if (intervals[i].contains(point[i])!=0)
					return false;
			return true;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether a specified n-dimensional interval is contained
	 * by this n-dimensional interval. <br>
	 * <b>Note:</b> The intervals must have the same dimension,
	 * otherwise an <tt>IllegalArgumentException</tt> will be thrown. <br>
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<tt><</tt>intervals.length; i++)
	 * 		if (!intervals[i].contains(multiDimInterval.intervals[i]))
	 * 			return false;
	 * 	return true;
	 * </code></pre>
	 * Because the intervals have the same dimension the method
	 * {@link xxl.core.util.Interval1D#contains(Interval1D)} is called
	 * for each dimension. If each one dimensional interval that
	 * belongs to the AbstractIntervalND, that called this method, covers
	 * the same dimension belonging to the given interval
	 * <tt>multiDimInterval</tt> the result is <tt>true</tt>,
	 * otherwise the result is <tt>false</tt>, because one dimension
	 * of the given interval is not contained by the interval
	 * that called this method.
	 *
	 * @param multiDimInterval The interval to be tested.
	 * @return Returns <tt>true</tt> iff the given interval is contained
	 * 		by this interval.
	 * @throws IllegalArgumentException if the interval can not be tested properly.
	 * @see xxl.core.util.Interval1D#contains(Interval1D)
	 */
	public boolean contains (IntervalND multiDimInterval) throws IllegalArgumentException {
		try {
			if (dimensions()!=multiDimInterval.dimensions())
				throw new IllegalArgumentException("Intervals do not have the same dimension.");
			for (int i=0; i<intervals.length; i++)
				if (!intervals[i].contains(multiDimInterval.intervals[i]))
					return false;
			return true;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks whether an interval and this interval do overlap. <br>
	 * <b>Note:</b> The intervals must have the same dimension,
	 * otherwise an <tt>IllegalArgumentException</tt> will be thrown. <br>
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<tt><</tt>intervals.length; i++)
	 * 		if (intervals[i].overlaps(multiDimInterval.intervals[i])!=0)
	 * 			return false;
	 * 	return true;
	 * </code></pre>
	 * Two intervals do only overlap,i.e. <tt>true</tt> is returned,
	 * if they overlap in each dimension,
	 * therefore {@link xxl.core.util.Interval1D#overlaps(Interval1D)} is
	 * called to compare every dimension.
	 *
	 * @param multiDimInterval The interval to be tested.
	 * @return Returns <tt>true</tt> iff the interval and this interval do overlap.
	 * @throws IllegalArgumentException if the given interval cannot be tested properly.
	 */
	public boolean overlaps (IntervalND multiDimInterval) throws IllegalArgumentException {
		try {
			if (dimensions()!=multiDimInterval.dimensions())
				throw new IllegalArgumentException("Intervals do not have the same dimension.");
			for (int i=0; i<intervals.length; i++)
				if (intervals[i].overlaps(multiDimInterval.intervals[i])!=0)
					return false;
			return true;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Extends this interval to contain a given interval, too. <br>
	 * <b>Note:</b> The intervals must have the same dimension,
	 * otherwise an <tt>IllegalArgumentException</tt> will be thrown. <br>
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<tt><</tt>intervals.length; i++)
	 * 		intervals[i].union(multiDimInterval.intervals[i]);
	 * 	return this;
	 * </code></pre>
	 * Each dimension of the interval calling this method is extended by
	 * calling {@link xxl.core.util.Interval1D#union(Interval1D)} using the
	 * same dimension of the given interval <tt>multiDimInterval</tt>.
	 *
	 * @param multiDimInterval The interval which defines the extension of this interval.
	 * @return The interval calling this method.
	 * @throws IllegalArgumentException if the union can not be performed properly.
	 */
	public IntervalND union (IntervalND multiDimInterval) throws IllegalArgumentException {
		try {
			if (dimensions()!=multiDimInterval.dimensions())
				throw new IllegalArgumentException("Intervals do not have the same dimension.");
			else {
				for (int i=0; i<intervals.length; i++)
					intervals[i].union(multiDimInterval.intervals[i]);
			}
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Shrinks this interval to reflect the intersection with a given interval.
	 * <b>Note:</b> The intervals must have the same dimension,
	 * and have to overlap, otherwise an <tt>IllegalArgumentException</tt>
	 * will be thrown. <br>
	 * The implementation is as follows:
	 * <br><br>
	 * <code><pre>
	 * 	for (int i=0; i<tt><</tt>intervals.length; i++)
	 * 		intervals[i].intersect(multiDimInterval.intervals[i]);
	 * 	return this;
	 * </code></pre>
	 * The intersection is applied to every dimension of the two
	 * intervals by invoking the method {@link xxl.core.util.Interval1D#intersect(Interval1D)}
	 * on this interval's ith component with the appertaining ith component of
	 * the specified array.
	 *
	 * @param multiDimInterval The interval to be intersected with.
	 * @return The interval calling this method.
	 * @throws IllegalArgumentException if the intersection cannot be performed properly.
	 */
	public IntervalND intersect (IntervalND multiDimInterval) throws IllegalArgumentException {
		if (!overlaps(multiDimInterval))
			throw new IllegalArgumentException("Intervals do not overlap.");
		try {
			for (int i=0; i<intervals.length; i++)
				intervals[i].intersect(multiDimInterval.intervals[i]);
			return this;
		}
		catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}
}
