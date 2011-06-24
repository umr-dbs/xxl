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

package xxl.core.spatial.points;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.Convertable;
import xxl.core.spatial.LpMetric;

/**
 *	An "implementation" of the Point interface.
 *
 *	@see xxl.core.spatial.points.Point
 *	@see xxl.core.spatial.points.DoublePoint
 *	@see xxl.core.spatial.points.FloatPoint
 *
 */
public abstract class AbstractPoint implements Convertable, Cloneable, Point {

	/** Returns a physical copy of this Point.
	 * 
	 * @return returns a physical copy of this Point
	*/
	public abstract Object clone();
	

	/** Returns the dimensionality of this Point.
	 * 
	 * @return returns the dimensionality of this Point
	*/
	public abstract int dimensions();

	/**
	 * Reads the state (the attributes) for an object of this class from
	 * the specified data input and restores the calling object. The state
	 * of the object before calling <tt>read</tt> will be lost.<br>
	 * The <tt>read</tt> method must read the values in the same sequence
	 * and with the same types as were written by <tt>write</tt>.
	 *
	 * @param dataInput the stream to read data from in order to restore
	 *        the object.
	 * @throws IOException if I/O errors occur.
	 */
	public abstract void read (DataInput dataInput) throws IOException;

	/**
	 * Writes the state (the attributes) of the calling object to the
	 * specified data output. This method should serialize the state of
	 * this object without calling another <tt>write</tt> method in order
	 * to prevent recursions.
	 *
	 * @param dataOutput the stream to write the state (the attributes) of
	 *        the object to.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public abstract void write (DataOutput dataOutput) throws IOException;

	/** Computes the distance from this Point to any given point using the euclidean metric.
	 *
	 * @param point the reference point used to compute the distance
	 * @return returns the euclidean distance between this point and the given one
	*/
	public double distanceTo(Point point){
		return LpMetric.EUCLIDEAN.distance(this, point);
	}
	
	/** Returns the coordinate of this point in a given dimension <dim>.
	 *	
	 * @param dim dimension to get coordinate
	 * @return returns the coordinate in given dimension
	 */
	public abstract double getValue(int dim);
	
	/** Returns the internal Object representing the Point.
	 *
	 * @return returns the internal Object representing the Point
	 */
	public abstract Object getPoint();

	/** Returns a string representation of this Point.
	 *
	 *   @return returns a string representation of this Point.
	 */
	public abstract String toString();

	/** Indicates whether some other object is "equal to" this one. 
	 *
	 * @param object the reference object to compare with
	 * @return returns true if given object is equal to this one
	 */
	public boolean equals(Object object){
		Point point = (Point) object;
		for(int i=0; i<dimensions(); i++)
			if(getValue(i) != point.getValue(i))
				return false;
		return true;
	}
}
