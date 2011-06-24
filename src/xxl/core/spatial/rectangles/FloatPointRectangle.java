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

package xxl.core.spatial.rectangles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import xxl.core.indexStructures.Descriptor;
import xxl.core.spatial.points.FloatPoint;
import xxl.core.spatial.points.Point;

/**
	A high-dimensional Rectangle (=hyper-cube)
    extends the abstract class Rectangle
    the value at each dimension should be of the type "float"
	@see xxl.core.spatial.rectangles.Rectangle
	@see xxl.core.spatial.rectangles.DoublePointRectangle
	@see xxl.core.spatial.rectangles.FixedPointRectangle
*/
public class FloatPointRectangle implements Rectangle {
	
	/**
	 * lower-left corner of the rectangle
	 */
	protected float[] leftCorner;
	
	/**
	 * upper-right corner of the rectangle
	 */
	protected float[] rightCorner;

	/** Creates a new FloatPointRectangle given leftCorner and rightCorner as FloatPoints
	 * 
	 * @param leftCorner FloatPoint representing lower-left corner of the rectangle
	 * @param rightCorner FloatPoint representing upper-right corner of the rectangle
	*/
	public FloatPointRectangle(FloatPoint leftCorner, FloatPoint rightCorner) {
		if (leftCorner.dimensions() != rightCorner.dimensions())
			throw new IllegalArgumentException("dimensions must be equal!");
		this.leftCorner = (float[])leftCorner.getPoint();
		this.rightCorner = (float[])rightCorner.getPoint();

	}
	
	/** Creates a new FloatPointRectangle given leftCorner and rightCorner as float arrays 
	 * (arrays lengths must be equal)
	 * 
	 * @param leftCorner float array representing lower-left corner of the rectangle
	 * @param rightCorner  float array representing upper-right corner of the rectangle
	*/	
	public FloatPointRectangle(float[] leftCorner, float[] rightCorner) {
		if (leftCorner.length != rightCorner.length)
			throw new IllegalArgumentException("dimensions must be equal!");
		this.leftCorner = leftCorner;
		this.rightCorner = rightCorner;
	}

	/** Creates a new FloatPointRectangle as a copy of the given Rectanle 
	 * 
	 * @param rectangle Rectangle which should be copied
	*/
	public FloatPointRectangle(Rectangle rectangle) {
		DoublePointRectangle rect = (DoublePointRectangle)rectangle;
		leftCorner = new float[rect.leftCorner.length];
		rightCorner = new float[rect.rightCorner.length];
		System.arraycopy (rect.leftCorner, 0, leftCorner, 0, rect.leftCorner.length);
		System.arraycopy (rect.rightCorner, 0, rightCorner, 0, rect.rightCorner.length);
	}

	/** Creates a new FloatPointRectangle of given dimension with corners
	 * (0,0,...,0) and (1,1,...,1)
	 * 
	 * @param dim specifies dimension for the rectangle
	 */
	public FloatPointRectangle(int dim) {
		float[] d = new float[dim];
		for (int i = 0; i < dim; i++)
			d[i] = 1.0f;
		this.rightCorner = d;
		this.leftCorner = new float[dim];
	}
	
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
	public void read(DataInput dataInput) throws IOException {
		for(int i=0; i< leftCorner.length; i++)
			leftCorner[i] = dataInput.readFloat();
		for(int i=0; i< rightCorner.length; i++)
			rightCorner[i] = dataInput.readFloat();
	}
	
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
	public void write(DataOutput dataOutput) throws IOException {
		for(int i=0; i< leftCorner.length; i++)
			dataOutput.writeDouble(leftCorner[i]);
		for(int i=0; i< rightCorner.length; i++)
			dataOutput.writeDouble(rightCorner[i]);
	}

	/** Returns the dimensionality of this rectangle.
	 * @return dimensionality of the rectangle
	*/
	public int dimensions() {
		return leftCorner.length;
	}

	/** Returns the left/right corner point of this rectangle.
	 * 
	 * @param right if this parameter is set to <tt>true</tt> upper-right corner will be 
	 *         returned otherwise it will be lower-left corner
	 * @return returns corner point of this rectangle (upper-right or lower-left depending 
	 *          on the input boolean parameter <tt>right</tt>)
	*/
	public Point getCorner(boolean right) {
		return right ? new FloatPoint(rightCorner) : new FloatPoint(leftCorner);
	}

	/** Returns the delta (vector containing size on each dimention) of this 
	 *  rectangle as an array of double-point values
	 * 
	 * @return returns the delta of this rectangle as an array of double-point values
	*/
	public double[] deltas() {
		double[] ret = new double[leftCorner.length];
		for (int d = leftCorner.length; --d >= 0; )
			ret[d] = rightCorner[d] - leftCorner[d];
		return ret;
	}

	/** Calculates the area (volume) of this rectangle as a double-point value.
	 * 
	 * @return returns the area of this rectangle  
	*/
	public double area() {
		double area = 1.0;
		for (int i = leftCorner.length;
			--i >= 0;
			area *= rightCorner[i] - leftCorner[i]);
		return area;
	}


	/** Calculates the margin (perimeter) of this rectangle as a double-point value.
	 * 
	 * @return returns the margin of this rectangle 
	*/

	public double margin() {
		double margin = 0.0;
		for (int i = dimensions();
			--i >= 0;
			margin += rightCorner[i] - leftCorner[i]);
		return margin;
	}

	/** Compares first coordinate of leftCorner Points of this and another given
	 *  rectangle. Returns 
	 * 
	 *  -1, if the value is smaller for this rectangle
	 *  0, if values are equal
	 *  1, if the value is bigger for this rectangle  
	 * 
	 * @param object is the DoublePointRectangle object, which will be compared with this rectangle.
	 * @return  returns the result of the comparison 
	 */
	public int compareTo(Object object) {
		FloatPointRectangle r = (FloatPointRectangle) object;
		return leftCorner[0] < r.leftCorner[0]
			? -1
			: leftCorner[0] == r.leftCorner[0]
			? 0
			: +1;
	}

	/** Checks whether the rectangle contains the given double point 
	 * 
	 * @param point is the point to be checked.
	 * @return <tt>true</tt> if this rectangle contains given double point
	 */
	public boolean contains(Point point) {
		float[] d = (float[])point.getPoint();
		for (int i = leftCorner.length; --i >= 0;)
			if (d[i] < leftCorner[i]
				|| rightCorner[i] < d[i])
				return false;
		return true;
	}

	/** Checks whether the rectangle contains an object in form of Tree.Descriptor.
	 * 
	 * @param descriptor is the Tree.Descriptor to be checked.
	 * @return <tt>true</tt> if this rectangle contains given Tree.Descriptor
	 */
	public boolean contains(Descriptor descriptor) {
		FloatPointRectangle rect = (FloatPointRectangle)descriptor;
		for (int i = leftCorner.length; --i >= 0;)
			if (leftCorner[i] > rect.leftCorner[i]
				|| rect.rightCorner[i] > rightCorner[i])
				return false;
		return true;
	}

	/** Checks whether the rectangle is equal to another object given.
	 * 
	 * @param object object to be tested.
	 * @return <tt>true</tt> if the rectangle is equal to given object.
	 */
	public boolean equals(Object object) {
		FloatPointRectangle rect = (FloatPointRectangle)object;
		if (Arrays.equals(leftCorner, rect.leftCorner) && Arrays.equals(rightCorner, rect.rightCorner))
			return true;
		return false;
	}

	/** Checks whether this rectangle overlaps another given rectangle at a given dimension.
	 * 
	 * @param rectangle the rectangle to be tested.
	 * @param dimension specifies in which dimension to test.
	 * @return <tt>true</tt> if this rectangle overlaps another rectangle at a given dimension.
	 */
	public boolean overlaps(Rectangle rectangle, int dimension) {
		FloatPointRectangle rect = (FloatPointRectangle)rectangle;
		return (
			leftCorner[dimension] <= rect.rightCorner[dimension]) 
			&& 
			(rect.leftCorner[dimension]	<= rightCorner[dimension]
		);
	}

	/** Checks whether this rectangle overlaps an object in form of Tree.Descriptor.
	 * 
	 * @param descriptor is the Tree.Descriptor to be tested.
	 * @return <tt>true</tt> if this rectangle overlaps given Tree.Descriptor
	 */
	public boolean overlaps(Descriptor descriptor) {
		FloatPointRectangle rect = (FloatPointRectangle)descriptor;
		for (int i = leftCorner.length; --i >= 0;)
			if (leftCorner[i] > rect.rightCorner[i]
				|| rect.leftCorner[i] > rightCorner[i])
				return false;
		return true;
	}

	/** Computes the area of overlap between this rectangle and another given one.
	 * 
	 * @param rectangle is the rectangle to calculate area of overlap with
	 * @return returns the calculated overlap area or 0 if the rectangles do not overlap
	 */
	public double overlap(Rectangle rectangle) {
		FloatPointRectangle rect = (FloatPointRectangle)rectangle;
		double overlap = 1.0;
		for (int i = dimensions();
			--i >= 0
				&& (overlap
					*= 
					Math.min(
						rightCorner[i],
						rect.rightCorner[i])
					- Math.max(
						leftCorner[i],
						rect.leftCorner[i]))
					> 0;
			);
		return overlap < 0d ? 0d : overlap;
	}

	/** Computes the distance between the given point and the nearest point of this rectangle 
	 *  using the specified Lp-Metrics.
	 * 
	 * @param  point the given point to be checked 
	 * @param  p the given metric to be used
	 * @return distance calculated using given Lp-Metrics   
	 */
	public double minDistance(Point point, int p) {
		double distance = 0.0;
		float[] d = (float[])point.getPoint();

		if (p == Integer.MAX_VALUE)
			for (int i = leftCorner.length;
				--i >= 0;
				distance =
					Math.max(
						distance,
						Math.max(leftCorner[i], d[i])
							- Math.min(rightCorner[i], d[i])));
		else {
			for (int i = leftCorner.length; --i >= 0;) {
				double dist =
					Math.max(leftCorner[i], d[i])
						- Math.min(rightCorner[i], d[i]);

				distance += p == 1 ? dist : Math.pow(dist, p);
			}
			if (p != 1)
				distance = Math.pow(distance, 1.0 / p);
		}
		return distance;
	}

	/** Computes the distance between the given point and the most distant point of this rectangle 
	 *  using the specified Lp-Metric.
	 * 
	 * @param  point the given point to be checked 
	 * @param  p the given metric to be used
	 * @return distance calculated using given Lp-Metrics    
	 */
	public double maxDistance(Point point, int p) {
		double distance = 0.0;
		float[] d = (float[])point.getPoint();

		if (p == Integer.MAX_VALUE)
			for (int i = leftCorner.length;
				--i >= 0;
				distance =
					Math.max(
						distance,
						Math.max(
							Math.abs(d[i] - leftCorner[i]),
							Math.abs(rightCorner[i] - d[i]))));
		else {
			for (int i = dimensions(); --i >= 0;) {
				double dist =
					Math.max(
						Math.abs(d[i] - leftCorner[i]),
						Math.abs(rightCorner[i] - d[i]));

				distance += p == 1 ? dist : Math.pow(dist, p);
			}
			if (p != 1)
				distance = Math.pow(distance, 1.0 / p);
		}
		return distance;
	}

	/** Computes the shortest distance between this rectangle and another given rectangle 
	 * using the Lp-Metric.
	 * @param  rectangle the given rectangle to be checked 
	 * @param  p the given metric to be used 
 	 * @return distance calculated using given Lp-Metrics    
	 */
	public double distance(Rectangle rectangle, int p) {
		FloatPointRectangle rect = (FloatPointRectangle)rectangle;
		double distance = 0.0;

		if (p == Integer.MAX_VALUE)
			for (int i = leftCorner.length;
				--i >= 0;
				distance =
					Math.max(
						distance,
						Math.max(
							leftCorner[i],
							rect.leftCorner[i])
							- Math.min(
								rightCorner[i],
								rect.rightCorner[i])));
		else {
			for (int i = leftCorner.length; --i >= 0;) {
				double dist =
					Math.max(
						leftCorner[i],
						rect.leftCorner[i])
						- Math.min(
							rightCorner[i],
							rect.rightCorner[i]);

				if (dist > 0)
					distance += p == 1 ? dist : Math.pow(dist, p);
			}
			if (p != 1)
				distance = Math.pow(distance, 1.0 / p);
		}
		return distance;
	}

	/**
	 * Creates rectangle witch represents the result of the union of this rectangle 
	 * and another given object in form Tree.Descriptor and and 
	 * stores the result instead of source rectangle. Attention! Source rectangle is modified.
	 * 
	 * @param descriptor is the Tree.Descriptor to union with.
	 */
	public void union(Descriptor descriptor) {
		FloatPointRectangle rect = (FloatPointRectangle)descriptor;
		for (int i = leftCorner.length; --i >= 0;) {
			if (leftCorner[i] > rect.leftCorner[i])
				leftCorner[i] = rect.leftCorner[i];
			if (rightCorner[i] < rect.rightCorner[i])
				rightCorner[i] = rect.rightCorner[i];	
		}
	}



	/** Returns a string representation of this rectangle.
	 * 
	 * @return returns the string representation of this rectangle
	*/
	public String toString() {
		StringBuffer sb = new StringBuffer("\nRectangle\n");
		sb.append("\tleftCorner\n");
		for (int d = 0;
			d < leftCorner.length;
			d++) {
			sb.append("\t\t" + d + ":\t" + leftCorner[d] + "\n");
		}
		sb.append("\trightCorner\n");
		for (int d = 0; d < rightCorner.length; d++) {
			sb.append("\t\t" + d + ":\t" + rightCorner[d] + "\n");
		}
		return new String(sb);
	}

	/** Create and returns a physical copy of this rectangle.
	 * 
	 * @return returns the copy of this rectangle
	*/
	public Object clone() {
		return new FloatPointRectangle(this);
	}
	
	/** Calculates hash code of this rectangle
	 * 
	 * @return returns hash code of this rectangle
	*/
	public int hashCode() {
		float c = 0;
		for (int i = 0; i < leftCorner.length; i++)
			c += (leftCorner[i] + rightCorner[i]);
		return (int)c%1117;
	}

	/** Computes the intersection of this rectangle and another given rectangle and 
	 * stores the result instead of source rectangle. Attention! Source rectangle is modified.
	 * There will be a exception IllegalArgumentException if they do not overlap
	 *
	 * @param rectangle
	 */
	public void intersect(Rectangle rectangle) {
		FloatPointRectangle rect = (FloatPointRectangle)rectangle;
		if (!overlaps(rectangle))
			throw new IllegalArgumentException("No overlap."); 
		for (int i = dimensions(); --i >= 0;) {
			if (leftCorner[i] < rect.leftCorner[i])
				leftCorner[i] = rect.leftCorner[i];
			if (rightCorner[i] > rect.rightCorner[i])
				rightCorner[i] = rect.rightCorner[i];
		}
	}

} 	
