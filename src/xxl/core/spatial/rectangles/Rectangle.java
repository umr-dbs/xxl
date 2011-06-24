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

import xxl.core.indexStructures.Descriptor;
import xxl.core.io.Convertable;
import xxl.core.spatial.points.Point;

/**
 *	A high-dimensional Rectangle (=hyper-cube).
 *	
 *  @see xxl.core.spatial.points.Point
 *	@see xxl.core.spatial.rectangles.RandomRectangle
 *	@see xxl.core.spatial.rectangles.DoublePointRectangle
 *  @see xxl.core.spatial.rectangles.FloatPointRectangle
 *  @see xxl.core.spatial.rectangles.FixedPointRectangle
 *	
 */

public interface Rectangle extends Comparable, Convertable, Descriptor {
	
	/** Returns the left/right corner point of this rectangle.
	 * 
	 * @param right if this parameter is set to <tt>true</tt> upper-right corner will be 
	 *         returned otherwise it will be lower-left corner
	 * @return returns corner point of this rectangle (upper-right or lower-left depending 
	 *          on the input boolean parameter <tt>right</tt>)
	*/
	public abstract Point getCorner(boolean right);

	/** Returns the dimensionality of this rectangle.
	 * 
	 * @return dimensionality of the rectangle
	*/
	public abstract int dimensions();

	/** Returns the delta (vector containing size on each dimention) of this 
	 *  rectangle as an array of double-point values
	 * 
	 * @return returns the delta of this rectangle as an array of double-point values
	*/
	public abstract double[] deltas();

	/** Calculates the area (volume) of this rectangle as a double-point value.
	 * 
	 * @return returns the area of this rectangle  
	*/
	public abstract double area();

	/** Calculates the margin (perimeter) of this rectangle as a double-point value.
	 * 
	 * @return returns the margin of this rectangle 
	*/
	public abstract double margin();

	/** Checks whether the rectangle contains the given point 
	 * 
	 * @param point is the point to be checked.
	 * @return <tt>true</tt> if this rectangle contains given double point
	 */
	public abstract boolean contains(Point point);
	
	/** Computes the shortest distance between this rectangle and another given rectangle 
	 * using the Lp-Metric.
	 * @param  rectangle the given rectangle to be checked 
	 * @param  p the given metric to be used 
 	 * @return distance calculated using given Lp-Metrics    
	 */
	public abstract double distance(Rectangle rectangle, int p); 
	
	/** Computes the distance between the given point and the nearest point of this rectangle 
	 *  using the specified Lp-Metrics.
	 * 
	 * @param  point the given point to be checked 
	 * @param  p the given metric to be used
	 * @return distance calculated using given Lp-Metrics   
	 */
	public abstract double minDistance(Point point, int p);
	
	/** Computes the distance between the given point and the most distant point of this rectangle 
	 *  using the specified Lp-Metric.
	 * 
	 * @param  point the given point to be checked 
	 * @param  p the given metric to be used
	 * @return distance calculated using given Lp-Metrics    
	 */
	public abstract double maxDistance(Point point, int p);

	/** Checks whether this rectangle overlaps another given rectangle at a given dimension.
	 * 
	 * @param rectangle the rectangle to be tested.
	 * @param dimension specifies in which dimension to test.
	 * @return <tt>true</tt> if this rectangle overlaps another rectangle at a given dimension.
	 */
	public abstract boolean overlaps(Rectangle rectangle, int dimension);


	/** Computes the area of overlap between this rectangle and another given one.
	 * 
	 * @param rectangle is the rectangle to calculate area of overlap with
	 * @return returns the calculated overlap area or 0 if the rectangles do not overlap
	 */
	public abstract double overlap(Rectangle rectangle);

	/** Computes the intersection of this rectangle and another given rectangle and 
	 * stores the result instead of source rectangle. Attention! Source rectangle is modified.
	 * There will be a exception IllegalArgumentException if they do not overlap
	 *
	 * @param rectangle
	 */
	public abstract void intersect(Rectangle rectangle);
	
}
