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

package xxl.core.spatial.cursors;

import java.io.File;

import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.points.FixedPoint;
import xxl.core.spatial.points.FloatPoint;
import xxl.core.spatial.points.Point;

/**
 * The PointInputCursor constructs an Iterator of Points
 * for a given flat File of Point-Objects.
 *
 */
public class PointInputCursor extends FileInputCursor<Point>{

	/**
	 * DOUBLE_POINT is a constant that indicates the dimensions of  a point being of type double
	 */
	public static final int DOUBLE_POINT = 0;
	/**
	 * FLOAT_POINT is a constant that indicates the dimensions of  a point being of type float
	 */	
	public static final int FLOAT_POINT  = 1;
	/**
	 * FIXED_POINT is a constant that indicates the dimensions of a point being of type
	 * bitsequence.
	 */
	public static final int FIXED_POINT  = 2;
	
	/**
	 * The method newDoublePoint creates a factory for DoublePoint.
	 * @param dim dim refers to the dimension of the point
	 * @return a functional factory for generating new points
	 */
	public static Function newDoublePoint(final int dim) {
		return new AbstractFunction() {
			public Object invoke(){
				return new DoublePoint(dim);
			}
		};
	}
	
	/**
	 * The method newFloatPoint creates a factory for FloatPoint.
	 * @param dim dim refers to the dimension of the point
	 * @return a functional factory for generating new points
	 */
	public static Function newFloatPoint(final int dim) {
		return new AbstractFunction() {
			public Object invoke(){
				return new FloatPoint(dim);
			}
		};
	}
	
	/**
	 * The method newFixedPoint creates a factory for FixedPoint.
	 * @param dim dim refers to the dimension of the point
	 * @return a functional factory for generating new points
	 */
	public static Function newFixedPoint(final int dim) {
		return new AbstractFunction() {
			public Object invoke(){
				return new FixedPoint(dim);
			}
		};
	}
	
	/** Creates a new PointInputCursor.
	 *
	 *  @param newPoint the factory for creating a new point
	 * 	@param file the file containing the input-data
	 *	@param bufferSize the buffer-size to be allocated for reading the data
	 */
	public PointInputCursor(final Function newPoint, File file, int bufferSize){
		super(
			new ConvertableConverter<Point>(newPoint),
			file,
			bufferSize
		);
	}

	/** Creates a new PointInputCursor. The bufferSize is set to 1MB bytes.
	 *
	 *	@param file the file containing the input-data
	 *  @param newPoint the factory for creating a new point
	*/
	public PointInputCursor(Function newPoint, File file){
		this(newPoint, file, 1024*1024);
	}
	
	/**
	 * 
	 * @param file the file containing the input data
	 * @param TYPE the type of the dimension of the point (must be 0,1 or 2)
	 * @param dim the dimension of the point
	 * @param bufferSize the buffer-size to be allocated for reading the data
	 */
	public PointInputCursor(File file, int TYPE, int dim, int bufferSize) {
		this(TYPE == 0 ? newDoublePoint(dim) : TYPE == 1 ? newFloatPoint(dim) : TYPE == 2 ? newFixedPoint(dim) : null,
			file, bufferSize
		);
		if (TYPE < 0 || TYPE > 2)
			throw new IllegalArgumentException("Undefined type specified.");
	}
	
	/**
	 * A constructor for PointInputCursor where the buffer-size is 1MB
	 * @param file the file containing the input data
	 * @param TYPE the type of the dimension of the point (must be 0,1 or 2)
	 * @param dim the dimension of the point
	 */
	public PointInputCursor(File file, int TYPE, int dim) {
		this(file, TYPE, dim, 1024*1024);
	}
}

