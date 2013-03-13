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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides functionality for saving and restoring rectangles to disk files 
 */

public abstract class Rectangles {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Rectangles(){}

	/** Reads rectangle object from the given file using Convertable interface read metod  
	 * @see xxl.core.io.Convertable
	 *	
	 * @param file input file 
	 * @param rectangle rectangle to save data from file
	 * @return returns rectangle which have been read
	 */
	public static Rectangle readSingletonRectangle(File file, Rectangle rectangle) {
		DataInputStream din;
		try {
			din = new DataInputStream(new FileInputStream(file));
			rectangle.read(din);
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
		return rectangle;
	}

	/** Writes rectangle object to the given file using Convertable interface write metod  
	 * @see xxl.core.io.Convertable
	 * @param file output file
	 * @param rectangle rectangle to write
	 */
	public static void writeSingletonRectangle(
		File file,
		Rectangle rectangle) {
		DataOutputStream dos;
		try {
			dos = new DataOutputStream(new FileOutputStream(file));
			rectangle.write(dos);
			dos.close();
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}
	/**
	 * 
	 * @param p
	 * @return
	 */
	public static DoublePoint upperLeftCorner(DoublePointRectangle p) {
		if (p.dimensions() != 2)
			throw new IllegalArgumentException();
		return new DoublePoint(new double[]{ p.leftCorner[0], p.rightCorner[1] });
	}
	/**
	 * 
	 * @param p
	 * @return
	 */
	public static DoublePoint lowerRightCorner(DoublePointRectangle p) {
		if (p.dimensions() != 2)
			throw new IllegalArgumentException();
		return new DoublePoint(new double[]{ p.rightCorner[0], p.leftCorner[1] });
	}
	/**
	 * Computes Universe for a set of doublepoint rectangles
	 * @param rectangles
	 * @return
	 */
	public static DoublePointRectangle computeDoublePointRectangleUniverse(Iterator<DoublePointRectangle> rectangles){
		DoublePointRectangle universe = null;
		while(rectangles.hasNext()){
			DoublePointRectangle item = rectangles.next(); 
			if (universe == null)
				universe = new DoublePointRectangle(item);
			else 
				universe.union(item);
		}
		return universe;
	}
	
	/**
	 * Factory Function e.g. for convertable converter; 
	 * 
	 * Note resulting function creates always new object; 
	 * @param dimension
	 * @return returns function object 
	 */
	public static final Function<Object, DoublePointRectangle> factoryFunctionDoublePointRectangle(final int dimension){ 
		return new AbstractFunction<Object, DoublePointRectangle>() {
			public DoublePointRectangle invoke() {
				return new DoublePointRectangle(dimension);
			}
		};
	}
	/**
	 * Factory method returns converter for doublepointrectangles
	 * 
	 * @param dimension
	 * @return Converter for doublepointrectangles
	 */
	public static Converter<DoublePointRectangle> getDoublePointRectangleConverter(int dimension){
		return new ConvertableConverter<>(factoryFunctionDoublePointRectangle(dimension)); 
	}
	
	
	/**
	 * Generates double point reactangle with universe values
	 * @param dimension
	 * @return
	 */
	public static DoublePointRectangle getUnitUniverseDoublePointRectangle(int dimension){
		return new DoublePointRectangle(dimension);
	}
	
}
