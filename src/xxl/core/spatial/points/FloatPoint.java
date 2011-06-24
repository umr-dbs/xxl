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
import java.util.Arrays;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.Convertable;

/**
 * A Wrapper for float[]-points that provides useful methods on points.
 * like e.g. a conversion mechanism.
 *
 * @see xxl.core.io.Convertable
 * @see xxl.core.spatial.points.Point
 * @see xxl.core.spatial.points.Points
 * @see xxl.core.spatial.points.AbstractPoint
 * @see xxl.core.spatial.points.FloatPoint
 *
 */
public class FloatPoint extends AbstractPoint implements Convertable, Cloneable {

	/** A factory for FloatPoints.
	 */
	public static final Function FACTORY = new AbstractFunction(){
		public Object invoke(Object point){
			return new FloatPoint( (float[]) point );
		}

		public Object invoke(Object object, Object dim){
			return new FloatPoint( ((Integer)dim).intValue() );
		}
	};

	/** The primitive float-point to be wrapped.
	 */
	protected float[] point;

	/** Creates a new FloatPoint.
	 *
	 *	@param point The primitive float-point to be wrapped.
	 */
	public FloatPoint(float[] point){
		this.point = point;
	}

	/** Creates a new FloatPoint from the given DoublePoint.
	 * @param dp the DoublePoint to use
	 */
	public FloatPoint(DoublePoint dp){
		double[] d = (double[]) dp.getPoint();
		float[] np = new float[d.length];
		for(int i=0; i<d.length; i++)
			np[i] = (float) d[i];
		this.point = np;
	}

	/** Creates a new FloatPoint.
	 *  (with coordinates (0,...,0)).
	 *
	 *	@param dim dimensionality of the point
	 */
	public FloatPoint(int dim){
		this(new float[dim]);
	}

	/** Creates a new FloatPoint from a NumberPoint<Float>.
	 *
	 *	@param point the NumberPoint<Float> to be wrapped.
	 */
	public FloatPoint(NumberPoint<Float> np){
		this(np.dimensions());
		Float[] arr = (Float[])np.getPoint();
		for (int i=0; i<point.length; i++)
			point[i] = arr[i];
	}
	
	/** Returns a physical copy of this FloatPoint.
	 * 
	 * @return returns a physical copy of this FloatPoint.
	 */
	@Override
	public Object clone(){
		return new FloatPoint((float[])point.clone());
	}

	/** Returns the primitive float-point wrapped by this FloatPoint.
	 * 
	 * @return the primitive float-point wrapped by this FloatPoint
	 */
	@Override
	public Object getPoint(){
		return point;
	}

	/** Returns the dimensionality of this FloatPoint.
	 * 
	 * @return returns the dimensionality of this FloatPoint
	 */
	@Override
	public int dimensions(){
		return point.length;
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
	@Override
	public void read (DataInput dataInput) throws IOException{
		for(int i=0; i< point.length; i++)
			point[i] = dataInput.readFloat();
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
	@Override
	public void write (DataOutput dataOutput) throws IOException{
		for(int i=0; i< point.length; i++)
			dataOutput.writeFloat(point[i]);
	}

	/** Returns the coordinate of this FloatPoint in a given dimension <dim>.
	 *	
	 * @param dim dimension to get coordinate
	 * @return returns the coordinate in given dimension
	 */
	@Override
	public double getValue(int dim){
		return point[dim];	
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals()
	 */
	@Override
	public boolean equals(Object o) {
		FloatPoint p = (FloatPoint)o;
		return Arrays.equals(point, p.point);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		float c = 0;
		for (int i = 0; i < point.length; i++)
			c += point[i];
		return (int)c%1117;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer("");
		for(int i=0; i<point.length; i++){
			sb.append(point[i]+"\t");
		}
		return sb.toString();
	}
}
