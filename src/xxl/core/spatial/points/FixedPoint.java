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
import xxl.core.util.BitSet;

/**
 *  A Wrapper for Fixed-Mantissa-Points.
 *
 *  The data is assumed to be inside the unit-cube [0;1)^dim  (i.e. 1 is excluded). For this type
 *  of data we only store the mantissa of the original floating-point. The conversion from 
 *  floating-point to fixed-point and vice-versa is proveded by the methods 
 *  {@link xxl.core.math.Maths#doubleToNormalizedLongBits(double) doubleToNormalizedLongBits} and
 *  {@link xxl.core.math.Maths#normalizedLongBitsToDouble(long) normalizedLongBitsToDouble}.
 *  These methods extract the mantissa using bit operations and perform
 *  necessary shift operations.
 *  
 *  The conversion to a fixed point representation is extremely useful to create z-codes and to compute replicate-rectangles.
 *  The bit-fields can be easily merged to a zCode. The split-lines of the replication process 
 *  correspond exactly to the bits of the mantissa: if a bit is set it corresponds to the "right" partition
 *  in the current recursion step of the replication process, otherwise it corresponds to the "left" partition.
 *  There is no "artificial" mapping/computation between floating points and split-lines involved
 *  to determine partitions.
 *  
 *
 *  @see xxl.core.spatial.points.Point
 *  @see xxl.core.spatial.points.Points
 *  @see xxl.core.spatial.points.AbstractPoint
 *  @see xxl.core.spatial.points.FloatPoint
 *
 */
public class FixedPoint extends AbstractPoint implements Convertable, Cloneable {

	/** A factory for FixedPoints.
	 */
	public static final Function FACTORY = new AbstractFunction(){
		public Object invoke(Object point){
			return new FixedPoint( (long[]) point );
		}

		public Object invoke(Object object, Object dim){
			return new FixedPoint( ((Integer)dim).intValue() );
		}
	};

	/** The primitive long-point to be wrapped.
	 */
	protected long[] point;

	/** Creates a new FixedPoint.
	 *
	 *	@param point the primitive long-point to be wrapped.
	 */
	public FixedPoint(long[] point){
		this.point = point;
	}

	/** Creates a new FixedPoint from the given DoublePoint (the DoublePoint has to be in [0;1)^dim).
	 *
	 * @param dp the DoublePoint to use
	 */
	public FixedPoint(DoublePoint dp){
		double[] d = (double[]) dp.getPoint();
		long[] np = new long[d.length];
		for(int i=0; i<d.length; i++)
			np[i] = xxl.core.math.Maths.doubleToNormalizedLongBits( d[i] ); //convert each component to fixed-point representation
		this.point = np;
	}

	/** Creates a new FixedPoint.
	 * (with coordinates (0,...,0)).
	 *
	 * @param dim dimensionality of the point
	 */
	public FixedPoint(int dim){
		this(new long[dim]);
	}

	/** Creates a new FixedPoint from a NumberPoint<Long>.
	 *
	 *	@param point the NumberPoint<Long> to be wrapped.
	 */
	public FixedPoint(NumberPoint<Long> np){
		this(np.dimensions());
		Long[] arr = (Long[])np.getPoint();
		for (int i=0; i<point.length; i++)
			point[i] = arr[i];
	}
	
	/** Returns a physical copy of this FixedPoint.
	 * 
	 * @return returns a physical copy of this FixedPoint
	 */
	@Override
	public Object clone(){
		return new FixedPoint((long[])point.clone());
	}
	
	/** Converts this FixedPoint to a DoublePoint and returns it.
	 * 
	 * @return returns DoublePoint representation of this FixedPoint
	 */
	public DoublePoint toDoublePoint(){
		double[] d = new double[dimensions()];
		for(int i=0; i<d.length; i++)
			d[i] = xxl.core.math.Maths.normalizedLongBitsToDouble(point[i]);
		return new DoublePoint(d);
	}

	/** Returns the primitive float-point wrapped by this FixedPoint.
	 * 
	 * @return returns the primitive float-point wrapped by this FixedPoint
	 */
	@Override
	public Object getPoint(){
		return point;
	}

	/** Returns the dimensionality of this FixedPoint.
	 * 
	 * @return returns the dimensionality of this FixedPoint
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
			point[i] = dataInput.readLong();
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
			dataOutput.writeLong(point[i]);
	}

	/** Returns the coordinate of this FixedPoint in a given dimension <dim>.
	 *	
	 * @param dim dimension to get coordinate
	 * @return returns the coordinate in given dimension
	 */
	@Override
	public double getValue(int dim){
		return xxl.core.math.Maths.normalizedLongBitsToDouble(point[dim]);	
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals()
	 */
	@Override
	public boolean equals(Object o) {
		FixedPoint p = (FixedPoint)o;
		return Arrays.equals(point, p.point);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		long c = 0;
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
			sb.append( (new BitSet(point[i])).toString2()+"point["+i+"]:\n" );
		}
		return sb.toString();
	}
}
