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
import xxl.core.io.converters.Converter;

/**
 * A Wrapper for Number[]-points that provides useful methods on points
 * like e.g. a conversion mechanism.
 *
 * @see xxl.core.io.Convertable
 * @see xxl.core.spatial.points.Point
 * @see xxl.core.spatial.points.Points
 * @see xxl.core.spatial.points.AbstractPoint
 */
public class NumberPoint<T extends Number> extends AbstractPoint implements Convertable, Cloneable {

	/** A factory for DoublePoints.
	 */
	public Function<Object,NumberPoint<T>> getFactoryFunction(final Converter<T> converter) {
		return new AbstractFunction<Object,NumberPoint<T>>(){
			public NumberPoint<T> invoke(Object point){
				return new NumberPoint<T>( (T[]) point );
			}
		
			public NumberPoint<T> invoke(Object object, Object dim){
				return new NumberPoint<T>( ((Integer)dim).intValue(), converter );
			}
		};
	}
	
	/** Converts a DoublePoint into a NumberPoint<Double>
	 * 
	 * @param doublePoint the DoublePoint to be converted
	 */
	public static NumberPoint<Double> doublePointToNumberPoint(DoublePoint doublePoint) {
		double[] point = (double[])doublePoint.getPoint();
		Double[] Dpoint = new Double[point.length];
		for (int i=0; i<point.length; i++)
			Dpoint[i] = point[i];
		return new NumberPoint<Double>(Dpoint);
	}

	/** Converts a FixedPoint into a NumberPoint<Double>
	 * 
	 * @param fixedPoint the FixedPoint to be converted
	 */
	public static NumberPoint<Long> doublePointToNumberPoint(FixedPoint fixedPoint) {
		long[] point = (long[])fixedPoint.getPoint();
		Long[] Lpoint = new Long[point.length];
		for (int i=0; i<point.length; i++)
			Lpoint[i] = point[i];
		return new NumberPoint<Long>(Lpoint);
	}

	/** Converts a FloatPoint into a NumberPoint<Float>
	 * 
	 * @param floatPoint the FloatPoint to be converted
	 */
	public static NumberPoint<Float> doublePointToNumberPoint(FloatPoint floatPoint) {
		float[] point = (float[])floatPoint.getPoint();
		Float[] Fpoint = new Float[point.length];
		for (int i=0; i<point.length; i++)
			Fpoint[i] = point[i];
		return new NumberPoint<Float>(Fpoint);
	}

	
	/** The data to be wrapped.
	 */
	protected T[] point;
	
	/** Converter for T objects
	 */
	Converter<T> converter;
	
	/** Creates a new TypedPoint.
	 *
	 *	@param point the primitive double-point to be wrapped.
	 */
	public NumberPoint(T[] point){
		this.point = point;
	}

	/** Creates a new TypedPoint.
	 *  (with coordinates (null,...,null)).
	 *
	 *	@param dim dimensionality of the point
	 */
	public NumberPoint(int dim, Converter<T> converter){
		this((T[])new Number[dim]);
		this.converter = converter;
	}

	/** Returns a physical copy of this TypedPoint.
	 * @return returns a physical copy of this TypedPoint
	 */
	@Override
	public Object clone(){
		return new NumberPoint((T[])point.clone());
	}

	/** Returns (gets) the primitive data-point wrapped by this TypedPoint.
	 * @return returns the primitive data-point wrapped by this TypedPoint
	 */
	@Override
	public Object getPoint(){
		return point;
	}

	/** Returns the dimensionality of this TypedPoint.
	 * @return returns the dimensionality of this TypedPoint
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
	public void read (DataInput dataInput) throws IOException {
		if (converter!=null) {			
			for (int i=0; i< point.length; i++)
				point[i] = converter.read(dataInput);
			return;
		}
		if (theClass == null)
			determineClass();
		if (theClass.equals("Byte")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Byte(dataInput.readByte());
		}
		else if (theClass.equals("Double")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Double(dataInput.readDouble());
		}
		else if (theClass.equals("Float")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Float(dataInput.readFloat());
		}
		else if (theClass.equals("Integer")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Integer(dataInput.readInt());
		}
		else if (theClass.equals("Long")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Long(dataInput.readLong());
		}
		else if (theClass.equals("Short")) {
			for (int i=0; i< point.length; i++)
				point[i] = (T)new Short(dataInput.readShort());
		}				
		else 
			throw new UnsupportedOperationException(theClass+" is not supported");
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
	public void write (DataOutput dataOutput) throws IOException {
		if (converter!=null) {			
			for (int i=0; i< point.length; i++)
				converter.write(dataOutput, point[i]);
			return;
		}
		if (theClass == null)
			determineClass();
		if (theClass.equals("Byte")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].byteValue());
		}
		else if (theClass.equals("Double")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].doubleValue());
		}
		else if (theClass.equals("Float")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].floatValue());
		}
		else if (theClass.equals("Integer")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].intValue());
		}
		else if (theClass.equals("Long")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].longValue());
		}
		else if (theClass.equals("Short")) {
			for (int i=0; i< point.length; i++)
				dataOutput.writeDouble(point[i].shortValue());
		}
		else 
			throw new UnsupportedOperationException(theClass+" is not supported");
	}
	
	private void determineClass() {
		theClass = point[0].getClass().getSimpleName();
		
	}
	private String theClass;

	/** Returns the coordinate of this DoublePoint in a given dimension <dim>.
	 * 	
	 * @param dim dimension to get coordinate
	 * @return returns the coordinate in given dimension
	 */
	@Override
	public double getValue(int dim){
		return point[dim].doubleValue();	
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals()
	 */
	@Override
	public boolean equals(Object o) {
		NumberPoint<T> p = (NumberPoint<T>)o;
		return Arrays.equals(point, p.point);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		double c = 0;
		for (int i = 0; i < point.length; i++)
			c += point[i].doubleValue();
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
