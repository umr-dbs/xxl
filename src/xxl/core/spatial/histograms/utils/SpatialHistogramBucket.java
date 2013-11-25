/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger
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
package xxl.core.spatial.histograms.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;


/**
 * 
 * this class represents double point rectangle with weight information 
 * is used to represent histogram bucket
 * 
 *
 */
@SuppressWarnings("serial")
public class SpatialHistogramBucket extends DoublePointRectangle{
	
	public static final int DEFAULT_WEIGHT = 1;
	
	
	
	public static final Function<Object, SpatialHistogramBucket > getFactoryFunction(final int dimension){
		return new AbstractFunction<Object, SpatialHistogramBucket>() {
			public SpatialHistogramBucket invoke(){
				return new  SpatialHistogramBucket(dimension); 
			}
		};
	}
	
	private int weight;
	
	public double[] avgExtent;
	
	int counter = 1;
	
	public double[] getExtent(){
		double[] avgArray = new double[dimensions()];
		
		for(int i = 0; i < dimensions(); i++  ){
			avgArray[i]= avgExtent[i] /(double)weight;
			
    	}
		return avgArray;
	}
	
	
	public SpatialHistogramBucket(double[] leftCorner,
			double[] rightCorner, int weight) {
		super(leftCorner, rightCorner);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}

	public SpatialHistogramBucket(DoublePointRectangle rec,  int weight) {
		super(rec);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}
	
	public SpatialHistogramBucket(int dimension,  int weight) {
		super(dimension);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}
	
	public SpatialHistogramBucket(double[] p1, double[] p2) {
		super(p1, p2);
		this.weight = 1;
		avgExtent = new double[leftCorner.length];
	}
	
	public SpatialHistogramBucket(int dimension) {
		super(dimension);
		this.weight = DEFAULT_WEIGHT;
		avgExtent = new double[leftCorner.length];
	}
	
	public SpatialHistogramBucket(DoublePointRectangle rec) {
		super(rec);
		this.weight = DEFAULT_WEIGHT;
		avgExtent = new double[leftCorner.length];
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		super.write(dataOutput);
		IntegerConverter.DEFAULT_INSTANCE.write(dataOutput, this.weight);
	}
	
	@Override
	public void read(DataInput dataInput) throws IOException {
		super.read(dataInput);
		int weight = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
		this.weight = weight;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	public int getSerializedByteSize(){
		return this.dimensions() * DoubleConverter.SIZE * 2 + IntegerConverter.SIZE;
	}
	
	public void updateAverage(double[] array){
		for(int i = 0; i < array.length; i++)
			avgExtent[i] += array[i];
	}
		
	public void updateAverage(DoublePointRectangle rec){
		counter++;
		for(int i= 0; i < dimensions(); i++){
			avgExtent[i]+=rec.deltas()[i];
		}
		
	}
	
	
	
	@Override
	public String toString() {
		return "WeightedDoublePointRectangle [weight=" + weight
				+ ", leftCorner=" + Arrays.toString(leftCorner)
				+ ", rightCorner=" + Arrays.toString(rightCorner) + "]";
	}
	
}
