package xxl.core.spatial.histograms;

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
public class WeightedDoublePointRectangle extends DoublePointRectangle{
	
	public static final int DEFAULT_WEIGHT = 1;
	
	
	
	public static final Function<Object, WeightedDoublePointRectangle > getFactoryFunction(final int dimension){
		return new AbstractFunction<Object, WeightedDoublePointRectangle>() {
			public WeightedDoublePointRectangle invoke(){
				return new  WeightedDoublePointRectangle(dimension); 
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
	
	
	public WeightedDoublePointRectangle(double[] leftCorner,
			double[] rightCorner, int weight) {
		super(leftCorner, rightCorner);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}

	public WeightedDoublePointRectangle(DoublePointRectangle rec,  int weight) {
		super(rec);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}
	
	public WeightedDoublePointRectangle(int dimension,  int weight) {
		super(dimension);
		this.weight = weight;
		avgExtent = new double[leftCorner.length];
	}
	
	public WeightedDoublePointRectangle(double[] p1, double[] p2) {
		super(p1, p2);
		this.weight = 1;
		avgExtent = new double[leftCorner.length];
	}
	
	public WeightedDoublePointRectangle(int dimension) {
		super(dimension);
		this.weight = DEFAULT_WEIGHT;
		avgExtent = new double[leftCorner.length];
	}
	
	public WeightedDoublePointRectangle(DoublePointRectangle rec) {
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
