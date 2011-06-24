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
package xxl.core.spatial.geometries.cursors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.converters.DoubleConverter;
import xxl.core.spatial.KPE;

/** A <tt>DistanceWeightedKPE</tt> wraps {@link KPE}-objects and additionally contains information on the
 *  distance of the <tt>KPE</tt>-descriptor from some query-object. Objects of this class are
 *  used in {@link NearestNeighborQuery} to reduce the computational amount of repeatedly
 *  determining the distance of <tt>KPE</tt>s to some query-object.
 *  <br><br>
 *  <tt>DistanceWeightedKPE</tt>-objects can be compared according to their distance.
 */
public class DistanceWeightedKPE extends KPE implements Comparable<DistanceWeightedKPE>{
	
	/** the distance of the KPE's descriptor to some query object */
	private double distance;
	
	/** The Constructor wraps the given KPE and stores the distance information in
	 *  a new <code>DistanceWeightedKPE</code>-object.
	 *  
	 * @param k the <tt>KPE</tt>-object to wrap
	 * @param d the distance of the <tt>KPE</tt>'s descriptor to a query object
	 */
	public DistanceWeightedKPE(KPE k, double d){				
		super(k);				
		distance = d;
	}
	
	/** Returns the distance of the <tt>KPE</tt>'s descriptor to a query object
	 * 
	 * @return the distance of the <tt>KPE</tt>'s descriptor to a query object
	 */
	public double getDistance(){ return distance;	}
	
	/** Compares this <tt>DistanceWeightedKPE</tt> to another one by comparing 
	 *  the distances of the objects.
	 * 
	 * @param other the other <tt>DistanceWeightedKPE</tt>-object
	 * @return 0 if both objects hav equal distance, -1 if the distance 
	 *         of this object is less, +1 if this distance is greater than the
	 *         distance of the other object
	 */
	public int compareTo(DistanceWeightedKPE other) {				
		return Double.compare(distance, other.distance);
	}
	
	/** Returns a deep copy of this object. 
	 * @inheritDoc 
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		return new DistanceWeightedKPE((KPE) super.clone(), distance);
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public String toString(){
		return super.toString() + "\nDistance: \t" +distance ;
	}

	/** Restores this object's attributes from the given data-input
	 *  @param dataInput the input (e.g. a stream) to read the attribute-values from
	 */
	public void read(DataInput dataInput) throws IOException{
		super.read(dataInput);
		distance = DoubleConverter.DEFAULT_INSTANCE.read(dataInput);
	}

	/** Stores this object's attributes to the given data-output
	 *  @param dataOutput the output (e.g. a stream) to store the attributes to
	 */
	public void write(DataOutput dataOutput) throws IOException{
		super.write(dataOutput);
		DoubleConverter.DEFAULT_INSTANCE.write(dataOutput, distance);
	}
}
