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
package xxl.core.spatial;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.mappers.ReservoirSampler;
import xxl.core.functions.Function;
import xxl.core.functions.Functions;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangles;

public class HistogramUtils {
	
	
	/**
	 * 
	 */
	public static final int BITS_PRO_DIM = 31; 
	/**
	 * 
	 */
	public static final int DIMENSION = 2;
	
	/**
	 * 
	 */
	public static final DoublePointRectangle universe = Rectangles.getUnitUniverseDoublePointRectangle(DIMENSION); 

	/**
	 * 
	 */
	public static Function<DoublePointRectangle, Long> toSFC = Functions.toFunction(xxl.core.spatial.SpatialUtils.zCruveSFC(universe, BITS_PRO_DIM));
	
	/**
	 * cursor should implement reset method!
	 * @param rectangles
	 * @param sampleSize
	 * @param samplerType
	 * @return
	 */
	public static Iterator<DoublePointRectangle> createRandomSFCBasedSample(Cursor<DoublePointRectangle> rectangles,  
			int sampleSize, 
			int samplerType){
		// read data 
		ReservoirSampler sampler = new ReservoirSampler(
				new Mapper<DoublePointRectangle, Long>(
						toSFC, 	rectangles) , sampleSize , samplerType);
		Number[] pairs = null; 
		final Set<Long> filter = new HashSet<Long>(); 
		while(sampler.hasNext()){
			Object o = sampler.next();
			if (o != null)
				pairs = (Number[])o; 
		}
		for(int i = 0; i < pairs.length; i++){
			filter.add(new Long((Long) pairs[i]));
		}
		//
		rectangles.reset();
		return new Filter<DoublePointRectangle>(rectangles, new AbstractPredicate<DoublePointRectangle>() {
			@Override
			public boolean invoke(DoublePointRectangle arg) {
				Long key = toSFC.invoke(arg);
				boolean exists = filter.remove(key);
				return exists;
			}
		});
	}
}
