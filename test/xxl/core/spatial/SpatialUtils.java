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

public class SpatialUtils {
	
	
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
	public static Function<DoublePointRectangle, Long> toSFC = Functions.toFunction(xxl.core.spatial.histograms.RGOhist.zCruveSFC(universe, BITS_PRO_DIM));
	
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
