package xxl.core.spatial.spatialBPlusTree.separators;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;

public class LongKeyRange extends KeyRange{

	public static Function<Long, LongKeyRange> FACTORY_FUNCTION = new AbstractFunction<Long, LongKeyRange>() {
		@Override
		public LongKeyRange invoke(Long argument0, Long argument1) {
			return new LongKeyRange(argument0, argument1);
		}
	}; 
	
	
	
	public LongKeyRange(Long min, Long max) {
		super(min, max);
	}

	@Override
	public Object clone() {
		return new LongKeyRange(new Long ((Long)this.minBound()),  new Long ((Long)this.maxBound()));
	}

}
