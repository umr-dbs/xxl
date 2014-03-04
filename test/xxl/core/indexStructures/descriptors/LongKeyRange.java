package xxl.core.indexStructures.descriptors;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.mvbts.SimpleLoadMVBTree;

/**
 * 
 * This class is implements interface @see {@link KeyRange} and is used in the test class @see {@link SimpleLoadMVBTree}.
 *
 */
public class LongKeyRange extends KeyRange{
	
	
	public static final Function FACTORY_FUNCTION = new AbstractFunction(){
		public Object invoke(Object min, Object max){
			return new LongKeyRange((Long)min, (Long)max);
		}
	}; 
	
	
	public LongKeyRange(Long min, Long max){
		super(min, max);
	}
	
	public LongKeyRange(long min, long max){
		this(new Long(min), new Long(max));
	}
	
	@Override
	public Object clone() {
		return new LongKeyRange(((Long)this.sepValue).longValue(), ((Long)this.maxBound).longValue());
	}

}
