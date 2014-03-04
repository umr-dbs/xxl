package xxl.core.indexStructures.descriptors;

import java.util.List;









import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.MVBTree.MVRegion;
import xxl.core.indexStructures.mvbts.SimpleLoadMVBTree;
/**
 * 
 * This class is implements interface @see {@link MVRegion} and is used in the test class @see {@link SimpleLoadMVBTree}.
 *
 */
public class LongMVRegion extends MVRegion {
	
	public static final Function FACTORY_FUNCTION = new AbstractFunction<Object,Object>() {
		public Object invoke() {throw new UnsupportedOperationException();}
		public Object invoke(Object arg1) {throw new UnsupportedOperationException();}
		public Object invoke(Object arg1,Object arg2) {throw new UnsupportedOperationException();}
		public Object invoke(List<? extends Object> arguments) {
			if(arguments.size()!=4) throw new IllegalArgumentException();
			LongVersion beginVersion=(LongVersion)arguments.get(0);
			LongVersion endVersion=(LongVersion)arguments.get(1);
			Long min= (Long)arguments.get(2);
			Long max= (Long)arguments.get(3);
			return new LongMVRegion(beginVersion, endVersion, min, max);	
		}
	};

	public LongMVRegion(LongVersion beginVersion, LongVersion endVersion, Long min, Long max) {
		super(beginVersion, endVersion, min, max);	
	}
				
	public Object clone() {
		LongVersion begin=(LongVersion)beginVersion().clone();
		LongVersion end=isDead()?(LongVersion)endVersion().clone(): null;
		Long min=new Long(((Long)minBound()).longValue());
		Long max=this.isDefinite()? new Long(((Long)maxBound()).longValue()): null;
		return new LongMVRegion(begin, end, min, max);
	}
}