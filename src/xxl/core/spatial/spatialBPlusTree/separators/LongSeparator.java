package xxl.core.spatial.spatialBPlusTree.separators;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;

@SuppressWarnings("serial")
public class LongSeparator extends Separator {

	public static Function<Long, LongSeparator> FACTORY_FUNCTION = new AbstractFunction<Long, LongSeparator>(){
		@Override
		public LongSeparator invoke(Long argument) {
			return new LongSeparator(argument);
		}
	};
	
	
	public LongSeparator(Long sepValue) {
		super(sepValue);
	}

	@Override
	public Object clone() {
		return new LongSeparator(new Long((Long)this.sepValue()));
	}

}
