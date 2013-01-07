package xxl.core.indexStructures.testData;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.SimpleLoadMVBTree;
import xxl.core.indexStructures.MVBTree.MVSeparator;

/**
 * 
 * This class is implements interface @see {@link Separator} and is used in the test class @see {@link SimpleLoadMVBTree}.
 *
 */
public class LongSeparator extends Separator{
	
	public static final Function FACTORY_FUNCTION = new AbstractFunction(){
		public Object invoke(Object value){
			return new LongSeparator((Long)value);
		}
	}; 
	
	
	public LongSeparator(Long value){
		super(value);
	}
	
	
	public LongSeparator(long value){
		this(new Long(value));
	}

	@Override
	public Object clone() {
		return new LongSeparator(((Long)this.sepValue).longValue());
	}
}
