package xxl.core.indexStructures.descriptors;

import java.util.List;

import xxl.core.functions.Function;
import xxl.core.indexStructures.MVBTree.MVRegion;
import xxl.core.indexStructures.MVBTree.MVSeparator;
import xxl.core.indexStructures.MVBTree.Version;
import xxl.core.indexStructures.mvbts.SimpleLoadMVBTree;
/**
 * 
 * This class is implements interface @see {@link MVSeparator} and is used in the test class @see {@link SimpleLoadMVBTree}.
 *
 */
public class LongMVSeparator extends MVSeparator{
	
	/**
	 * 
	 */
	public static final Function FACTORY_FUNCTION = new Function<Object,Object>(){
		public Object invoke() { throw new UnsupportedOperationException(); }

		public Object invoke(Object arg1) { throw new UnsupportedOperationException(); }
		
		public Object invoke(Object arg1, Object arg2) { throw new UnsupportedOperationException(); }

		public Object invoke(List<? extends Object> arguments) {
			if(arguments.size() !=3 ) throw new IllegalArgumentException();
			Version insertVersion = (Version)arguments.get(0);
			Version deleteVersion = (Version)arguments.get(1);
			Long min =  new Long((Long)arguments.get(2));
			return new LongMVSeparator(insertVersion, deleteVersion, min);	
		}
	};
	
	
	/**
	 * 
	 * @param insertVersion
	 * @param sepValue
	 */
	public LongMVSeparator(Version insertVersion, Long sepValue) {
		super(insertVersion, sepValue);
	}
	
	/**
	 * 
	 * @param insertVersion
	 * @param deleteVersion
	 * @param sepValue
	 */
	public LongMVSeparator(Version insertVersion, Version deleteVersion, Long sepValue) {
		super(insertVersion, deleteVersion, sepValue);
	}

	
	@Override
	public Object clone() {
		Long copySepValue = new Long((Long)sepValue());
		return new LongMVSeparator((this.insertVersion() != null)?
				(Version)this.insertVersion().clone() : null , 
				(this.deleteVersion() != null)? (Version)this.deleteVersion().clone() : null, 
						copySepValue);
	}

	

}
