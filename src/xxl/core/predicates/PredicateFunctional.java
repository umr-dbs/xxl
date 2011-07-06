package xxl.core.predicates;

import xxl.core.functions.Functional;

public class PredicateFunctional {
	private PredicateFunctional(){}
	
	public interface NullaryPredicate extends Functional.NullaryFunction<Boolean>{
	}
	
	public interface UnaryPredicate<I> extends Functional.UnaryFunction<I, Boolean>{
	}
	
	public interface BinaryPredicate<I0, I1> extends Functional.BinaryFunction<I0, I1, Boolean>{
	}

	public interface TrinaryPredicate<I0, I1, I2> extends Functional.TrinaryFunction<I0, I1, I2, Boolean>{
	}
}
