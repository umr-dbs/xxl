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

package xxl.core.functions;

import java.io.Serializable;
import java.util.Iterator;

import xxl.core.predicates.PredicateFunctional.UnaryPredicate;

public class Functional {

	private Functional(){}
	
	public interface NullaryFunction<T> extends Serializable {
		public T invoke();
	}
	
	public interface UnaryFunction<I, O> extends Serializable {
		public O invoke(I arg);
	}
	
	public interface BinaryFunction<I0, I1, O> extends Serializable {
		public O invoke(I0 arg0, I1 arg1);
	}

	public interface TrinaryFunction<I0, I1, I2, O> extends Serializable {
		public O invoke(I0 arg0, I1 arg1, I2 arg2);
	}
	
	public static <I0,I1,O> UnaryFunction<I0,O> rightBind(final BinaryFunction<I0,I1,O> f, final I1 rightArg){
		return new UnaryFunction<I0,O>(){
			public O invoke(I0 leftArg) {
				return f.invoke(leftArg, rightArg);
			};
		};
	}

	public static <I0,I1,O> UnaryFunction<I1,O> leftBind(final BinaryFunction<I0,I1,O> f, final I0 leftArg){
		return new UnaryFunction<I1,O>(){
			public O invoke(I1 rightArg) {
				return f.invoke(leftArg, rightArg);
			};
		};
	}

	public static <I,T,O> UnaryFunction<I,O> compose(final UnaryFunction<I, T> f, final UnaryFunction<T,O> g){
		return new UnaryFunction<I,O>(){
			public O invoke(I arg) {
				return g.invoke(f.invoke(arg));
			};
		};
	}
	
	public static class Identity<I> implements UnaryFunction<I, I>{
		public I invoke(I arg) {
			return arg;
		};
	}
	public static class ArrayFactory<I1, I2, T> implements BinaryFunction<I1, Iterator<I2>, T[]>{

		/**
		 * A factory method that gets one parameter and returns an array.
		 */
		protected UnaryFunction<I1, T[]> newArrayFactory;
		
		/**
		 * A factory method that gets one parameter and returns an object used for
		 * initializing the array.
		 */
		protected UnaryFunction<I2, ? extends T> newObjectFactory;
		
		/**
		 * Creates a new ArrayFactory.
		 * 
		 * @param newArray factory method that returns an array.
		 * @param newObject factory method that returns the elements of the array.
		 */
		public ArrayFactory(UnaryFunction<I1, T[]> newArrayFactory, UnaryFunction<I2, ? extends T> newObjectFactory) {
			this.newArrayFactory = newArrayFactory;
			this.newObjectFactory = newObjectFactory;
		}
		/**
	     * Returns the result of the ArrayFactory as a typed array. This method
	     * calls the invoke method of the newArray function which returns an array
	     * of typed objects. After this, the invoke method of the newObject
	     * function is called, so many times as the length of the array. As
	     * parameter to the function an element of the iterator is given that is
	     * specified as second argument.
	     * 
	     * @param must be an object used as argument to the newArrayFactory function.
	     *        The second argument must be an iterator holding the arguments to
	     *        the newObject function.
	     * @param must be an iterator holding the arguments to the newObjectFactory function.
	     * @return the initialized array.
	     */
		@Override
		public T[] invoke(I1 arg0, Iterator<I2> arg1) {
			T[] array = newArrayFactory.invoke(arg0);
			for (int i = 0; i < array.length; i++)
				array[i] = newObjectFactory.invoke(arg1.hasNext() ? arg1.next() : null);	
			return array;
		}
	}

	public static class Iff<I0, I1, I2, O> implements TrinaryFunction<I0, I1, I2, O> {

		/**
		 * providing the functionality of an if-clause.
		 */
		protected UnaryPredicate<? super I0> predicate;
		
		/**
		 * using in the case of TRUE.
		 */
		protected UnaryFunction<? super I1, ? extends O> f1;

		/**
		 * using in the case of FALSE.
		 */
		protected UnaryFunction<? super I2, ? extends O> f2;
		
		public Iff(UnaryPredicate<? super I0> predicate, UnaryFunction<? super I1, ? extends O> f1, UnaryFunction<? super I2, ? extends O> f2){
			this.predicate = predicate;
			this.f1 = f1;
			this.f2 = f2;
		}

		@Override
		public O invoke(I0 arg0, I1 arg1, I2 arg2) {
			return predicate.invoke(arg0) ? 
					f1.invoke(arg1) :
				    f2.invoke(arg2);
		}
	}

}
