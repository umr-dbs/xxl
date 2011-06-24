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

public class Functional {

	public interface NullaryFunction<T>{
		public T invoke();
	}
	
	public interface UnaryFunction<I, O>{
		public O invoke(I arg);
	}
	
	public interface BinaryFunction<I0, I1, O>{
		public O invoke(I0 arg0, I1 arg1);
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
}
