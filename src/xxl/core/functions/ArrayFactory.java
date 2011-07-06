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

import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.sources.EmptyCursor;

/** 
 * This class provides a factory method for typed arrays. The ArrayFactory
 * creates an array and fills it with objects. An iterator of arguments can be
 * passed to the invoke method of this class to create the objects which are
 * stored in the array.
 * 
 * @param <T> the component type of the array to be returned.
 * @deprecated replaced by {@link Functional.ArrayFactory}
 */
@SuppressWarnings("serial")
@Deprecated
public class ArrayFactory<T> extends AbstractFunction<Object, T[]> {

	/**
	 * A factory method that gets one parameter and returns an array.
	 */
	protected Function<Object, T[]> newArray;

	/**
	 * A factory method that gets one parameter and returns an object used for
	 * initializing the array.
	 */
	protected Function<Object, ? extends T> newObject;

	/**
	 * Creates a new ArrayFactory.
	 * 
	 * @param newArray factory method that returns an array.
	 * @param newObject factory method that returns the elements of the array.
	 */
	public ArrayFactory(Function<Object, T[]> newArray, Function<Object, ? extends T> newObject) {
		this.newArray = newArray;
		this.newObject = newObject;
	}

    /**
     * Returns the result of the ArrayFactory as a typed array. This method
     * calls the invoke method of the newArray function which returns an array
     * of typed objects. After this, the invoke method of the newObject
     * function is called, so many times as the length of the array. As
     * parameter to the function an element of the iterator is given that is
     * specified as second argument.
     * 
     * @param arguments the arguments to this function. The first arguments
     *        must be an object used as argument to the newArray function.
     *        The second argument must be an iterator holding the arguments to
     *        the newObject function.
     * @return the initialized array.
     */
	@Override
	public T[] invoke(List<? extends Object> arguments) {
		if (arguments.size() != 2)
			throw new IllegalArgumentException("the function must be invoked with an object and an iterator.");
		T[] array = newArray.invoke(arguments.get(0));
		Iterator<?> newObjectArguments = (Iterator<?>)arguments.get(1);
		for (int i = 0; i < array.length; i++)
			array[i] = newObject.invoke(newObjectArguments.hasNext() ? newObjectArguments.next() : null);	//call invoke on newObject with argument-Object from arguments-Iterator
		return array;
	}
    
    /**
     * Returns the result of the ArrayFactory as a typed array. This method
     * calls the invoke method of the newArray function using the given
     * argument which returns an array of typed objects. After this, the invoke
     * method of the newObject function is called without any arguments, so
     * many times as the length of the array.
     * 
     * @param argument the argument to the newArray function.
     * @return the initialized array.
     */
	@Override
	public T[] invoke(Object argument) {
		return invoke(argument, EmptyCursor.DEFAULT_INSTANCE);
	}

    /**
     * Returns the result of the ArrayFactory as a typed array. This method
     * calls the invoke method of the newArray function using the argument
     * <code>null</code> which returns an array of typed objects. After this,
     * the invoke method of the newObject function is called without any 
     * arguments, so many times as the length of the array.
     * 
     * @return the initialized array.
     */
	@Override
	public T[] invoke() {
		return invoke((Object)null);
	}
}
