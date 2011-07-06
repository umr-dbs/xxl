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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;

/**
 * This class provides a functional <i>switch</i> statement. Arbitrary
 * functions can be registered to the switch function using an unique
 * identifier for each function. When the switch function is invoked, the first
 * parameter of the invocation method is taken as the identifier of the
 * internally stored function to be executed. If there is not such a case a
 * function representing the default case is invoked.<br />
 * <b>Note,</b> that the iterations returned by the methods
 * <code>identifiers</code> and <code>functions</code> deliver their data in
 * the same order as it is put in the switch function. When a map is specified
 * to the constructor that already contains some functions the order of them is
 * specified by the map's {@link Map#keySet() keySet} method.
 * 
 * @param <I> the type of the identifiers used for identifying the different
 *        cases.
 * @param <R> the result type of the switch function, i.e., a supertype for all
 *        given functions modeling the cases of the switch function.
 */
@SuppressWarnings("serial")
public class Switch<I, R> extends AbstractFunction<Object, R> {

	/**
	 * A map that is used to store the functions representing the different
	 * cases of this switch function.
	 */
	protected Map<I, Function<Object, ? extends R>> functions;

	/**
	 * An array-list storing the identifiers of the functions representing the
	 * different cases of the switch function. This list is internally used to
	 * save the order the functions representing the different cases of this
	 * switch function are put in it.
	 */
	protected ArrayList<I> identifiers;
	
	/**
	 * The default case of this switch function. It is invoked when is does not
	 * contain a function for a given identifier.
	 */
	protected Function<Object, ? extends R> defaultFunction;
	
	/**
	 * Creates a new switch function using the functions and their identifiers
	 * stored in the given map to represent its different cases and the
	 * specified function for representing the default case. The order of the
	 * stored functions is given by the map's {@link Map#keySet() keySet}
	 * method.
	 * 
	 * @param functions a map storing the functions and their identifiers used
	 *        to represent its different cases.
	 * @param defaultFunction a function representing the default case of this
	 *        switch function.
	 */
	public Switch(Map<I, Function<Object, ? extends R>> functions, Function<Object, ? extends R> defaultFunction) {
		this.functions = functions;
		this.identifiers = new ArrayList<I>(functions.keySet());
		this.defaultFunction = defaultFunction;
	}
	
	/**
	 * Creates a new switch function without a specified case and a function
	 * representing the default case that returns an
	 * <code>IllegalArgumentException</code> if it is invoked. In order to
	 * replace the default case by a meaningful one, call the
	 * {@link #putDefault(Function) putDefault} with the desired function.
	 */
	public Switch() {
		this(
			new HashMap<I, Function<Object, ? extends R>>(),
			new AbstractFunction<Object, R>() {
				@Override
				public R invoke(List<? extends Object> arguments) throws IllegalArgumentException {
					throw new IllegalArgumentException("the switch function does not contain a function for the given identifier and no default function is specified");
				}
			}
		);
	}
	
	/**
	 * Associates the specified function with the case specified by the given
	 * identifier in this switch function. If the switch function previously
	 * contained a function dealing with this case, the old function is
	 * replaced by the specified function.
	 *
	 * @param identifier an unique identifier associated with the case the
	 *        specified function deals with.
	 * @param function the function that deals with the case associated with
	 *        the given identifier.
	 * @return the function that previously dealt with the case the specified
	 *         identifier is associated with.
	 */
	public Function<Object, ? extends R> put(I identifier, Function<Object, ? extends R> function) {
		if (!functions.containsKey(identifier))
			identifiers.add(identifier);
		return functions.put(identifier, function);
	}
	
	/**
	 * Associates the specified function with the default case of the switch
	 * function and returns the function previously associated with this case.
	 *
	 * @param function the function that deals with the default case.
	 * @return the function that previously dealt with the default case.
	 */
	public Function<Object, ? extends R> putDefault(Function<Object, ? extends R> function) {
		Function<Object, ? extends R> defaultFunction = this.defaultFunction;
		this.defaultFunction = function;
		return defaultFunction;
	}
	
	/**
	 * Removes the function associated with the specified identifier from this
	 * switch function. If the switch function contains a function dealing with
	 * this case, the function is removed and returned, otherwise
	 * <code>null</code> is returned.
	 *
	 * @param identifier an unique identifier associated with the function
	 *        to be removed.
	 * @return the function associated with the specified identifier or
	 *         <code>null</code> if this switch function contains no function
	 *         dealing with this case.
	 */
	public Function<Object, ? extends R> remove(I identifier) {
		if (functions.containsKey(identifier))
			identifiers.remove(identifier);
		return functions.remove(identifier);
	}
	
	/**
	 * Returns the function that deals with the case identified by the given
	 * identifier. If the switch function associates no case with this
	 * identifier the function representing the default case is returned.
	 *
	 * @param identifier an unique identifier associated with the case whose
	 *        function should be returned.
	 * @return the function dealing with the case identified by the given
	 *         identifier or the function representing the default case if
	 *         there is no such case.
	 */
	public Function<Object, ? extends R> get(I identifier) {
		if (functions.containsKey(identifier))
			return functions.get(identifier);
		return defaultFunction;
	}
	
	/**
	 * Returns the function representing the default case of this switch
	 * function.
	 *
	 * @return the function representing the default case of this switch
	 *         function.
	 */
	public Function<Object, ? extends R> getDefault() {
		return defaultFunction;
	}
	
	/**
	 * Returns <code>true</code> if and only if this switch function contains a
	 * function that deals with the case identified by the given identifier. If
	 * it associates no case with this identifier <code>false</code> is
	 * returned.
	 * 
	 * @param identifier an unique identifier associated with the case that
	 *        should be tested for being handled by this switch function.
	 * @return <code>true</code> if and only if this switch function contains a
	 *         function that deals with the case identified by the given
	 *         identifier, otherwise <tt>false</tt>.
	 */
	public boolean contains(I identifier) {
		return identifiers.contains(identifier);
	}
	
    /**
     * Returns an iteration over the identifiers associated with the cases of
     * this switch function. This iteration delivers its data in the same order
     * as it is put in the switch function. When a map is specified to the
     * constructor that already contains some functions the order of them is
     * specified by the map's {@link Map#keySet() keySet} method.
     * 
     * @return an iteration containing the identifiers associated with the
     *         cases of this switch function.
     */
	public Cursor<I> identifiers() {
		return new AbstractCursor<I>() {
			protected int index = 0;
			
			@Override
			protected boolean hasNextObject() {
				return index < identifiers.size();
			}

			@Override
			protected I nextObject() {
				return identifiers.get(index++);
			}

			@Override
			public void remove() throws IllegalStateException {
				super.remove();
				functions.remove(identifiers.remove(--index));
			}
			
			@Override
			public boolean supportsRemove() {
				return true;
			}
		};
	}
	
    /**
     * Returns an iteration over the functions dealing with the cases of this
     * switch function. This iteration delivers its data in the same order
     * as it is put in the switch function. When a map is specified to the
     * constructor that already contains some functions the order of them is
     * specified by the map's {@link Map#keySet() keySet} method.
     * 
     * @return an iteration containing the functions dealing with the cases of
     *         this switch function.
     */
	@SuppressWarnings("unchecked")
	public Cursor<Function<Object, ? extends R>> functions() {
		return new Mapper<I, Function<Object, ? extends R>>(
			new AbstractFunction<I, Function<Object, ? extends R>>() {
				@Override
				public Function<Object, ? extends R> invoke(I identifier) {
					return functions.get(identifier);
				}
			},
			identifiers()
		);
	}
	
	/**
	 * Returns the result of the switch function as an object. The first
	 * element of the given <code>Object</code> array is used to identify the
	 * case of the switch function that should be invoked. Thereafter all
	 * arguments are used to invoke the function that represents the desired
	 * case. Because at least one argument is needed by the switch function to
	 * identify the case to be invoked, this method throws an
	 * <code>IllegalArgumentException</code> if it is invoked with zero
	 * arguments or an empty <code>Object</code> array.
	 * 
	 * @param arguments the arguments to the switch function whereas the first
	 *        element of the <code>Object</code> array identifies the case of
	 *        the switch function to be invoked with the remaining elements.
	 * @return the return value of the function representing the case
	 *         identified by the first element of the given <code>Object</code>
	 *         array.
	 * @throws IllegalArgumentException if the invoke method is called without
	 *         any arguments or with an empty <code>Object</code> array or
	 *         the switch function associates no case with the given identifier
	 *         and the function representing the default case is not set.
	 */
	@Override
	@SuppressWarnings("unchecked") // the first argument must be the identifier
	public R invoke(List<? extends Object> arguments) throws IllegalArgumentException {
		if (arguments.size() == 0)
			throw new IllegalArgumentException("switch functions must be invoked with at least one argument identifying the internally stored function to be invoked.");
		return get((I)arguments.get(0)).invoke(arguments);
	}
	
}
