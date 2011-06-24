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

import xxl.core.util.metaData.CompositeMetaData;

/**
 * This class provides a CMDFunction (= CompositeMetaDataFunction). It is an
 * implementation of MetaDataFunction that uses a hash map to store the meta
 * data.
 *  
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 * @param <I> the type of the identifiers used by the composite meta data.
 * @param <M> the type of the metadata fragments used by the composite meta
 *        data.
 */
public class CMDFunction<P, R, I, M> extends FunctionMetaDataFunction<P, R, CompositeMetaData<I, M>> {

	/**
	 * A composite meta data object, that stores the meta data.
	 */
	protected CompositeMetaData<I, M> cmd;
	
	/**
	 * Wraps the specified function to a composite meta data function, that
	 * returns the given composite meta data objects when
	 * <code>getMetaData</code> is called.
	 * 
	 * @param function the function to be wrapped.
	 * @param cmd the meta data of the new function.
	 */
	public CMDFunction(Function<P, R> function, CompositeMetaData<I, M> cmd) {
		super(function);
		this.cmd = cmd;
	}

	/**
	 * Wraps the specified function to a composite meta data function.
	 * 
	 * @param function the function to be wrapped.
	 */
	public CMDFunction(Function<P, R> function) {
		this(function, new CompositeMetaData<I, M>());
	}

	/**
	 * Returns the meta data for this class represented by a composite meta
	 * data object.
	 * 
	 * @return the meta data for this class represented by a composite meta
	 *         data object.
	 */
	@Override
	public CompositeMetaData<I, M> getMetaData() {		
		return cmd;
	}
}
