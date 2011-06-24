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

/**
 * This class provides a skeleton implementation based upon the
 * {@link xxl.core.functions.DecoratorFunction DecoratorFunction} for functions
 * providing meta data. Functions inherited from this class are used to provide
 * meta data information used for example by
 * {@link xxl.core.cursors.MetaDataCursor MetaDataCursors} by implementing
 * <code>Object getMetaData()</code> from the
 * {@link xxl.core.util.metaData.MetaDataProvider MetaDataProvider-interface}.
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 * @param <M> the type of the meta data provided by this function.
 */
public abstract class FunctionMetaDataFunction<P, R, M> extends DecoratorFunction<P, R> implements MetaDataFunction<P, R, M> {

	/**
	 * Constructs a new meta data function.
	 * 
	 * @param function the function to decorate as
	 *        {@link xxl.core.util.metaData.MetaDataProvider meta data provider}.
	 */
	public FunctionMetaDataFunction(Function<P, R> function){
		super(function);
	}

	/**
	 * Returns the meta data for this class.
	 * 
	 * @return the meta data of the class implementing this method.
	 */
	public abstract M getMetaData();
	
}
