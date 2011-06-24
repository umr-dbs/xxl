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

import java.util.List;

import xxl.core.util.metaData.MetaDataProvider;

/**
 * A marker interface for functions providing metadata.
 *
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 * @param <M> the type of the meta data provided by this function.
 */
public interface MetaDataFunction<P, R, M> extends Function<P, R>, MetaDataProvider<M> {
	
	@Override
	public abstract R invoke(List<? extends P> arguments);

	@Override
	public abstract R invoke();

	@Override
	public abstract R invoke(P argument);

	@Override
	public abstract R invoke(P argument0, P argument1);

	@Override
	public abstract M getMetaData();

}
