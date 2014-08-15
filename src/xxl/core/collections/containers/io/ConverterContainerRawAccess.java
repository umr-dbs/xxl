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

package xxl.core.collections.containers.io;

import xxl.core.collections.containers.Container;
import xxl.core.io.Block;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;

/**
 * This class extends a <code>ConverterContainer</code> with
 * functionality for flushing arrays of blocks.
 */
public class ConverterContainerRawAccess extends ConverterContainer {

    /**
     * Constructs a new ConverterContainerRawAccess that decorates the specified
     * container and uses the specified converter for converting its
     * elements.
     *
     * @param container the underlying container that is used for storing
     *                  the converted elements.
     * @param converter the converter that is used for converting the
     */
    public ConverterContainerRawAccess(Container container, Converter converter) {
        super(container, converter);
    }

    public Object[] flushArrayOfBlocks(Object[] blocks) {
		Block[] blocksToWrite = new Block[blocks.length];
		for(int i = 0; i < blocks.length; i++){
			byte [] array = Converters.toByteArray(converter,blocks[i],serializationMode,bufferSize);
			blocksToWrite[i]= new Block(array, 0, array.length);
		}
		return this.container.batchInsert(blocksToWrite);
	}
}
