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

package xxl.core.util.memory;

/**
 * Models an Object (e.g. an operator or a collection) whose memory usage can
 * be monitored. 
 */
public interface MemoryMonitorable {

	/**
	 * A constant signing that the memory usage for an object is unknown.
	 */
	public static final int SIZE_UNKNOWN = -1;
	
	/**
	 * A constant that should be used as key to store the objectsize in the 
	 * composite meta data if the object provides metadata.
	 * @see xxl.core.util.metaData.MetaDataProvider MetaDataProvider-interface
	 */
	public static final String OBJECT_SIZE = "OBJECT_SIZE";

	/**
	 * Returns the amount of memory, which is currently used by this object.
	 * This method can be called from the strategy of the memory manager to
	 * obtain useful information for distributing main memory among the
	 * memory using objects.
	 * 
	 * @return Returns the amount of memory currently used by this object
	 *         (in bytes).
 	 */
	public abstract int getCurrentMemUsage();
	
}
