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

package xxl.core.io.fat.errors;

import xxl.core.io.fat.FAT;

/**
 * Error which will be thrown if an operation is used that is only valid for an other FAT type.
 */
public class WrongFATType extends Exception
{
	/**
	 * Create an instance of this object.
	 * @param isFatType the actual FAT type.
	 */
	public WrongFATType(byte isFatType)
	{
		super("The actual fat type is "+isFatType+", but needed is one\n of the following types "+FAT.FAT12+", "+FAT.FAT16+", or "+FAT.FAT32);
	}	//end constructor
	
		
	/**
	 * Create an instance of this object.
	 * @param isFatType the actual FAT type.
	 * @param neededFatType the FAT type that is needed.
	 */
	public WrongFATType(byte isFatType, byte neededFatType)
	{
		super("The actual fat type is "+isFatType+", but needed is fat type "+neededFatType);
	}	//end constructor

}	//end class WrongFATType
