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

/**
 * Error which will be thrown if something has a wrong length.
 */
public class WrongLength extends Exception
{
	/**
	 * The length that was used.
	 */
	long length;

	
	/**
	 * Create an instance of this object.
	 * @param str error message.
	 * @param length the used length.
	 */
	public WrongLength(String str, long length)
	{
		super(str+" Wrong length. The length was "+length);
		this.length = length;
	}	//end constructor

}	//end class WrongLength
