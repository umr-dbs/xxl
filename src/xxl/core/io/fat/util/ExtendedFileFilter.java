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

package xxl.core.io.fat.util;

import java.io.FileFilter;

import xxl.core.io.fat.ExtendedFile;

/**
 * A filter for pathnames. 
 * Instances of this interface may be passed to the listFiles(ExtendedFileFilter)
 * method of the ExtendedFileFilter class. 
 */
public interface ExtendedFileFilter extends FileFilter
{
	/**
	 * Tests whether or not the specified pathname should be included in a pathname list.
     * @param pathname the pathname to be tested.
     * @return true if and only if pathname should be included.
	 */
	public boolean accept(ExtendedFile pathname);
}	//end class ExtendedFileFilter
