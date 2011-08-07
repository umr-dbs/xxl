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

package xxl.core.io;

import java.io.File;

import xxl.core.util.XXLSystem;

/**
 * Class that contains common functionality for the i/o examples.
 */
public class Common {
	/** This class is not instanciable */
	private Common () {
	}

	/** 
	 * Constructs the output directory and returns the path. The returned path contains a file separator 
	 * at the end.
	 *
	 * @return String - the OutPath
	 */
	public static String getOutPath() {
		String path = XXLSystem.getOutPath() + System.getProperty("file.separator") +
			"output" + System.getProperty("file.separator") + 
			"applications" + System.getProperty("file.separator") + 
			"io";
		File f = new File(path);
		f.mkdirs();
		return path + System.getProperty("file.separator");
	}

	/** 
	 * Returns the relational data path. The returned path contains a file separator 
	 * at the end.
	 *
	 * @return String - the data path to relational sample files
	 */
	public static String getRelationalDataPath() {
		return XXLSystem.getRootPath() + System.getProperty("file.separator") + 
			"data" + System.getProperty("file.separator") +
			"relational" + System.getProperty("file.separator");
	}
}
