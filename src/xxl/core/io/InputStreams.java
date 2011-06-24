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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Some useful methods which use InputStreams (for example diff).
 */
public class InputStreams {

	/**
	 * You cannot generate instances of this class.
	 */
	private InputStreams() {
	}
	
	/**
	 * Compares the content of two InputStreams byte per byte.
	 * @param i1 the first input stream
	 * @param i2 the second input stream
	 * @return true iff both InputStreams produce the same bytes.
	 */
	public static boolean compareInputStreams(InputStream i1, InputStream i2) {			
		int c1,c2;
		try {
			do {
				c1 = i1.read();
				c2 = i2.read();
				if (c1!=c2)
					return false;
			} while (c1!=-1);
			// in the case of leaving the slope, c1==c2==-1
		}
		catch (IOException e) {
			return false;
		}
		
		return true;
	}

	/**
	 * Compares two files byte for byte.
	 * @param name1 filename of the first file.
	 * @param name2 filename of the second file.
	 * @return true iff both files have the same content.
	 */
	public static boolean compareFiles(String name1, String name2) {	
		boolean equal = false;
		
		try {
			InputStream i1 = new BufferedInputStream(new FileInputStream(name1));
			InputStream i2 = new BufferedInputStream(new FileInputStream(name2));
				
			equal = compareInputStreams(i1,i2);
				
			i1.close();
			i2.close();
		}
		catch (IOException e) {}
		
		return equal;
	}
	
	/**
	 * Compares two files byte for byte.
	 * @param file1 the first file.
	 * @param file2 the second file.
	 * @return true iff both files have the same content.
	 */
	public static boolean compareFiles(File file1, File file2) {	
		boolean equal = false;
		
		try {
			InputStream i1 = new BufferedInputStream(new FileInputStream(file1));
			InputStream i2 = new BufferedInputStream(new FileInputStream(file2));
				
			equal = compareInputStreams(i1,i2);
				
			i1.close();
			i2.close();
		}
		catch (IOException e) {}
		
		return equal;
	}
}
