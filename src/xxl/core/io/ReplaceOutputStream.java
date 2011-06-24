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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Filters an Output, so that some characters are replaces by other characters 
 * (or no characters). This is useful for example for filtering line separators.
 */
public class ReplaceOutputStream extends OutputStream {
	/** Stream to be filtered. */
	OutputStream os;
	/** Characters that are searched. */
	int chars[];
	/** Characters that are inserted instead of the searched characters. */
	int replaces[];
	
	/** 
	 * Constructs a ReplaceOutputStream. 
	 * @param os Stream to be filtered.
	 * @param chars characters that are replaced.
	 * @param replaces characters that are used instead.
	 */
	public ReplaceOutputStream(OutputStream os, int chars[], int replaces[]) {
		this.os = os;
		this.chars = chars;
		this.replaces = replaces;
	}

	/**
	 * Flushes the output stream.
	 * @throws IOException
	 */
	public void flush() throws IOException {
		os.flush();
	}
	
	/**
	 * Closes the output stream.
	 * @throws IOException
	 */
	public void close() throws IOException {
		os.close();
	}
	
	/**
	 * Writes a character into the output stream.
	 * @param b character to be written.
	 */
	public void write(int b) {
		try {
			for (int i=0; i<chars.length; i++) {
				if (b==chars[i]) {
					if (replaces[i]!=-1)
						os.write(replaces[i]);
					return;
				}
			}
			os.write(b);
		}
		catch (IOException e) {
		}
	}
}
