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

import java.io.PrintStream;
import java.io.RandomAccessFile;

/**
 * This class logs all calls to a FilesystemOperations object.
 */
public class LogFilesystemOperations implements FilesystemOperations {
	
	/**
	 * FilesystemOperations object to be logged.
	 */
	protected FilesystemOperations fso;
	
	/**
	 * Output Stream
	 */
	protected PrintStream ps;
	
	/**
	 * Log every access or only errors?
	 */
	protected boolean logOnlyErrors;
	
	/**
	 * Constructs an object which logs filesystem operations.
	 * @param fso FilesystemOperations object which is logged.
	 * @param ps PrintStream which is used for the output.
	 * @param logOnlyErrors determines if only errors are reported or everything.
	 */
	public LogFilesystemOperations (FilesystemOperations fso, PrintStream ps, boolean logOnlyErrors) {
		this.fso = fso;
		this.ps = ps;
		this.logOnlyErrors = logOnlyErrors;
	}

	/**
	 * Opens a file and returns a RandomAccessFile.
	 * @param fileName the name of the file.
	 * @param mode the access mode ("r" or "rw").
	 * @return the new RandomAccessFile or null, if the operation was not successful.
	 */
	public RandomAccessFile openFile(String fileName, String mode) {
		RandomAccessFile raf = fso.openFile(fileName,mode);
		if ( (raf==null) || (!logOnlyErrors) )
			ps.println("FilesystemOperations: openFile('"+fileName+"','"+mode+"')="+raf);
		return raf;
	}

	/**
	 * Determines if a file exists or not.
	 * @param fileName the name of the file to be checked.
	 * @return true iff the file exists.
	 */
	public boolean fileExists(String fileName) {
		boolean ret = fso.fileExists(fileName);
		if ( (!ret) || (!logOnlyErrors) )
			ps.println("FilesystemOperations: fileExists('"+fileName+"')="+ret);
		return ret;
	}

	/**
	 * Renames the name of a file to a new name.
	 * @param oldName the old name of the file.
	 * @param newName the new name of the file.
	 * @return true iff the operation completed successfully.
	 */
	public boolean renameFile(String oldName, String newName) {
		boolean ret = fso.renameFile(oldName,newName);
		if ( (!ret) || (!logOnlyErrors) )
			ps.println("FilesystemOperations: renameFile('"+oldName+"','"+newName+"')="+ret);
		return ret;
	}

	/**
	 * Deletes a file.
	 * @param fileName the name of the file.
	 * @return true iff the operation completed successfully.
	 */
	public boolean deleteFile(String fileName) {
		boolean ret = fso.deleteFile(fileName);
		if ( (!ret) || (!logOnlyErrors) )
			ps.println("FilesystemOperations: deleteFile('"+fileName+"')="+ret);
		return ret;
	}
}
