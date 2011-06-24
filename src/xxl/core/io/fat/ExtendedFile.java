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

package xxl.core.io.fat;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import xxl.core.io.fat.util.ExtendedFileFilter;
import xxl.core.io.fat.util.ExtendedFilenameFilter;
import xxl.core.io.fat.util.StringOperations;

/**
 * An abstract representation of file and directory pathnames.
 *
 * This class presents an abstract, this raw-system-dependent view of hierarchical
 * pathnames. An abstract pathname has two components:
 * An optional prefix string, "\\", and
 * a sequence of zero or more string names.
 *
 * Each name in an abstract pathname except for the last denotes a directory;
 * the last name may denote either a directory or a file.  The empty
 * abstract pathname has no prefix and an empty name sequence.
 * 
 * A pathname, whether abstract or in string form, is always absolute.
 * An absolute pathname is complete in that no other information is
 * required in order to locate the file that it denotes.
 */
public class ExtendedFile implements Comparable
{
	/**
	 * The FileSystem object representing the platform's local file system.
	 */
	protected FATDevice device;
		 
	/**
	 * The complete filename without the device name.
	 */
	protected String completeName;
	
	/**
	 * The mode of the file it is either "r" which means read access only or
	 * it is "rw" which means read and write access.
	 */
	protected String mode = FATDevice.FILE_MODE_READ;
	
	/**
	 * The system-dependent default name-separator character. This field is
	 * initialized to contain the first character of the value of the system
	 * property file.separator. On UNIX systems the value of this
	 * field is '/'; on Win32 systems it is '\'.
	 */
	public static final char separatorChar = System.getProperty("file.separator").charAt(0);
	
	
	/**
	 * Creates a new ExtendedFile instance. The given pathname is used to
	 * navigate through the directory structure. If the given string is
	 * the empty string, then the result is the empty pathname.
	 * @param device instance of FATDevice.
	 * @param pathname a pathname string.
	 * @throws NullPointerException if the pathname argument is null.
	 */
	protected ExtendedFile(FATDevice device, String pathname)// throws Exception
	{
		if (pathname == null)
		    throw new NullPointerException();
		
		this.device = device;
		completeName = StringOperations.removeDeviceName(pathname);
		if (completeName.startsWith(System.getProperty("file.separator")))
		    completeName = completeName.substring(1);
	}	//end constructor

	
	/**
	 * Creates a new ExtendedFile instance from a parent pathname string
	 * and a child pathname string.
	 *
	 * If parent is null then the new ExtendedFile instance is created
	 * as if by invoking the single-argument ExtendedFile constructor on
	 * the given child pathname string.
	 *
	 * Otherwise the parent pathname string is taken to denote
	 * a directory, and the child pathname string is taken to
	 * denote either a directory or a file. If parent is the empty string then
	 * it is handled as if it is the root directory. Otherwise the two pathname
	 * strings are merged to get one pathname.
	 * @param device instance of FATDevice.
	 * @param parent the parent pathname string.
	 * @param child the child pathname string.
	 * @throws NullPointerException if child is null.
	 */
	protected ExtendedFile(FATDevice device, String parent, String child)
	{
		if (child == null)
			throw new NullPointerException();
		this.device = device;
		
		if (parent != null)
		{
			if (parent.equals(""))
			{
				completeName = StringOperations.extractFileName(child);
				if (completeName.startsWith(System.getProperty("file.separator")))
					completeName = completeName.substring(1);
			}
		    else
			{
				completeName = StringOperations.removeDeviceName(parent);
				if (completeName.startsWith(System.getProperty("file.separator")))
					completeName = completeName.substring(1);
				String fileName = StringOperations.extractFileName(child);
				if (!fileName.startsWith(System.getProperty("file.separator")) && !completeName.endsWith(System.getProperty("file.separator")))
					fileName = System.getProperty("file.separator") + fileName;
				completeName += fileName;
			}
		}
		else
		{
			completeName = StringOperations.removeDeviceName(child);
			if (completeName.startsWith(System.getProperty("file.separator")))
				completeName = completeName.substring(1);
		}
	}
	
	
	/**
	 * Returns the name of the file or directory denoted by this pathname.
	 * This is just the last name in the pathname's name
	 * sequence. If the pathname's name sequence is empty, then the empty
	 * string is returned.
	 * @return the name of the file or directory denoted by this pathname,
	 * or the empty string if this pathname's name sequence is empty.
	 */
	public String getName()
	{
		return StringOperations.extractFileName(completeName);
	}	//end getName()
	
	
	/**
	 * Returns the pathname string of this abstract pathname's parent, or
	 * null if this pathname does not name a parent directory.
	 * The parent of an pathname consists of the
	 * pathname's prefix, if any, and each name in the pathname's name
	 * sequence except for the last. At last the root directory is the
	 * parent if the pathname string is empty or is a filename only.
	 * @return the pathname string of the parent directory named by this
	 * pathname, or null if this pathname does not name a parent.
	 */
	public String getParent()
	{
		String fileName = StringOperations.extractFileName(completeName);
		if (fileName.equals(""))
			return null;
		String path = StringOperations.extractPath(completeName);
		if (!path.startsWith(System.getProperty("file.separator")))
			path = System.getProperty("file.separator") + path;
		if (FileSystem.isUnixDeviceName(device.getDeviceName()))
			return device.getDeviceName() + path;
		else
			return device.getDeviceName() + ":" + path;
	}	//end getParent()
	
	
	/**
	 * Returns the pathname of this pathname's parent, or null if this
	 * pathname does not name a parent directory.
	 * The parent of an pathname consists of the pathname's prefix, if
	 * any, and each name in the pathname's name sequence except for
	 * the last. If the name sequence is empty then the pathname does
	 * not name a parent directory.
	 * @return the pathname of the parent directory named by this
	 * pathname, or null if this pathname does not name a parent.
	 */
	public ExtendedFile getParentFile()
	{
		String p = this.getParent();
		if (p == null)
			return null;
		return new ExtendedFile(device, p);
	}	//end getParentFile()

	
	/**
	 * Converts this pathname into a pathname string.
	 * @return the string representation of this pathname.
	 */
	public String getPath()
	{
		if (FileSystem.isUnixDeviceName(device.getDeviceName()))
			return device.getDeviceName() + System.getProperty("file.separator") + completeName;
		else
			return device.getDeviceName() + ":" + System.getProperty("file.separator") + completeName;
	}	//end getPath()

	
	/**
	 * Returns the absolute pathname string of this pathname inclusive
	 * the name of the device.
	 * If this pathname is the empty pathname then only the name of the device
	 * is returned.
	 * @return the absolute pathname string inclusive the name of the device.
	 */
	public String getAbsolutePath()
	{
		if (FileSystem.isUnixDeviceName(device.getDeviceName()))
			return device.getDeviceName() + System.getProperty("file.separator") + completeName;
		else
			return device.getDeviceName() + ":" + System.getProperty("file.separator") + completeName;
	}	//end getAbsolutePath()
	
	
	/**
	 * Returns the absolute form of this pathname.
	 * @return the absolute pathname denoting the same file or
	 * directory as this pathname.
	 */
	public ExtendedFile getAbsoluteFile()
	{
		return new ExtendedFile(device, getAbsolutePath());
	}	//end getAbsoluteFile()
	
	
	/**
	 * Converts this abstract pathname into a file: URL. The
	 * exact form of the URL is system-dependent. If it can be determined that
	 * the file denoted by this abstract pathname is a directory, then the
	 * resulting URL will end with a slash.
	 * @return a URL object representing the equivalent file URL.
	 * @throws MalformedURLException if the path cannot be parsed as a URL.
	 * @see java.net.URL
	 */
	public URL toURL() throws MalformedURLException
	{
		String path = getAbsolutePath();
	    if (ExtendedFile.separatorChar != '/')
	        path = path.replace(ExtendedFile.separatorChar, '/');
	
	    if (!path.startsWith("/"))
	        path = "/" + path;
	
	    if (!path.endsWith("/") && isDirectory())
	        path = path + "/";
	
		return new URL("file", "", path);
	}	//end toURL()
	
	
	/**
	 * Tests whether the file denoted by this pathname exists.
	 * @return true if and only if the file denoted by this
	 * pathname exists; false otherwise.
	 */
	public boolean exists()
	{
		return device.fileExists(completeName);
	}	//end exists()
	
	
	/**
	 * Tests whether the file denoted by this pathname is a directory.
	 * @return true if and only if the file denoted by this
	 * pathname exists and is a directory; false otherwise.
	 */
	public boolean isDirectory()
	{
		return device.isDirectory(completeName);
	}	//end isDirectory()
	
	
	/**
	 * Tests whether the file denoted by this pathname is a normal file.
	 * A file is normal if it is not a directory and, in addition,
	 * satisfies other criteria. Any non-directory file created by
	 * this class is guaranteed to be a normal file.
	 * @return true if and only if the file denoted by this pathname
	 * exists and is a normal file; false otherwise.
	 */
	public boolean isFile()
	{
		return device.isFile(completeName);
	}	//end isFile()
	
	
	/**
	 * Tests whether the file named by this pathname is a hidden file.
	 * @return true if and only if the file denoted by this pathname
	 * is hidden.
	 */
	public boolean isHidden()
	{
		return device.isHidden(completeName);
	}	//end isHidden()
	
	
	/**
	 * Returns the time that the file denoted by this pathname was
	 * last modified.
	 * @return  a long value representing the time the file was last
	 * modified, measured in milliseconds since the epoch
	 * (00:00:00 GMT, January 1, 1970), or 0L if the file does not
	 * exist or if an I/O error occurs.
	 */
	public long lastModified()
	{
		return device.getWriteTime(completeName);
	}	//end lastModified()
	
	
	/**
	 * Returns the time that the file denoted by this pathname was created.
	 * @return a long value representing the time the file was created,
	 * measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970),
	 * or 0L if the file does not exist or if an I/O error occurs.
	 */
	public long created()
	{
		return device.getCreationTime(completeName);
	}	//end created()
	
	
	/**
	 * Returns the length of the file denoted by this pathname.
	 * @return the length, in bytes, of the file denoted by this
	 * pathname, or 0L if the file does not exist.
	 */
	public long length()
	{
		try
		{
			return device.length(completeName);
		}
		catch (Exception e)
		{
			return 0L;
		}
	}	//end length()
	
	
	/**
	 * Creates a new, empty file named by this pathname if and only if a
	 * file with this name does not yet exist and there is enough free space. 
	 * @return true if the named file does not exist and was successfully
	 * created; false otherwise.
	 */
	public boolean createNewFile()
	{
		try
		{
			device.createFile(completeName);
		}
		catch (xxl.core.io.fat.errors.DirectoryException e)
		{
			return false;
		}
		return true;
	}	//end createNewFile()
	
	
	/**
	 * Deletes the file or directory denoted by this pathname. If
	 * this pathname denotes a directory, then the directory must be empty in
	 * order to be deleted.
	 * @return true if and only if the file or directory is
	 * successfully deleted; false otherwise.
	 */
	public boolean delete()
	{
		return device.delete(completeName);
	}	//end delete()
	
	
	/**
	 * Returns an array of strings naming the files and directories in the
	 * directory denoted by this pathname.
	 * If this pathname does not denote a directory, then this method returns
	 * null. Otherwise an array of strings is returned, one for each file or
	 * directory in the directory. Names denoting the directory itself and
	 * the directory's parent directory are not included in the result.
	 * Each string is a file name rather than a complete path.
	 * There is no guarantee that the name strings in the resulting array
	 * will appear in any specific order; they are not, in particular,
	 * guaranteed to appear in alphabetical order.
	 * @return an array of strings naming the files and directories in the
	 * directory denoted by this pathname. The array will be empty if the
	 * directory is empty. Returns null if this pathname does not denote a
	 * directory, or if an I/O error occurs.
	 */
	public String[] list()
	{
		return device.list(completeName);
	}	//end list()
	
	
	/**
	 * Returns an array of strings naming the files and directories in the
	 * directory denoted by this pathname that satisfy the specified
	 * filter.  The behavior of this method is the same as that of the
	 * {@link #list()} method, except that the strings in the
	 * returned array must satisfy the filter. If the given
	 * filter is null then all names are accepted.
	 * Otherwise, a name satisfies the filter if and only if the value
	 * true results when the {@link ExtendedFilenameFilter} method
	 * of the filter is invoked on this pathname and the name of a file
	 * or directory in the directory that it denotes.
	 * @param filter a filename filter.
	 * @return an array of strings naming the files and directories, in the
	 * directory denoted by this pathname, that were accepted by the given
	 * filter. The array will be empty if the directory is empty or if no
	 * names were accepted by the filter. Returns null if this pathname
	 * does not denote a directory, or if an I/O error occurs.
	 */
	public String[] list(ExtendedFilenameFilter filter)
	{
		String names[] = list();
		if ((names == null) || (filter == null))
		   	return names;
	
		ArrayList v = new ArrayList();
		for (int i = 0 ; i < names.length ; i++)
	    	if (filter.accept(this, names[i]))
			v.add(names[i]);
		return (String[])(v.toArray(new String[0]));
	}	//end list(ExtendedFilenameFilter filter)
	
	
	/**
	 * Returns an array of pathnames denoting the files in the directory
	 * denoted by this pathname.
	 * If this pathname does not denote a directory, then this method
	 * returns null. Otherwise an array of ExtendedFile objects is
	 * returned, one for each file or directory in the directory.
	 * Pathnames denoting the directory itself and the directory's parent
	 * directory are not included in the result.
	 * There is no guarantee that the name strings in the resulting array
	 * will appear in any specific order; they are not, in particular,
	 * guaranteed to appear in alphabetical order.
	 * @return an array of pathnames denoting the files and directories
	 * in the directory denoted by this pathname. The array will be empty
	 * if the directory is empty. Returns null if this pathname does not
	 * denote a directory, or if an I/O error occurs.
	 */
	public ExtendedFile[] listFiles()
	{
		String[] ss = list();
		if (ss == null)
			return null;
		int n = ss.length;
		ExtendedFile[] fs = new ExtendedFile[n];
		for (int i = 0; i < n; i++)
		{
			fs[i] = new ExtendedFile(device, getPath(), ss[i]);
		}

		return fs;
	}	//end listFiles()

	
	/**
	 * Returns an array of pathnames denoting the files and
	 * directories in the directory denoted by this pathname that
	 * satisfy the specified filter. The behavior of this method is the
	 * same as that of the {@link #listFiles()} method, except
	 * that the pathnames in the returned array must satisfy the filter.
	 * If the given filter is null then all
	 * pathnames are accepted. Otherwise, a pathname satisfies the filter
	 * if and only if the value true results when the
	 * {@link ExtendedFilenameFilter} method of the filter is
	 * invoked on this abstract pathname and the name of a file or
	 * directory in the directory that it denotes.
	 * @param filter a filename filter.
	 * @return an array of pathnames denoting the files and directories
	 * in the directory denoted by this pathname. The array will be empty
	 * if the directory is empty. Returns null if this pathname does not
	 * denote a directory, or if an I/O error occurs.
	 */
	public ExtendedFile[] listFiles(ExtendedFilenameFilter filter)
	{
		String ss[] = list();
		if (ss == null)
			return null;
		ArrayList v = new ArrayList();
		for (int i = 0 ; i < ss.length ; i++)
		{
	    	if ((filter == null) || filter.accept(this, ss[i]))
	    	{
				v.add(new ExtendedFile(device, StringOperations.extractPath(completeName), ss[i]));
	    	}
		}
		
		return (ExtendedFile[])(v.toArray(new ExtendedFile[0]));
	}	//end listFiles(ExtendedFilenameFilter filter)

	
	/**
	 * Returns an array of pathnames denoting the files and
	 * directories, in the directory denoted by this pathname, that
	 * satisfy the specified filter. The behavior of this method is the
	 * same as that of the {@link #listFiles()} method, except
	 * that the pathnames in the returned array must satisfy the filter.
	 * If the given filter is null then all pathnames are accepted.
	 * Otherwise, a pathname satisfies the filter if and only if the
	 * value true results when the {@link ExtendedFilenameFilter}
	 * method of the filter is invoked on the pathname.
	 * @param filter a filename filter.
	 * @return an array of pathnames denoting the files and directories
	 * in the directory denoted by this pathname and satisfy the 
	 * specified filter. The array will be empty if 
	 * the directory is empty. Returns null if this pathname
	 * does not denote a directory, or if an I/O error occurs.
	 */
	public ExtendedFile[] listFiles(ExtendedFileFilter filter)
	{
		String ss[] = list();
		if (ss == null)
			return null;
		ArrayList v = new ArrayList();
		for (int i = 0 ; i < ss.length ; i++)
		{
	    	ExtendedFile f = new ExtendedFile(device, StringOperations.extractPath(completeName), ss[i]);
	    	if ((filter == null) || filter.accept(f))
	    	{
				v.add(f);
	    	}
		}
		return (ExtendedFile[])(v.toArray(new ExtendedFile[0]));
	}	//end listFiles(ExtendedFileFilter filter)
	
	
	/**
	 * Creates the directory named by this pathname.
	 * @return true if and only if the directory was
	 * created; false otherwise.
	 */
	public boolean mkdir()
	{
		return device.makeDirectory(completeName);
	}	//end mkdir()
	
	
	/**
	 * Creates the directory named by this pathname, including any
	 * necessary but nonexistent parent directories. Note that if this
	 * operation fails it may have succeeded in creating some of the necessary
	 * parent directories.
	 * @return true if and only if the directory was created, along with all
	 * necessary parent directories; false otherwise.
	 */
	public boolean mkdirs()
	{
		if (exists())
	    	return false;
	
		if (mkdir())
	    	return true;
		String parent = getParent();
		return (parent != null) && (new ExtendedFile(device, parent).mkdirs() && mkdir());
	}	//end mkdirs()
	
	
	/**
	 * Renames the file denoted by this pathname. If the entry named by this pathname
	 * is an existing file, it will be deleted and a new file/directory given by dest
	 * will be created (with all subdirectories if they don't exist). Since ExtendedFile
	 * objects always belong to one device the device will and can not be changed. In case
	 * the dest file/directory already exists, the old file will not be deleted and the
	 * renaming operation will not processed. In case the
	 * entry named by this pathname is a directory the directory is only deleted if it is
	 * empty.
	 * @param dest the new pathname for the named file.
	 * @return true if and only if the renaming succeeded; false
	 * otherwise.
	 * @throws NullPointerException if parameter dest is null.
	 */
	public boolean renameTo(ExtendedFile dest)
	{
		if (dest == null)
			throw new NullPointerException();
				
		boolean result = device.renameTo(	StringOperations.removeDeviceName(getAbsolutePath()),
											StringOperations.removeDeviceName(dest.getAbsolutePath()));
		if (result)
		{
			completeName = StringOperations.removeDeviceName(dest.getAbsolutePath());
		
			if (completeName.startsWith(System.getProperty("file.separator")))
		    	completeName = completeName.substring(1);
		}					
		
		return result;
	}	//end renameTo(ExtendedFile dest)
	
	
	/**
	 * Sets the last-modified time of the file or directory named by this
	 * pathname. The modified time will be written to disk immediately.
	 * @param time the new last-modified time, measured in milliseconds since
	 * the epoch (00:00:00 GMT, January 1, 1970).
	 * @return true if and only if the operation succeeded; false otherwise.
	 * @throws IllegalArgumentException if the argument is negative.
	 */
	public boolean setLastModifiedTime(long time)
	{
		if (time < 0)
			throw new IllegalArgumentException("Negative time");
		return device.setLastWriteTime(completeName, time, true);
	}	//end setLastModifiedTime(long time)
		
	
	/**
	 * Compares two pathnames without the device names lexicographically.
	 * @param pathname the pathname to be compared to this pathname.
	 * @return zero if the argument is equal to this pathname, a
	 * value less than zero if this pathname is lexicographically less
	 * than the argument, or a value greater than zero if this pathname
	 * is lexicographically greater than the argument.
	 */
	public int compareTo(ExtendedFile pathname)
	{
		String thisAbsPath = StringOperations.removeDeviceName(getAbsolutePath());
		return thisAbsPath.compareTo(StringOperations.removeDeviceName(pathname.getAbsolutePath()));
	}	//end compareTo(ExtendedFile pathname)

	
	/**
	 * Compares this pathname to another object. If the other object
	 * is an pathname, then this function behaves like {@link
	 * #compareTo(ExtendedFile)}. Otherwise, it throws a ClassCastException,
	 * since pathnames can only be compared to pathnames.
	 * @param o the Object to be compared to this pathname.
	 * @return  if the argument is an pathname, returns zero if the
	 * argument is equal to this pathname, a value less than zero if
	 * this pathname is lexicographically less than the argument, or
	 * a value greater than zero if this pathname is
	 * lexicographically greater than the argument.
	 * @throws ClassCastException if the argument is not an pathname.
	 */
	public int compareTo(Object o)
	{
		return compareTo((ExtendedFile)o);
	}	//end compareTo(Object o)
	
	
	/**
	 * Tests this pathname for equality with the given object.
	 * Returns true if and only if the argument is not null and is an
	 * pathname that denotes the same file or directory as this
	 * pathname.
	 * @param obj the object to be compared with this pathname.
	 * @return true if and only if the objects are the same;
	 * false otherwise.
	 */
	public boolean equals(Object obj)
	{
		if ((obj != null) && (obj instanceof ExtendedFile))
		{
	    	return compareTo((ExtendedFile)obj) == 0;
		}
		return false;
	}	//end equals(Object obj)
	
		
	/**
	 * Returns the pathname string of this pathname. This is just the
	 * string returned by the getPath method.
	 * @return the string form of this pathname.
	 */
	public String toString()
	{
		return getPath();
	}	//end toString()
	
	
	/**
	 * Return the attribute of this pathname file or directory.
	 * @return attribute stored at the directory entry.
	 */
	public byte getAttribute()
	{
		return device.getAttribute(completeName);
	}	//end getAttribute()
	
	
	/**
	 * Return the date the file was last modified.
	 * @return DirectoryDate object which contains the date information.
	 */
	public DirectoryDate getLastModifiedDate()
	{
		return new DirectoryDate(lastModified());
	}	//end getLastModifiedDate()
	
	
	/**
	 * Return the time the file was last modified.
	 * @return DirectoryTime object which contains the time information.
	 */
	public DirectoryTime getLastModifiedTime()
	{
		return new DirectoryTime(lastModified());
	}	//end getLastModifiedTime()
		
	
	/**
	 * Return the time the file was created.
	 * @return DirectoryTime object which contains the time information.
	 */
	public DirectoryTime getCreationTime()
	{
		return new DirectoryTime(created());
	}	//end getCreationTime()
	
	
	/**
	 * Return the date the file was created.
	 * @return DirectoryDate object which contains the date information.
	 */
	public DirectoryDate getCreationDate()
	{
		return new DirectoryDate(created());
	}	//end getCreationDate()
	
}	//end class ExtendedFile
