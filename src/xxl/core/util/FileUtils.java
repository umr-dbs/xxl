/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
 * of Mathematics and Computer Science University of Marburg Germany
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * http://code.google.com/p/xxl/
 */

package xxl.core.util;

import static java.lang.Math.max;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Contains some useful static utility methods for dealing with files.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class FileUtils {

  /**
   * Returns the file extension of the given <i>filename</i> or empty string if there is no
   * extension. Please note: The last "." is also removed. <br/>
   * <br/>
   * <b>Example</b><br/>
   * <code>getFileExtension("/MyFile.More.bin") = "bin"</code><br/>
   * <code>getFileExtension("/MyFile") = ""</code>
   * 
   * @param filename A file name string
   * @return File extension of <code>filename</code>
   */
  public static String getFileExtension(String filename) {
    StringBuilder extension = new StringBuilder(filename);
    return filename.substring(extension.length()
        - max(0, extension.reverse().indexOf(".")));
  }

  /**
   * Returns the path (excluding the filename) of a given file. The last directory separator symbol
   * is also removed. If the given filename is inside root directory the empty string is returned.<br/>
   * <br/>
   * <b>Example</b><br/>
   * <code>getFileExtension("/Folder1/MyFile.bin") = "/Folder1"</code><br/>
   * <code>getFileExtension("/MyFile.bin") = ""</code>
   * 
   * @param filenamePath A file path string
   * @return Directory in which the file is
   */
  public static String getFilePath(String filenamePath) {
    return filenamePath.substring(0, filenamePath.lastIndexOf(File.separator));
  }

  /**
   * Removes the given File or Directory with all sub directories and files.
   * 
   * @param file File name or directory (can contain files and sub folder)
   * @return true and only true if everything was deleted
   * @throws FileNotFoundException
   */
  public static boolean removeFile(File file) throws FileNotFoundException {
    if (!file.exists())
      throw new FileNotFoundException(file.getAbsolutePath());
    boolean returnValue = true;
    if (file.isDirectory()) for (File f : file.listFiles())
      returnValue = returnValue && removeFile(f);
    return returnValue && file.delete();
  }

  /**
   * Creates and returns a new empty (and unique) temp-Dir. 
   * 
   * @return %SYSTEM_TEMP_DIR_ROOT%xx/%TIMESTAMP_OF_MKTEMPDIR_METHOD_CALL%
   * @throws IOException If the directory already exists or the system is unable to create the directory.
   */
  public static String mkTempDir() throws IOException {
    String randomTempPath =
        System.getProperty("java.io.tmpdir") + "xxl/"
            + System.currentTimeMillis() + "/";

    if (!(new File(randomTempPath).exists())) {
      if (!new File(randomTempPath).mkdirs()) throw new IOException();
    } else
      throw new IOException("\"" + randomTempPath + "\" exists already.");

    return randomTempPath;
  }
}
