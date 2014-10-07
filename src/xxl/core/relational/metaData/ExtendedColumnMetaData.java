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

package xxl.core.relational.metaData;

import xxl.core.io.converters.meta.ExtendedResultSetMetaData;

/**
 * An object that provides information about a <i>single</i> columns of a given table.
 * 
 * This extension to standard <tt>ColumnMetaData</tt> also provides the method
 * {@link ExtendedColumnMetaData#getMaxContainingStringLength() getColumnCharacterCount()} method
 * which returns the maximum length for a string entry if this column is a <b>string</b> typed
 * column. Please mark, that <tt>ExtendedColumnMetaData</tt> stores a <i>single</i> column whereas
 * {@link ExtendedResultSetMetaData} is designed to hold a bundle of columns..
 * 
 * @see java.sql.ResultSetMetaData
 * @see ExtendedResultSetMetaData
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public interface ExtendedColumnMetaData extends ColumnMetaData {

  /**
   * Gets the maximum character length for a string value entry if this column is declared as a
   * <b>string</b> typed one.
   * 
   * @return Maximum character length
   */
  int getMaxContainingStringLength();

  /**
   * Sets the maximum character length for a string value entry if this column is declared as a
   * <b>string</b> typed one.
   * 
   * <br/>
   * <br/>
   * <b>Note:</b> Please ensure that each string content you'll write into this column is shorter or
   * equal to <code>length</code>. Otherwise the import of an entry will fail.
   * 
   * @param length Maximum length
   */
  void setMaxContainingStringLength(int length);

}
