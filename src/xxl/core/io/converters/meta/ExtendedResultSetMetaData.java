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

package xxl.core.io.converters.meta;

import java.sql.ResultSetMetaData;

import xxl.core.relational.metaData.ExtendedColumnMetaData;

/**
 * An object that provides information about the columns of a given table. Each column stores
 * specific information about the value of a tuple inside the table.
 * 
 * This extension to standard <tt>ResultSetMetaData</tt> also provides the method
 * {@link ExtendedResultSetMetaData#getContentLength(int) getMaxColumnStringLength(int)} to handle
 * the maximum length if the column is declared as a string type. Please mark, that
 * <tt>ExtendedResultSetMetaData</tt> stores <i>all</i> columns for a given table whereas
 * {@link ExtendedColumnMetaData} is for a single column.
 * 
 * @see java.sql.ResultSetMetaData
 * @see ExtendedColumnMetaData
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public interface ExtendedResultSetMetaData extends ResultSetMetaData {

  /**
   * Returns the <code>ExtendedColumnMetaData</code> for the specific column with index
   * <code>columnIndex</code>. Each column has it's own <code>ExtendedColumnMetaData</code>
   * descriptor which the contains column meta data.
   * 
   * @param columnIndex The column index
   * @return The meta data descriptor for the column with index <b>columnIndex</b>
   */
  ExtendedColumnMetaData getColumnMetaData(int columnIndex);

  /**
   * Returns the maximum length of a string object that can be imported into this column
   * 
   * @param columnIndex The column index
   * @return The maximum length of a string
   */
  int getContentLength(int columnIndex);

}
