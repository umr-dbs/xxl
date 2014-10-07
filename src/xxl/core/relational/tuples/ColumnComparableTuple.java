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

package xxl.core.relational.tuples;



/**
 * A tuple is a hard copy (materialization) of an (existing) tuple of a result set.<br/>
 * <br/>
 * This extension to {@link xxl.core.relational.tuples.Tuple Tuple} ensures that two given tuples
 * can be compared and a single tuple can be converted into an array of {@link java.lang.Comparable
 * Comparables} by {@link #toComparableArray()}.<br/>
 * <br/>
 * 
 * The getXXX methods retrieve the column values like result set does. You only can retrieve values
 * using the index number of the column. Columns are numbered from 1.<br/>
 * <br/>
 * 
 * A tuple throws only RuntimeExceptions. If a column is requested, that does not exist, an
 * IndexOutOfBoundsException is thrown.<br/>
 * <br/>
 * 
 * Empty tuples are not allowed. By creating a tuple from a result set, it must be ensured, that the
 * result set is not empty. If so, the constructor of the Tuple-class throws a RuntimeException.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public interface ColumnComparableTuple extends Tuple, Comparable {

  /**
   * Converts the tuple into an array of {@link java.lang.Comparable Comparables}
   * 
   * @return An array which contains the components of this tuple converted into
   *         <code>Comparable</code>. This is used for key handling. <br/>
   *         <b>Note:</b> Contrary to <code>tuple</code> the first component index starts with 0 as
   *         usually for arrays
   */
  public Comparable[] toComparableArray();
}
