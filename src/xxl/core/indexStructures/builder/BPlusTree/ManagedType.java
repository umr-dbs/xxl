/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
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

package xxl.core.indexStructures.builder.BPlusTree;

import xxl.core.indexStructures.BPlusTree;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.io.converters.meta.KeyFunctionFactory;
import xxl.core.io.converters.MeasuredTupleConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.relational.JavaType;

/**
 * To specify which data type is managed by a generic index structure use a subclass of this
 * abstract base class. For example, a {@link BPlusTree} manages generic tuples ({@link TupleType})
 * as well as standard java data types ({@link PrimitiveType}). This class includes all of the data
 * type-specific methods that are needed for a specific use.<br/>
 * <br/>
 * <br/>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see TupleType Use tuples for an index structure
 * @see PrimitiveType Use primitive types like Integer or Float for an index structure
 * @see BPlusTreeBuilder How to setup a BPlus tree easy and fast
 * 
 */
public abstract class ManagedType {

  /**
   * The content type enumeration which represents if the index structure deals with primitive data
   * (e.g. Integer) or tuples
   */
  public enum ContentClass {
    CONTENT_CLASS_COMPLEX, CONTENT_CLASS_PRIMITIVE
  }

  /**
   * Returns an array of indices which should be used as compounded key for indexing. Please node
   * that this returns an integer array in which the first item is at position zero whereas the
   * first <i>column</i> index for the table is one not zero.
   * 
   * <br/>
   * <br/>
   * <b>Caution</b>: This methods should only be called if there is a way to compound a key through
   * indices. For primitive types a call to this function will throw an <code>Exception</code>
   * 
   * @return The indices of the compounded key
   */
  public abstract int[] getCompoundKeyIndices();

  /**
   * Returns the current <code>ContentClass</code> which indicates the managed data type for this
   * index structure
   * 
   * This <code>Class</code> determines the index managed type class. This could be primitive data
   * types (CONTENT_CLASS_PRIMITIVE) or complex (tuple) data types (CONTENT_CLASS_COMPLEX). To get
   * information about <i>which</i> type is actually manages, call {@link #getContentClassSubType()}
   * .
   * 
   * @return Data type content class
   */
  public abstract ContentClass getContentClass();

  /**
   * @return The concrete {@link JavaType} which is managed by the index structure.
   */
  public abstract JavaType getContentClassSubType();

  /**
   * When storing an index structure to hard drive on top of the content the meta data will be
   * written. This meta data contains a (unique) <code>ContentType</code> string which identifies the
   * managed data. If the index structure manages e.g. tuples, "complex/tuples" could the
   * identifier. This is used to check the configuration when restoring an index structure from
   * persistent storage.
   * 
   * @return Unique identifier for the managed content
   */
  public abstract String getContentType();

  /**
   * Returns the measured converter which should be used by the index structure tree and is needed
   * to calculate the size for each component of the tuple or a single data type.
   * 
   * 
   * @return The converter
   * 
   * @see MeasuredTupleConverter
   */
  abstract MeasuredConverter getDataConverter();

  /**
   * Returns the key function factory which contains ready to use functions according to the meta
   * data and the compound key indices (if used).
   * 
   * 
   * @return the key function factory
   */
  abstract KeyFunctionFactory getKeyFunctionFactory();

  /**
   * Returns the table meta data if available. Please note: calling this method for primitive data
   * types will throw an <code>Exception</code>
   * 
   * @return Meta data
   */
  public abstract ExtendedResultSetMetaData getMetaData();

  /**
   * Returns the table name
   * 
   * @return Table name
   */
  public abstract String getTableName();

  /**
   * Set the indices of the column which should be used as the compounded key for indexing (if
   * possible, otherwise an <code>Exception</code> is thrown).<br/>
   * <br/>
   * 
   * The columns are compared lexicographically in descending order of their key indices given by
   * <code>compoundKeyIndices</code>. The order of the table columns is the order in which you
   * define it in <code>compoundKeyIndices</code> array. Thus, the first column (according to the
   * first array item value) is compared with the first column of another tuple (according to the
   * first array item value).
   * 
   * <br/>
   * <br/>
   * If there is more than one key column, lets say two, the second columns are compared if there is
   * equality in the first column for both tuples.
   * 
   * <br/>
   * <br/>
   * Please mark that e.g. a <code>compoundKeyIndices</code> array <code>[1,2]</code> first compares
   * the <i>first</i> column and after this (if necessary) the <i>second</i> column. Whereas
   * <code>[2,1]</code> forces to compare at first the <i>second</i> column and after this (if
   * necessary) the <i>first</i> column. Here, it must be ensured that the components of a tuple,
   * which form a key, have to be <b>comparable</b> and also that there will be no duplicate values
   * for a (compounded) key, so that a (compounded) key uniquely identifies a tuple.
   * 
   * <br/>
   * <br/>
   * 
   * @param compoundKeyIndices An array that contains the column indices. Please note that the order
   *        of the columns matters for comparing and that each column index is in bounds of the
   *        table column count.
   * 
   */
  public abstract void setCompoundKey(int[] compoundKeyIndices);

}
