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

import java.sql.SQLException;

import xxl.core.functions.Function;
import xxl.core.functions.concrete.TupleGetKeyFunction;
import xxl.core.indexStructures.keyRanges.TupleKeyRangeFunction;
import xxl.core.indexStructures.separators.TupleSeparator;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.MeasuredTupleConverter;
import xxl.core.relational.tuples.ComparableTuples;
import xxl.core.relational.tuples.Tuple;

/**
 * Provides a bundle of functions and a converter to work with {@link Tuple}.<br/>
 * <br/>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class TupleKeyFunctionFactory extends KeyFunctionFactory {

  /*
   * Converter which knows the (byte) size of tuples to store and load
   */
  private MeasuredTupleConverter mKeyConverter;

  /*
   * Functions needed for initialization of BPlus Tree
   */
  protected Function mKeyFunction;
  protected Function mKeyRangeFunction;

  protected Function mSeparatorFunction;

  /**
   * Builds the needed function for initialization of BPlusTree depending of the given
   * <b>tupleMetaData</b> and the columns marked as the (compound) key. </br></br> This functions
   * are available with {@link #getKeyFunction()}, {@link #getKeyRangeFunction(int)} and
   * {@link #getKeyValueSeparatorFunction()}. The needed converter to load and store tuple with the
   * schema of <b>tupleMetaData</b> is accessable through {@link #getKeyConverter()}
   * 
   * @param tupleMetaData The meta data for tuples to be stored, loaded and indexed
   * @param keyIndex A index array of the columns which should be used as the compounded key
   */
  public TupleKeyFunctionFactory(ExtendedResultSetMetaData tupleMetaData,
      int[] keyIndex) {
    try {
      mKeyFunction = new TupleGetKeyFunction<>(keyIndex);
      mKeyConverter =
          new MeasuredTupleConverter(ComparableTuples.project(tupleMetaData, keyIndex));
      mSeparatorFunction = TupleSeparator.FACTORY_FUNCTION;
      mKeyRangeFunction = TupleKeyRangeFunction.FACTORY_FUNCTION;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * @return The MeasuredConverter built up with the given information
   */
  @Override
  public MeasuredConverter getKeyConverter() {
    return mKeyConverter;
  }

  /**
   * @return The key function built up with the given information
   */
  @Override
  public Function getKeyFunction() {
    return mKeyFunction;
  }

  /**
   * @return The KeyRange function built up with the given information
   */
  @Override
  public Function getKeyRangeFunction(int compoundKeyColumnIndex) {
    return mKeyRangeFunction;
  }

  /**
   * @return The KeyValueSeparator function built up with the given information
   */
  @Override
  public Function getKeyValueSeparatorFunction() {
    return mSeparatorFunction;
  }

}
