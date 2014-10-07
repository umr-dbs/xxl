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

package xxl.core.functions.concrete;

import xxl.core.functions.AbstractFunction;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;

/**
 * Functional programming style lambda function to extract the key of a given tuple.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */

@SuppressWarnings("serial")
public class TupleGetKeyFunction<Tuple, ColumnComparableTuple>
    extends
    AbstractFunction<xxl.core.relational.tuples.Tuple, ColumnComparableTuple> {

  /*
   * The indices of the columns which should be used as compounded key
   */
  private int mColumnIndices[] = new int[] {-1};

  public TupleGetKeyFunction(int[] columnIndex) {
    for (int componentIndex : columnIndex)
      if (componentIndex < 1) throw new IllegalArgumentException();

    mColumnIndices = columnIndex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ColumnComparableTuple invoke(xxl.core.relational.tuples.Tuple argument) {
    try {
      Object[] content = argument.toArray();
      Comparable[] comparableSubset = new Comparable[mColumnIndices.length];

      for (int i = 0; i < comparableSubset.length; i++)
        comparableSubset[i] = (Comparable) content[mColumnIndices[i] - 1];

      return (ColumnComparableTuple) new ColumnComparableArrayTuple(
          comparableSubset);

    } catch (Exception e) {
      throw new RuntimeException(
          "The given tuple does not contain comparable objects at all.");
    }
  }
}
