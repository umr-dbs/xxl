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

package xxl.core.indexStructures.keyRanges;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.relational.tuples.ColumnComparableTuple;

/**
 * This class represents key ranges (i.e. intervals of keys) for
 * {@link xxl.core.relational.tuples.ColumnComparableTuple}. It is used to specify (range) queries
 * on the BPlusTree, see {@link xxl.core.indexStructures.BPlusTree.KeyRange KeyRange}.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class TupleKeyRangeFunction extends KeyRange {

  /**
   * Used for a functional like programming style which creates in this case a new ranges.
   */
  public static Function<ColumnComparableTuple, TupleKeyRangeFunction> FACTORY_FUNCTION =
      new AbstractFunction<ColumnComparableTuple, TupleKeyRangeFunction>() {
        @Override
        public TupleKeyRangeFunction invoke(ColumnComparableTuple argument0,
            ColumnComparableTuple argument1) {
          return new TupleKeyRangeFunction(argument0, argument1);
        }
      };

  /**
   * @see xxl.core.indexStructures.BPlusTree.KeyRange
   */
  public TupleKeyRangeFunction(Comparable min, Comparable max) {
    super(min, max);
  }

  /**
   * @see xxl.core.indexStructures.BPlusTree.KeyRange#clone()
   */
  @Override
  public Object clone() {
    return new TupleKeyRangeFunction(this.sepValue, this.maxBound);
  }

}
