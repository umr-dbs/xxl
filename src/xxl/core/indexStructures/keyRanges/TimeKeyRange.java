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

import java.sql.Time;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;

/**
 * {@link xxl.core.indexStructures.BPlusTree.KeyRange KeyRange} represents key ranges (i.e.
 * intervals of keys). It is used to specify (range) queries on the <tt>BPlusTree</tt> and to hold
 * the key range of the data objects stored in the tree in the member field <tt>rootDescriptor</tt>.<br/>
 * <br/>
 * 
 * This class extends <tt>KeyRange</tt> for using <b>Time</b> as keys.<br/>
 * <br/>
 * 
 * You will find a <b>list of all available implemented KeyRanges</b> in
 * {@link xxl.core.indexStructures.keyRanges.KeyRangeFactory KeyRangeFactory} class.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see xxl.core.indexStructures.BPlusTree.KeyRange
 * @see java.sql.Time
 */
public class TimeKeyRange extends KeyRange {

  /**
   * Used for a functional like programming style which creates in this case a new ranges.
   */
  public static Function<Object, TimeKeyRange> FACTORY_FUNCTION =
      new AbstractFunction<Object, TimeKeyRange>() {
        @Override
        public TimeKeyRange invoke(Object argument0, Object argument1) {
          return new TimeKeyRange((argument0 instanceof Long)
              ? (Long) argument0
              : ((Time) argument0).getTime(), (argument1 instanceof Long)
              ? (Long) argument1
              : ((Time) argument1).getTime());
        }
      };

  /**
   * @see xxl.core.indexStructures.BPlusTree.KeyRange
   */
  public TimeKeyRange(long min, long max) {
    super(min, max);
  }

  /**
   * @see xxl.core.indexStructures.BPlusTree.KeyRange
   */
  public TimeKeyRange(Long min, Long max) {
    super(min, max);
  }

  /**
   * @see xxl.core.indexStructures.BPlusTree.KeyRange
   */
  public TimeKeyRange(Time min, Time max) {
    super(min.getTime(), max.getTime());
  }

  @Override
  public Object clone() {
    return new TimeKeyRange(new Time((Long) this.sepValue), new Time(
        (Long) this.maxBound));
  }
}
