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
import java.sql.Timestamp;

/**
 * Provides a single point of contact for all XXL built-in
 * {@link xxl.core.indexStructures.BPlusTree.KeyRange BPlus KeyRange}. <br/>
 * <br/>
 * A <tt>KeyRange</tt> represents key ranges (i.e. intervals of keys). It is used to specify (range)
 * queries on the <tt>BPlusTree</tt> and to hold the key range of the data objects stored in the
 * tree in the member field <tt>rootDescriptor</tt>.<br/>
 * <br/>
 * <b>There are implementations for the java primitive types</b>
 * <ul>
 * <li>BooleanKeyRange (for <tt>boolean</tt> and <tt>Boolean</tt>)</li>
 * <li>ByteKeyRange (for <tt>byte</tt> and <tt>Byte</tt>)</li>
 * <li>DateKeyRange (for <tt>java.sql.Date</tt>)</li>
 * <li>DoubleKeyRange (for <tt>double</tt> and <tt>Double</tt>)</li>
 * <li>FloatKeyRange (for <tt>float</tt> and <tt>Float</tt>)</li>
 * <li>IntegerKeyRange (for <tt>int</tt> and <tt>Integer</tt>)</li>
 * <li>ShortKeyRange (for <tt>short</tt> and <tt>Short</tt>)</li>
 * <li>StringKeyRange (for <tt>String</tt>)</li>
 * <li>TimeKeyRange (for <tt>java.sql.Time</tt>)</li>
 * <li>TimestampKeyRange (for <tt>java.sql.Timestamp</tt>)</li>
 * </ul>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see xxl.core.indexStructures.BPlusTree BPlusTree index structure (class)
 * @see xxl.core.indexStructures.BPlusTree.KeyRange BPlusTree KeyRange
 */
public class KeyRangeFactory {

  /**
   * Returns a new instance of <tt>BooleanKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.BooleanKeyRange
   */
  public static BooleanKeyRange create(boolean min, boolean max) {
    return new BooleanKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>BooleanKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.BooleanKeyRange
   */
  public static BooleanKeyRange create(Boolean min, Boolean max) {
    return new BooleanKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>ByteKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.ByteKeyRange
   */
  public static ByteKeyRange create(byte min, byte max) {
    return new ByteKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>ByteKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.ByteKeyRange
   */
  public static ByteKeyRange create(Byte min, Byte max) {
    return new ByteKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>DoubleKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.DoubleKeyRange
   */
  public static DoubleKeyRange create(double min, double max) {
    return new DoubleKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>DoubleKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.DoubleKeyRange
   */
  public static DoubleKeyRange create(Double min, Double max) {
    return new DoubleKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>FloatKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.FloatKeyRange
   */
  public static FloatKeyRange create(float min, float max) {
    return new FloatKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>FloatKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.FloatKeyRange
   */
  public static FloatKeyRange create(Float min, Float max) {
    return new FloatKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>IntegerKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.IntegerKeyRange
   */
  public static IntegerKeyRange create(int min, int max) {
    return new IntegerKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>IntegerKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.IntegerKeyRange
   */
  public static IntegerKeyRange create(Integer min, Integer max) {
    return new IntegerKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>LongKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.LongKeyRange
   */
  public static LongKeyRange create(long min, long max) {
    return new LongKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>LongKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.LongKeyRange
   */
  public static LongKeyRange create(Long min, Long max) {
    return new LongKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>ShortKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.ShortKeyRange
   */
  public static ShortKeyRange create(short min, short max) {
    return new ShortKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>ShortKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.ShortKeyRange
   */
  public static ShortKeyRange create(Short min, Short max) {
    return new ShortKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>StringKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.StringKeyRange
   */
  public static StringKeyRange create(String min, String max) {
    return new StringKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>TimeKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.TimeKeyRange
   */
  public static TimeKeyRange create(Time min, Time max) {
    return new TimeKeyRange(min, max);
  }

  /**
   * Returns a new instance of <tt>TimestampKeyRange</tt>.
   * 
   * @param min The minimum of your range
   * @param max the maximum of your range
   * 
   * @see xxl.core.indexStructures.keyRanges.TimestampKeyRange
   */
  public static TimestampKeyRange create(Timestamp min, Timestamp max) {
    return new TimestampKeyRange(min, max);
  }

  /*
   * To prevent instancing
   */
  private KeyRangeFactory() {}

}
