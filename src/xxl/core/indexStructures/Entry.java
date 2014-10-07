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

package xxl.core.indexStructures;

import java.util.Arrays;

import xxl.core.relational.schema.Schema;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.relational.tuples.ColumnComparableTuple;
import xxl.core.relational.tuples.Tuple;

/**
 * An abstraction layer over table items and tuples. By using <code>Entry</code> objects instead of
 * Object-arrays or tuples it is easier to perform queries and use the tuples key subset with the
 * nested {@link WithKey} class inside <code>Entry</code><br/>
 * <br/>
 * Suppose <code>relationalSet</code> is an instance of {@link BPlusIndexedSet}. Independent of the
 * {@link Schema} you can perform e.g. an existence check for an entry with the key
 * <code>{2,4}</code> like the following example shows
 * 
 * <pre><code>
 *  Entry.WithKey element = new Entry.WithKey(2,4);
 *  boolean result = relationalSet.contains(element);
 * </code></pre>
 * 
 * Adding data to <code>relationalSet</code> is done by
 * 
 * <pre><code>
 * relationalSet.add(new Entry(Object1, Object2, ...))
 * </code></pre>
 * 
 * <b>Note</b>: An instance of <code>Entry</code> is implemented as an array of <code>Object</code>.
 * Whereas an instance of <code>Entry.WithKey</code> is implemented as an array of
 * <code>Comparable</code>. Please ensure, that items you add to a {@link IndexedSet} contains
 * Objects which implement {@link Comparable} at least for the columns which forms the <i>Key</i>.
 * Otherwise operations on the {@link IndexedSet} will throw an exception.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class Entry {
  /**
   * Querying data from a {@link IndexedSet} is done by queries over the keys. You can easily query
   * for data with this class. Suppose <code>relationalSet</code> is an instance of
   * {@link BPlusIndexedSet}. Independent of the {@link Schema} you can perform e.g. an existence
   * check for an entry with the key <code>{2,4}</code> like the following example shows
   * 
   * <pre><code>
   *  Entry.WithKey element = new Entry.WithKey(2,4);
   *  boolean result = relationalSet.contains(element);
   * </code></pre>
   * 
   * Please note, that each component of a <code>Entry.WithKey</code> have to be {@link Comparable}
   * and should match the tables {@link Schema}.
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   */
  public static class WithKey implements Comparable {
    Comparable[] e;

    /**
     * Constructs a new instance with the given key <b>e</b>.
     * 
     * @param e The list of items which forms the key value for an entry inside an
     *        {@link IndexedSet}.
     */
    public WithKey(Comparable... e) {
      this.e = e;
    }

    /**
     * Converts this instance to an array of comparable objects.
     * 
     * @return The converted object
     */
    public Comparable[] asComparableArray() {
      return new ColumnComparableArrayTuple(e).toComparableArray();
    }

    /**
     * Converts this instance to an Tuple of comparable components.
     * 
     * @return The converted object
     */
    public ColumnComparableTuple asTuple() {
      return new ColumnComparableArrayTuple(e);
    }

    @Override
    public int compareTo(Object o) {
      return asTuple().compareTo(o);
    }
  }

  /**
   * Query for an item with the given key <b>o</b>
   * 
   * @param o The items key
   * @return An ready-to-use instance of Entry.WithKey
   */
  public static Entry.WithKey withKey(Comparable... o) {
    return new Entry.WithKey(o);
  }

  Object[] e;

  /**
   * Constructs a new entry with the given array of content. <br/>
   * <br/>
   * Please make sure, that your tables {@link Schema} match the data type of <b>e</b>
   * 
   * @param e The array of content
   */
  public Entry(Object... e) {
    this.e = e;
  }

  /**
   * Converts this object to an {@link Tuple}.
   * 
   * @return The converted object
   */
  public Tuple asTuple() {
    return new ArrayTuple(e);
  }

  public String toString() {
    return Arrays.toString(e);
  }
}
