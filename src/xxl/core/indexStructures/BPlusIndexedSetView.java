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

import static xxl.core.util.ConvertUtils.autoCast;
import static xxl.core.util.ConvertUtils.autoComparable;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import xxl.core.cursors.Cursor;
import xxl.core.relational.tuples.Tuple;

/**
 * This class is a fixed non-editable portion of data of a {@link BPlusIndexedSet} defined by its
 * lower and a upper bound and acts like a fixed range query. It is created by calling
 * {@link BPlusIndexedSet#subSet(Object, Object)}, {@link BPlusIndexedSet#headSet(Object)} or
 * {@link BPlusIndexedSet#tailSet(Object)}. Depending on set type a <code>BPlusIndexedSetView</code>
 * contains the elements of the parent set which are greater or equal to the lower bound and (may be
 * strictly) less than the upper bound. <br/>
 * <b>Please note:</b> Because it is a fixed view not all methods of Java's {@link SortedSet}
 * interface are supported. Although you can not edit the content of a view, it's content will be
 * refreshed automatically if the source set changes. That means if a view <code>v</code> contains
 * an item <code>e</code> and you remove <code>e</code> from the parent set, <code>e</code> will not
 * be visible in <code>v</code> anymore.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @param <E> The data type which is managed by the parent set
 */
public final class BPlusIndexedSetView<E extends Comparable>
    extends FixedQuery<E> implements Comparator<E> {

  /*
   * Actually an implementation of XXL Cursor to realize the "strictly less than" constrain of the
   * Java SortedSet Interface for some kinds of sub set. That means e.g. SortedSet#subSet requires
   * to skip the upper bound inside the view whereas SortedSet#tailSet includes the upper bound.
   * 
   * This Cursor implementation just checks if the item after the next item to query is equal to the
   * upper bound. If "excluding" the upper bound is enabled it will not return the upper bound when
   * calling Cursor#hasNext/Cursor#next
   */
  class RightOpenIntervalCursor<E extends Comparable> implements Cursor<E> {

    private Cursor<E> m_Cursor;
    private boolean m_ExcludeToElement;
    private FixedQuery m_Parent;
    private Comparable m_UpperBound;

    public RightOpenIntervalCursor(Cursor cursor, Comparable upperBound,
        FixedQuery parent) {
      m_Cursor = cursor;
      m_UpperBound =
          autoComparable(autoCast(upperBound), parent.m_DataSource.mCreator);
      m_Parent = parent;
      m_ExcludeToElement = parent.m_ExcludeToElement;
    }

    @Override
    public void close() {
      m_Cursor.close();
    }

    @Override
    public boolean hasNext() throws IllegalStateException {
      if (m_ExcludeToElement)
        return m_Cursor.hasNext()
            && autoComparable(autoCast(peek()), m_DataSource.mCreator)
                .compareTo(
                    autoComparable(autoCast(m_UpperBound),
                        m_DataSource.mCreator)) < 0;
      else
        return m_Cursor.hasNext()
            && autoComparable(autoCast(peek()), m_DataSource.mCreator)
                .compareTo(
                    autoComparable(autoCast(m_UpperBound),
                        m_DataSource.mCreator)) <= 0;
    }

    @Override
    public E next() throws IllegalStateException, NoSuchElementException {
      return (E) autoComparable(autoCast(m_Cursor.next()),
          m_DataSource.mCreator);
    }

    @Override
    public void open() {
      m_Cursor.open();
    }

    @Override
    public E peek() throws IllegalStateException, NoSuchElementException {
      return (E) autoComparable(m_Cursor.peek(), m_Parent.m_DataSource.mCreator);
    }

    /**
     * This method is not supported and will throw an <b>UnsupportedOperationException</b>
     */
    @Override
    public void remove() throws IllegalStateException,
        UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws UnsupportedOperationException {
      m_Cursor.reset();
    }

    @Override
    public boolean supportsPeek() {
      return m_Cursor.supportsPeek();
    }

    /**
     * This method is not supported and will throw an <b>UnsupportedOperationException</b>
     */
    @Override
    public boolean supportsRemove() {
      return false;
    }

    @Override
    public boolean supportsReset() {
      return m_Cursor.supportsReset();
    }

    @Override
    public boolean supportsUpdate() {
      return false;
    }

    /**
     * This method is not supported and will throw an <b>UnsupportedOperationException</b>
     */
    @Override
    public void update(E object) throws IllegalStateException,
        UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

  }

  /*
   * The reference of the parent set BPlusTree
   */
  private BPlusTree m_BPlusTree;

  /**
   * Constructs a new instance of a fixed view of a BPlus indexed set.
   * 
   * @param dataSource BPlusTree reference of the parent set
   * @param minBound The views minimum bound
   * @param maxBound The views maximum bound
   * @param excludeToElement A flag which indicates to include or exclude the upper bound for
   *        queries
   */
  public BPlusIndexedSetView(final BPlusIndexedSet dataSource,
      Comparable minBound, Comparable maxBound, boolean excludeToElement) {
    super(dataSource, minBound, maxBound, excludeToElement);
    m_BPlusTree = (BPlusTree) dataSource.mTree;
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException("Not available");
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException("Not available");
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not available");
  }

  @Override
  public Comparator<? super E> comparator() {
    return this;
  }

  @Override
  public int compare(E o1, E o2) {
    return o1.compareTo(o2);
  }

  @Override
  public boolean contains(Object o) {
    if (!inbounds(o)) return false;
    return m_DataSource.contains(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object item : c)
      if (!contains(item)) return false;
    return true;
  }

  private Comparable findElementBefore(Comparable e) {
    Cursor cursor =
        new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
            autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
            autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
            m_maxBound, this);
    Comparable last = null;
    while (cursor.hasNext()) {
      last = (Comparable) cursor.next();
    }
    return last;
  }

  @Override
  public E first() {
    return (E) new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
        autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
        autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
        m_maxBound, this).next();
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    if (!inbounds(toElement)) throw new IndexOutOfBoundsException();

    return new BPlusIndexedSetView<E>((BPlusIndexedSet) m_DataSource,
        m_minBound, toElement, true);
  }

  /*
   * Checks if the Object o is inside the views minimum and maximum bound
   */
  private boolean inbounds(Object o) {
    if ((autoComparable(o, m_DataSource.mCreator)).compareTo(m_minBound) < 0)
      return false;
    if ((autoComparable(o, m_DataSource.mCreator)).compareTo(m_maxBound) >= 0)
      return false;
    return true;
  }

  @Override
  public boolean isEmpty() {
    return size() <= 0;
  }

  @Override
  public Iterator<E> iterator() {
    return new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
        autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
        autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
        m_maxBound, this);
  }

  @Override
  public E last() {
    RightOpenIntervalCursor roiCursor =
        new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
            autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
            autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
            m_maxBound, this);
    return (E) findElementBefore(m_maxBound);
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Not available");
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not available");
  }

  /**
   * This method is unsupported and will throw an <b>UnsupportedOperationException</b>.
   */
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not available");
  }

  @Override
  public int size() {
    RightOpenIntervalCursor roiCursor =
        new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
            autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
            autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
            m_maxBound, this);
    int size = 0;
    while (roiCursor.hasNext()) {
      ++size;
      roiCursor.next();
    }
    return size;
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    if (!inbounds(fromElement) || !inbounds(toElement))
      throw new IndexOutOfBoundsException();

    return new BPlusIndexedSetView<E>((BPlusIndexedSet) m_DataSource,
        fromElement, toElement, true);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    if (!inbounds(fromElement)) throw new IndexOutOfBoundsException();

    return new BPlusIndexedSetView<E>((BPlusIndexedSet) m_DataSource,
        fromElement, m_maxBound, false);
  }

  @Override
  public Object[] toArray() {
    Object[] result = new Object[size()];
    Cursor cursor =
        new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
            autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
            autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
            m_maxBound, this);
    int index = 0;
    while (cursor.hasNext()) {
      Object item = m_BPlusTree.exactMatchQuery((Comparable) cursor.next());
      if (item instanceof Tuple)
        result[index] = ((Tuple) item).toArray();
      else
        result[index] = item;

      index++;
    }
    return result;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Object[] result = new Object[size()];
    Cursor cursor =
        new RightOpenIntervalCursor<>(m_BPlusTree.rangeQuery(
            autoComparable(autoCast(m_minBound), m_DataSource.mCreator),
            autoComparable(autoCast(m_maxBound), m_DataSource.mCreator)),
            m_maxBound, this);
    int index = 0;
    while (cursor.hasNext()) {
      Object item = m_BPlusTree.exactMatchQuery((Comparable) cursor.next());
      if (item instanceof Tuple)
        result[index] = (T) ((Tuple) item).toArray();
      else
        result[index] = (T) item;

      index++;
    }
    return (T[]) result;
  }



}
