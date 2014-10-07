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

package xxl.core.indexStructures;

import java.util.SortedSet;

/**
 * A subclass of <code>AbstractView</code> is a fixed non-editable portion of data of a
 * {@link IndexedSet}. By sub setting an <i>IndexedSet</i> you setup a lower and a upper bound. All
 * entries which are greater or equal to the lower bound and less (depending of the subset kind it's
 * strictly less or also equal to) the upper bound will be visible inside the view. Depending on the
 * implementation of <code>AbstractView</code> some methods of the <code>SortedSet</code> interface
 * will not be available. Please consider documentation of the individual sub classes. <br/>
 * <br/>
 * <b>Please note:</b> Although you can not edit the content of a view, it's content will be
 * refreshed automatically if the source set changes. That means if a view <code>v</code> contains
 * an item <code>e</code> and you remove <code>e</code> from the parent set, <code>e</code> will not
 * be visible in <code>v</code> anymore.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @param <E> The data type of the set
 */
public abstract class FixedQuery<E> implements SortedSet<E> {

  /*
   * A reference to the parent indexed set to handle query performance and updates.
   */
  protected IndexedSet m_DataSource;

  /*
   * Depending on the kind of the subset the last element is excluded or included. This flags
   * controls that.
   */
  protected boolean m_ExcludeToElement;

  /*
   * The upper and lower boundaries for this view
   */
  protected Comparable m_minBound, m_maxBound;

  /**
   * Constructs a new view for the set <code>dataSource</code> with the given boundaries. If it is
   * necessary to exclude the last element, set <code>excludeToElement</code> to <b>true</b>.
   * Otherwise the view will contains elements which are less or equal to the last element.
   * 
   * @param dataSource The parent set
   * @param minBound The lower bound
   * @param maxBound The upper bound
   * @param excludeToElement Flag to indicate how to handle elements relative to the upper bound
   */
  public FixedQuery(final IndexedSet dataSource, Comparable minBound,
      Comparable maxBound, boolean excludeToElement) {
    if (minBound == null || maxBound == null)
      throw new IllegalAccessError("Boundaries can not be null");
    m_DataSource = dataSource;
    m_minBound = minBound;
    m_maxBound = maxBound;
    m_ExcludeToElement = excludeToElement;
  }

}
