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

package xxl.core.io.propertyList;

import xxl.core.io.propertyList.json.JSONPrinter;


/**
 * The property class models a single value assignment (item) for a {@link PropertyList}. Although
 * you can add any object as an value please note that some {@link PropertyListPrinter printers}
 * e.g. {@link JSONPrinter} only support primitive data types like
 * <ul>
 * <li>String</li>
 * <li>java.sql.Date</li>
 * <li>java.sql.Time</li>
 * <li>java.sql.Timestamp</li>
 * <li>Integer</li>
 * <li>Boolean</li>
 * <li>Byte</li>
 * <li>Double</li>
 * <li>Float</li>
 * <li>Long</li>
 * <li>Short</li>
 * </ul>
 * For detailed information which data types are supported see the documentation for the chosen
 * printer.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see PropertyList Store properties inside a list
 * 
 */
public final class Property {

  /*
   * Name and value member
   */
  private String mName;
  private Object mValue;

  /**
   * Constructs a new property with given name and a <code>null</code> value.
   * 
   * @param name
   * 
   * @see #Property(String, Object) Setting up values other than <code>null</code>
   */
  public Property(String name) {
    if (name == null) throw new IllegalAccessError("Proerty name is null");
    mName = name.trim();
    mValue = null;
  }

  /**
   * Constructs a new property with given name and value. <b>Note:</b> You should use a primitive
   * data type as value or a nested {@link PropertyList} instance. It is forbidden to add a Property
   * instance as value of another Property and will rise an IllegalArgumentException. <br/>
   * <br/>
   * 
   * <b>Example</b> The following line creates a new property item called "Name" with the value
   * "Mustermann". <code><pre>
   * new Property("Name", "Mustermann");
   * </pre></code>
   * 
   * <b>Example</b> The following line creates a new property item called "Worker" which could be a
   * single list items for a set of workers inside a PropertyList. The value for this item is a
   * nested property list which gives detailed information about this single worker. <code><pre>
   * PropertyList workerInfo = new PropertyList();
   * workerInfo.add(new Property("Name", "Mustermann");
   * workerInfo.add(new Property("Age", 27);
   * 
   * new Property("Worker", workerInfo);
   * </pre></code>
   * 
   * <b>Warning:</b> When adding an array as value it <u>has to be an <code>Object[]</code></u>. If
   * you add <code>int[]</code> or <code>Integer[]</code> the printer will store this as
   * <code>null</code>. Maybe you have to convert you non-Object array in Object[] by youself before
   * adding it as property value. Nested object arrays (an object inside the array is also an array)
   * are not allowed and will throw an IllegalArgumentException<br/>
   * <br/>
   * 
   * @param name The item name
   * @param o The item value
   * 
   * @see #Property(String) Set a value to <code>null</code>
   */
  public Property(String name, Object o) {
    this(name);
    if (o instanceof Property)
      throw new IllegalArgumentException("Nested property as value (\"" + name
          + "\")");
    mValue = o;

    StringBuilder b = new StringBuilder();
    b.append(o);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Property other = (Property) obj;
    if (mName == null) {
      if (other.mName != null) return false;
    } else if (!mName.equals(other.mName)) return false;
    return true;
  }

  /**
   * Returns the item name
   * 
   * @return item name
   */
  public String getName() {
    return mName;
  }

  /**
   * Returns the item value
   * 
   * @return item value
   */
  public Object getValue() {
    return mValue;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mName == null) ? 0 : mName.hashCode());
    return result;
  }

  public String toString() {
    return mName + " := " + ((mValue == null) ? "null" : mValue.toString())
        + " | ";
  }
}
