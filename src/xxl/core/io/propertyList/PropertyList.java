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

import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A PropertyList is a compact format to store and receive complex data inside a single and easily
 * serializable structure. It contains a list of {@link Property Properties} which could be added
 * with {@link #add(Property)} and iterated with {@link #iterator()}. Each Property inside a
 * property list is a map of a name and a value which either is a primitive data type,
 * <code>null</code> or a nested property list. Please note that a PropertyList itself does not have
 * a <i>name</i>. Instead it distinguishes itself by it's content. This ensures that a property list
 * could be a value of a property item inside another proptery list<br/>
 * <br/>
 * 
 * <b>Example</b> In the example a property list is creates which contains a (simple) table dump of
 * a student database table. At first the table name "Students" is set. After this two tuples of
 * students where added. Each has two attributes which are ID and the students name. <br/>
 * <code><pre>
 * PropertyList myTable = new PropertyList();
 * myTable.add(new Property("Table", "Students"));
 * 
 * PropertyList myTuple1 = new PropertyList();
 * myTuple1.add(new Property("ID", 202031));
 * myTuple1.add(new Property("Name", "Mustermann"));
 * myTuple1.add(new Property("Subjects", new Object[] { "Maths", "Physics" }));
 * 
 * PropertyList myTuple2 = new PropertyList();
 * myTuple2.add(new Property("ID", 405066));
 * myTuple2.add(new Property("Name", "Musterfrau"));
 * myTuple2.add(new Property("Subjects", new Object[] { "English", "Geography" }));
 * 
 * myTable.add(new Property("Tuple1", myTuple1));
 * myTable.add(new Property("Tuple2", myTuple2));
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see PropertyListPrinter How to print the content of a PropertyList to a output stream
 * 
 */
public final class PropertyList implements Iterable<Property> {

  /**
   * This class models a nested PropertyList inside another {@link PropertyList}. A nested one is
   * accessible by an identifier of its parent {@link PropertyList} like any other {@link Property}.
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   */
  public class PropertyListItem {

    /*
     * Identifier and parent PropertyList
     */
    private String mName;
    private PropertyList mPropertyList;

    /**
     * Takes the property name and the parent list.
     * 
     * @param name Name
     * @param list Parent
     */
    public PropertyListItem(String name, PropertyList list) {
      mName = name;
      mPropertyList = list;
    }

    /**
     * @return The identifier for this property
     */
    public String getName() {
      return mName;
    }

    /**
     * @return The parent
     */
    public PropertyList getPropertyList() {
      return mPropertyList;
    }

    /**
     * Prints the nested property list to string.<br/>
     * <br/>
     * <code> toString() = "#NAME# := (Content of PropertyList)"
     */
    public String toString() {
      return mName + " := " + mPropertyList.toString();
    }
  }

  /*
   * The list of properties
   */
  ArrayList<Property> mProperties = new ArrayList<Property>();
  ArrayList<PropertyListItem> mPropertyLists =
      new ArrayList<PropertyListItem>();

  /**
   * Adds a new Property to the list
   * 
   * @param property an instance of Property
   */
  public void add(Property property) {
    if (property == null)
      throw new NullPointerException("Property to add is null");
    if (property.getValue() instanceof PropertyList)
      mPropertyLists.add(new PropertyListItem(property.getName().trim(),
          (PropertyList) property.getValue()));
    else {
      if (property.getValue() instanceof Object[]) {
        Object[] objectArray = (Object[]) property.getValue();
        for (Object o : objectArray) {
          if (o instanceof Object[])
            throw new IllegalArgumentException(
                "Nested arrays are forbidden (in \"" + property.getName()
                    + "\" assigment)");
          if (o instanceof PropertyList)
            throw new IllegalArgumentException(
                "Nested property lists are forbidden (in \""
                    + property.getName() + "\" assigment)");
        }
      }

      if (!mProperties.contains(property))
        mProperties.add(property);
      else {
        if (property.getValue() instanceof ArrayList) {
          mProperties.remove(property);
          mProperties.add(property);
        }
      }
    }
  }

  /**
   * Checks if the PropertyList contains the given list of property names.
   * 
   * @param names A collection of property identifiers
   * @return <b>true</b> if this contains <i>all</i> property names, otherwise <b>false</b>
   */
  public boolean containsAllProperties(Collection<String> names) {
    ArrayList<String> checkNames = new ArrayList<>();
    for (String name : names)
      checkNames.add(name);

    for (String find : listPropertyNames())
      checkNames.remove(find);

    return checkNames.isEmpty();
  }

  /**
   * Checks if the PropertyList contains a property with the given identifier <code>name</code>
   * 
   * @param name A property identifier
   * @return <b>true</b> if there is such a property, otherwise <b>false</b>
   */
  public boolean containsProperty(String name) {
    for (Property property : mProperties)
      if (property.getName().equals(name.trim())) return true;
    return false;
  }

  /**
   * Checks if this contains a nested PropertyList which is accessible by the identifier
   * <code>name</code>
   * 
   * @param name The nested list identifier
   * @return <b>true</b> if this contains a property list <code>name</code>, otherwise <b>false</b>
   */
  public boolean containsPropertyList(String name) {
    for (PropertyListItem property : mPropertyLists)
      if (property.getName().equals(name.trim())) return true;
    return false;
  }

  /**
   * Returns the value of the property <code>name</code>
   * 
   * @param name The property identifier
   * @return The value assigned to <code>name</code>.
   * @throws NoSuchElementException if there is no property with <code>name</code>
   */
  public Object getProperty(String name) throws NoSuchElementException {
    for (Property property : mProperties)
      if (property.getName().equalsIgnoreCase(name.trim()))
        return property.getValue();
    throw new NoSuchElementException("There is no property associated with \""
        + name + "\"");
  }

  /**
   * Returns the nested property list assigned to <code>name</code>
   * 
   * @param name The property list identifier
   * @return Nested list assigned to <code>name</code>.
   * @throws NoSuchElementException if there is no property with <code>name</code>
   */
  public PropertyList getPropertyList(String name)
      throws NoSuchElementException {
    for (PropertyListItem propertyLists : mPropertyLists)
      if (propertyLists.mName.equalsIgnoreCase(name.trim()))
        return propertyLists.mPropertyList;
    throw new NoSuchElementException(
        "There is no nested List associated with \"" + name + "\"");
  }

  @Override
  public Iterator<Property> iterator() {
    return mProperties.iterator();
  }

  /**
   * Lists all identifiers referring to a nested property list. The list may be empty if there are
   * no nested ones inside this list.
   * 
   * @return List of identifiers
   */
  public String[] listNestedPropertyListNames() {
    ArrayList<String> names = new ArrayList<>();
    for (PropertyListItem propertyList : mPropertyLists)
      names.add(propertyList.getName());
    return names.toArray(new String[names.size()]);
  }

  /**
   * Lists all nested property lists. The returned array contains {@link PropertyListItem
   * PropertyListItems} which allow to get the assigned identifier outside their parent. The list
   * may be empty if there are no nested ones inside this list.
   * 
   * @return List of property list (items)
   */
  public PropertyListItem[] listPropertyLists() throws NoSuchObjectException {
    return mPropertyLists.toArray(new PropertyListItem[mPropertyLists.size()]);
  }

  /**
   * Lists all identifiers referring to property which are not lists. The list may be empty if there
   * are properties inside this list.
   * 
   * @return List of identifiers
   */
  public String[] listPropertyNames() {
    ArrayList<String> names = new ArrayList<>();
    for (Property property : mProperties)
      names.add(property.getName());
    return names.toArray(new String[names.size()]);
  }

  /**
   * Prints the current list to String
   */
  public String toString() {
    StringBuilder retval = new StringBuilder();
    retval.append("(");

    for (Property p : this)
      retval.append(p.toString());

    try {
      for (PropertyListItem pli : listPropertyLists())
        retval.append(pli.toString());
    } catch (Exception e) {};

    retval.append(")");
    return retval.toString();

  }
}
