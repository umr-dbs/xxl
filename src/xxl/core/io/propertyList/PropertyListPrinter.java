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

import java.io.IOException;
import java.io.OutputStream;

import xxl.core.io.propertyList.json.JSONPrinter;

/**
 * This is an abstract base class for printers which converts a {@link PropertyList} into a specific
 * output format. The format and handling or discarding of e.g. complex data type assignments
 * depends on the concrete implementation of the printer. <br/>
 * <br/>
 * The given {@link PropertyList} is accessible through the protected member {@link #mPropertyList}.
 * Any subclass has to implement the abstract method {@link #print(OutputStream)}.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see JSONPrinter Print the content of PropertyList as JSON file
 * 
 */
public abstract class PropertyListPrinter {

  /*
   * The property list which should be printed
   */
  protected PropertyList mPropertyList;

  /**
   * Constructs a new printer to a given instance of {@link PropertyList}. Use
   * {@link #print(OutputStream)} to print the content of the property list to several output
   * streams.
   * 
   * @param propertyList The property list which should be printed in JSON format
   */
  public PropertyListPrinter(PropertyList propertyList) {
    mPropertyList = propertyList;
  }

  /**
   * Prints the given {@link PropertyList} to the output.
   * 
   * @param output Output stream
   * @throws IOException If it fails to write into output stream
   * 
   */
  public abstract void print(OutputStream output) throws IOException;
}
