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

import java.io.InputStream;

import xxl.core.io.propertyList.json.JSONReader;

/**
 * This base class models the domain of class which is able to restore a {@link PropertyList}.
 * 
 * @see JSONReader
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public abstract class PropertyListReader {

  /**
   * Restores the property list provided inside input stream an returns it.
   * 
   * @param inputStream The source which contains a (printed) PropertyList
   * @return The restored PropertyList
   */
  public abstract PropertyList read(InputStream inputStream);

}
