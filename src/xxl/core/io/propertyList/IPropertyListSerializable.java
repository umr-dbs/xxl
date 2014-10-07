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

import xxl.core.indexStructures.builder.IndexBuilder;

/**
 * This interface models the ability of a class to serialize itself into a {@link PropertyList}.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @param <Self> The class which implements the interface
 * @param <ExternInformation> A provider for additional information (additional)
 * 
 * @see IndexBuilder
 */
public interface IPropertyListSerializable<Self, ExternInformation> {

  /**
   * Serialize the current instance into {@link PropertyList} object.
   * 
   * @param information Additional information from another object
   * @return A PropertyList instance which contains the serialized instance of this object
   * @throws Exception
   */
  public PropertyList serialize(ExternInformation information) throws Exception;

}
