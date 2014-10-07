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

package xxl.core.relational.schema;

import xxl.core.indexStructures.IndexedSet;

/**
 * Helper class which provides and uniform entry point to create {@link Schema} objects. <br/>
 * <br/>
 * <b>Example</b> The following example shows how to create a schema for a relational table.
 * 
 * <pre><code>
 * Builders.createBPlusTree.Tuples(Schemas.createSchema(MY_TABLE_NAME)
 *                                  .addInteger("ID")
 *                                  .addInteger("NUM")).getBuilder().create();
 * </code></pre>
 * 
 * @see IndexedSet
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class Schemas {

  /**
   * Constructs a new schema with the given name.
   * 
   * @param schemaName The schema's (table's) name
   * @return
   */
  public static Schema createSchema(String schemaName) {
    Schema result = new Schema(schemaName);
    return result;
  }

}
