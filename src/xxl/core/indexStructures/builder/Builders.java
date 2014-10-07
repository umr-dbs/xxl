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

package xxl.core.indexStructures.builder;

import java.io.File;
import java.io.FileNotFoundException;
import java.rmi.NoSuchObjectException;
import java.sql.SQLException;

import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration;
import xxl.core.indexStructures.builder.BPlusTree.PrimitiveType;
import xxl.core.indexStructures.builder.BPlusTree.TupleType;
import xxl.core.relational.JavaType;
import xxl.core.relational.metaData.TupleMetaData;
import xxl.core.relational.schema.Schema;
import xxl.core.relational.schema.Schemas;


/**
 * The class <i>Builders</i> allows uniform access to builders for various classes of XXL library.
 * The static methods provided by this class helps you to initialize or reload objects e.g. an BPlus
 * tree.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class Builders {

  /**
   * This class simplifies the creation for BPlus indexes for various primitive or relational data
   * types. The creation process is encapsulated.
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   */
  public static class BPlusCreatorFacade {

    /**
     * Creates a new BPlus Index which handles primitive boolean types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Boolean(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.BOOLEAN,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive date types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Date(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.DATE,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive double types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Double(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.DOUBLE,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive float types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Float(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.FLOAT,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive integer types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Integer(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.INT,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive long types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Long(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.LONG,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive short types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Short(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.SHORT,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive time types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Time(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(JavaType.TIME,
          tableName));
    }

    /**
     * Creates a new BPlus Index which handles primitive timestamp types.
     * 
     * @param tableName The table name for this index
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Timestamp(String tableName) {
      return new BPlusConfiguration.Creator(new PrimitiveType(
          JavaType.TIMESTAMP, tableName));
    }

    /**
     * Creates a new BPlus Index which handles <i>relational</i> types. A relational table is
     * defined by it's schema which contains columns (and types) for each entry. Use the
     * <code>Schemas</code> class to quickly create schemas.
     * 
     * @see Schemas
     * 
     * @param schema The table' schema
     * @return A ready-to-use BPlus configuration
     */
    public static BPlusConfiguration.Creator Tuples(Schema schema)
        throws SQLException {
      TupleMetaData metaData =
          new TupleMetaData(schema.getName(), schema.getColumns());
      return new BPlusConfiguration.Creator(new TupleType(metaData));
    }

  }
  /**
   * This is a helper class to provide a static <code>from</code> method which is for
   * BPlusConfiguration loading. Mostly it's for syntactic sugar within the <i>Builders</i> class.
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   */
  public static class BPlusLoaderFacade {

    /**
     * Loads a BPlus index from persistent storage <code>path</code>. The string
     * <code>tableName</code> indicates the tree which should be loaded and is equal to the table
     * name which was set when creating the tree in the last session. <br/>
     * <br/>
     * <br/>
     * <i>Note:</i> The information needed to reload the tree is stored in a file which ends with
     * "Meta.json". You don't care about this. Just set <code>tableName</code> to the name of the
     * table you want to load.
     * 
     * @param path File system path to a directory which contains the BPlus memory files
     * @param tableName The table name
     * @return Setups a BPlusConfiguration.Loader instance which can reconstruct the tree
     * 
     * @throws NoSuchObjectException Is thrown if the file is not well formed or if it is invalid
     * @throws FileNotFoundException If there is no file for <code>tableName</code>
     */
    public static BPlusConfiguration.Loader from(String path, String tableName)
        throws NoSuchObjectException, FileNotFoundException {
      return new BPlusConfiguration.Loader(path + File.separatorChar
          + tableName + "Meta.json");
    }

  }

  // TODO: Comment
  public static BPlusCreatorFacade createBPlusTree;

  public static BPlusLoaderFacade loadBPlusTree;
}
