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

package xxl.core.indexStructures.builder.BPlusTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.rmi.NoSuchObjectException;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.SortedSet;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.MapContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.builder.IndexBuilder;
import xxl.core.indexStructures.builder.IndexConfiguration;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.io.converters.meta.KeyFunctionFactory;
import xxl.core.io.converters.MeasuredTupleConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.propertyList.PropertyList;
import xxl.core.io.propertyList.PropertyListReader;
import xxl.core.io.propertyList.json.JSONReader;
import xxl.core.relational.JavaType;
import xxl.core.relational.metaData.TupleMetaData;
import xxl.core.util.ConvertUtils;
import xxl.core.util.FileUtils;

/**
 * This implementation of {@link IndexConfiguration} base class allows to setup all requirements for
 * building a {@link BPlusTree}. It contains the two nested classes {@link Creator} and
 * {@link Loader} which handle either creating or reloading the wrapper class
 * {@link BPlusIndexedSet} which is an implementation of Java's {@link SortedSet}. Use
 * {@link Creator} to setup a completely new BPlusTree. <br/>
 * <br/>
 * <b>Example</b> This example shows how to create a indexed set which manages <i>timestamp</i>
 * data. <code><pre>
 * BPlusConfiguration.Creator requirements = new BPlusConfiguration.Creator(new PrimitiveType(JavaType.TIMESTAMP, "TimestampTable"));
 * BPlusIndexedSet myTree = requirements.storeAtFileSystem("myStorage/myTree").getBuilder().create();
 * // add some data
 * myTree.save();
 * </pre></code>
 * 
 * <b>Example</b> This example shows how to reload the indexed set from the previous example.
 * <code><pre>
 * BPlusTreeBuilder builder = new BPlusConfiguration.Loader("myStorage/myTree/TimestampTableMeta.json").getBuilder();
 * BPlusIndexedSet<Long> myRestoredTree = builder.create();
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see Creator Create a BPlus tree indexed set with <i>Creator</i>
 * @see Loader Loading a BPluss tree index set with <i>Loader</i>
 */
public abstract class BPlusConfiguration extends IndexConfiguration {

  /**
   * A concrete implementation of {@link IndexConfiguration} for setting up the requirements of a
   * {@link BPlusTree} according to your needs. With an object of
   * <code>BPlusTreeConfiguration</code> type you can define whatever your BPlusTree should be kept
   * in memory or stored on the hard drive, the block size (in Byte), the buffer size, the needed
   * {@link Container Containers} and which table schema you want to manage including a set of key
   * which should be used for the indexing job. If you don't make the use of any setter methods a
   * BPlusTree with the following configuration will be created by <i>default</i>:
   * <ul>
   * <li>The block size used it set to 4096 Bytes (4 KB)</li>
   * <li>A single (unshared) {@link LRUBuffer} with size of 20 items is used</li>
   * <li>The converter container is a {@link MapContainer}</li>
   * <li>The tree will be kept in main memory</li>
   * <li>The first column of your table will be the (single) index key</li>
   * </ul>
   * If you want to change this configuration just use the corresponding <i>setter</i> methods. <br/>
   * <br/>
   * <b>Note:</b> Ensure that there is no index out of bounds when setting up a compounded key, see
   * {@link ResultSetMetaData} behavior for attribute index design in contrast to "<i>normal</i>"
   * item index of e.g. an array. Please note that the first item index is one not zero for table
   * columns.
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   * @see BPlusTreeBuilder Easily build a BPlus tree with a given BPlusTreeConfiguration
   * @see IndexConfiguration The base class for all index configurations
   * @see BPlusTree The BPlus tree index structure itself
   * 
   */
  public static class Creator extends BPlusConfiguration {
    /**
     * Constructs a new <code>BPlusTreeConfiguration</code>.<br/>
     * <br/>
     * <b>Example</b> <code><pre>
     * TupleMetaData myTupleMetaData = new TupleMetaData("Students", new ColumnMetaInfo [] {
     * new ColumnMetaInfo (RelationalType.INTEGER, "ID")
     * new ColumnMetaInfo (RelationalType.VARCHAR, 20, "name")});
     * 
     * BPlusTreeConfiguration configuration = new BPlusTreeConfiguration(new TupleType(myTupleMetaData));
     * </pre></code>
     * 
     * @param primitiveType The meta data for your table which should be managed
     * 
     * @see TupleMetaData
     * @see ManagedType
     */
    public Creator(ManagedType primitiveType) {
      mManagedType = primitiveType;
    }



    /**
     * Returns a builder for a concrete <code>BPlusTree</code> which satisfies your requirements
     * which you have specified. Please node that the method {@link IndexBuilder#create()} returns
     * the <code>BPlusTree</code> ready to use.
     * 
     * @see IndexBuilder
     * 
     */
    @Override
    public BPlusTreeBuilder getBuilder() {
      return new BPlusTreeBuilder(this);
    }

    /**
     * Returns an array of indices which should be used as compounded key for indexing. Please node
     * that this returns an integer array in which the first item is at position zero whereas the
     * first <i>column</i> index for the table is one not zero.
     * 
     * <br/>
     * <br/>
     * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method
     * is restricted to package wide visibility.
     * 
     * @return The indices of the compounded key
     */
    int[] getCompoundKeyIndices() {
      return mManagedType.getCompoundKeyIndices();
    }



    /**
     * Returns the content type of the data managed by the tree. This is a string which represents
     * the type of data. The string starts with "primitive/" (for primitive types) or "complex/"
     * (for e.g. tuples) followed by a identifier.
     * 
     * @see ManagedType#getContentType()
     * @return the current content type
     */
    public String getContentType() {
      return mManagedType.getContentType();
    }

    /**
     * Returns the meta data for the table to manage.
     * 
     * <br/>
     * <br/>
     * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method
     * is restricted to package wide visibility.
     * 
     * @return the given meta data
     * 
     * @see ExtendedResultSetMetaData
     */
    public ExtendedResultSetMetaData getMetaData() {
      return mManagedType.getMetaData();
    }

    /**
     * Returns the table name which is given by the user by constructor call of {@link Creator}.
     * 
     * @return the table name
     */
    public String getTableName() {
      return mManagedType.getTableName();
    }

    /**
     * Sets the block size used by the BPlusTree.
     * 
     * @param blockSize the block size
     */
    public void setBlockSize(int blockSize) {
      mBlockSize = blockSize;
    }


    /**
     * Sets the buffer used for the BPlus tree. By default it is a single (unshared)
     * {@link LRUBuffer} with a capacity of INDEX_REQUIREMENTS_DEFAULT_LRU_BUFFER_SIZE items. When
     * setting a new buffer feel free to use a shared buffered for multiple trees.
     * 
     * @param buffer A buffer which should be used by the BPlus tree
     * @return The current <code>BPlusTreeConfiguration</code> instance including the effect of this
     *         method call. With this it is possible to set the configuration in one single line
     *         like
     * 
     *         <code><pre>BPlusTreeConfiguration con = new BPlusTreeConfiguration(..).setA().setB()...</pre></code>
     * 
     *         instead of setting each property in a single call like <code><pre>
     *  BPlusTreeConfiguration con = new BPlusTreeConfiguration(..);
     *  con.setA();
     *  con.setB();
     *  ...
     *  </pre></code>
     */
    public Creator setBuffer(Buffer buffer) {
      mBuffer = buffer;
      setBuffer(null);
      updateBufferContainer();
      return this;
    }

    /**
     * Set the indices of the column which should be used as the compounded key for indexing. The
     * tuples are compared lexicographically in descending order of their key indices given by
     * <code>compoundKeyIndices</code>. The order of the table columns is the order in which you
     * define it in <code>compoundKeyIndices</code> array. Thus, the first column (according to the
     * first array item value) is compared with the first column of another tuple (according to the
     * first array item value).
     * 
     * <br/>
     * <br/>
     * If there is more than one key column, lets say two, the second columns are compared if there
     * is equality in the first column for both tuples.
     * 
     * <br/>
     * <br/>
     * Please mark that e.g. a <code>compoundKeyIndices</code> array <code>[1,2]</code> first
     * compares the <i>first</i> column and after this (if necessary) the <i>second</i> column.
     * Whereas <code>[2,1]</code> forces to compare at first the <i>second</i> column and after this
     * (if necessary) the <i>first</i> column. Here, it must be ensured that the components of a
     * tuple, which form a key, have to be <b>comparable</b> and also that there will be no
     * duplicate values for a (compounded) key, so that a (compounded) key uniquely identifies a
     * tuple.
     * 
     * <br/>
     * <br/>
     * <b>Please note:</b> The first column has the index one not zero whereas the first array item
     * has the index zero.
     * 
     * @param compoundKeyIndices An array that contains the column indices. Please note that the
     *        order of the columns matters for comparing and that each column index is in bounds of
     *        the table column count.
     * 
     * @return The current <code>BPlusTreeConfiguration</code> instance including the effect of this
     *         method call. With this it is possible to set the configuration in one single line
     *         like
     * 
     *         <code><pre>BPlusTreeConfiguration con = new BPlusTreeConfiguration(..).setA().setB()...</pre></code>
     * 
     *         instead of setting each property in a single call like <code><pre>
     *  BPlusTreeConfiguration con = new BPlusTreeConfiguration(..);
     *  con.setA();
     *  con.setB();
     *  ...
     *  </pre></code>
     */
    public Creator setCompoundKey(int... compoundKeyIndices) {
      mManagedType.setCompoundKey(compoundKeyIndices);
      return this;
    }

    /**
     * Sets the converter container used for the BPlus tree.
     * 
     * @param converterContainer
     * @return The current <code>BPlusTreeConfiguration</code> instance including the effect of this
     *         method call. With this it is possible to set the configuration in one single line
     *         like
     * 
     *         <code><pre>BPlusTreeConfiguration con = new BPlusTreeConfiguration(..).setA().setB()...</pre></code>
     * 
     *         instead of setting each property in a single call like <code><pre>
     *  BPlusTreeConfiguration con = new BPlusTreeConfiguration(..);
     *  con.setA();
     *  con.setB();
     *  ...
     *  </pre></code>
     * 
     * @see Container
     */
    public Creator setConverterContainer(Container converterContainer) {
      mConverterContainer = converterContainer;
      updateBufferContainer();
      return this;
    }

    /**
     * Sets the file container used to manage file output. By default a matching one is generated
     * automatically when calling {@link #storeAt(String)}.
     * 
     * @param fileContainer The file container.
     * 
     * @return The current <code>BPlusTreeConfiguration</code> instance including the effect of this
     *         method call. With this it is possible to set the configuration in one single line
     *         like
     * 
     *         <code><pre>BPlusTreeConfiguration con = new BPlusTreeConfiguration(..).setA().setB()...</pre></code>
     * 
     *         instead of setting each property in a single call like <code><pre>
     *  BPlusTreeConfiguration con = new BPlusTreeConfiguration(..);
     *  con.setA();
     *  con.setB();
     *  ...
     *  </pre></code>
     */
    public Creator setFileContainer(Container fileContainer) {
      mFileContainer = fileContainer;
      return this;
    }

    /**
     * If you want to store the BPlus tree on a persistent storage instead of the default main
     * memory usage you have to set a directory path in which the BPlus tree stores it's files.
     * Please ensure that <code>storeDir</code> is a valid and accessible directory on the file
     * system. If it is necessary from the perspective of BPlus tree to write down the data there
     * will be a couple of files generated including a single file which contains the given meta
     * data information. The file name is taken from the table name given in meta data when calling
     * the constructor. The file extensions depends on the content and will be managed by the BPlus
     * tree itself.
     * 
     * @param storeDir A valid and accessible directory at the hard drive or network storage.
     * 
     * @return The current <code>BPlusTreeConfiguration</code> instance including the effect of this
     *         method call. With this it is possible to set the configuration in one single line
     *         like
     * 
     *         <code><pre>BPlusTreeConfiguration con = new BPlusTreeConfiguration(..).setA().setB()...</pre></code>
     * 
     *         instead of setting each property in a single call like <code><pre>
     *  BPlusTreeConfiguration con = new BPlusTreeConfiguration(..);
     *  con.setA();
     *  con.setB();
     *  ...
     *  </pre></code>
     * 
     */
    public Creator storeAt(String storeDir) {
      mLocation = Location.LOCATION_FILESYSTEM;

      if (!new File(storeDir).isDirectory())
        throw new IllegalArgumentException(
            "Given path to store BPlus data to hard drive is not a directory (\""
                + storeDir + "\")");
      if (storeDir.charAt(storeDir.length() - 1) != '/') storeDir += '/';

      try {
        mFileSystemFilePath = storeDir + mManagedType.getTableName();
      } catch (Exception e) {
        throw new RuntimeException("Unable to set file path (" + e.getMessage()
            + ")");
      }

      mFileContainer = new BlockFileContainer(mFileSystemFilePath, mBlockSize);
      return this;
    }

    /**
     * Prints the content of this configuration into a string. It contains
     * <ul>
     * <li>The block size</li>
     * <li>File path where the tree should be stored</li>
     * <li>A flag which indicates if the tree should be hold in main memory or at hard drive</li>
     * <li>The content type</li>
     * <li>The table name</li>
     * </ul>
     * If the tree should manage tuples instead of primitive data types there are some additionally
     * information printed <li>The compound key indices</li> <li>A dump of all column meta data</li>
     * </ul>
     */
    public String toString() {
      String dump =
          "BPlusTreeConfiguration: \n\tBlockSize: " + mBlockSize
              + "\n\tFilePath: " + mFileSystemFilePath + "\n\tLocation: "
              + mLocation;
      dump += "\n\tContent Type: " + mManagedType.getContentType();
      dump += "\n\tTable Name: " + mManagedType.getTableName();

      if (mManagedType instanceof TupleType) {
        TupleType tt = (TupleType) mManagedType;
        dump +=
            "\n\tCompound Key Indices: "
                + Arrays.toString(tt.getCompoundKeyIndices());
        ExtendedResultSetMetaData metaData = tt.getMetaData();

        try {
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            dump += "\n\t** COLUMN #" + (i + 1);
            dump += "\n\tCatalog Name: " + metaData.getCatalogName(i + 1);
            dump += "\n\tClass Name: " + metaData.getColumnClassName(i + 1);
            dump += "\n\tDisplay Size: " + metaData.getColumnDisplaySize(i + 1);
            dump += "\n\tColumn Label: " + metaData.getColumnLabel(i + 1);
            dump += "\n\tColumn Name: " + metaData.getColumnName(i + 1);
            dump += "\n\tColumn Type: " + metaData.getColumnType(i + 1);
            try {
              if (ConvertUtils.toJavaType(ConvertUtils
                  .toRelationalType(metaData.getColumnType(i + 1))) == JavaType.STRING)
                dump +=
                    "\n\tColumn Type Name: "
                        + metaData.getColumnTypeName(i + 1);
            } catch (Exception e) {
              e.printStackTrace();
            }
            dump += "\n\tContent Length: " + metaData.getContentLength(i + 1);
            dump += "\n\tPrecision: " + metaData.getPrecision(i + 1);
            dump += "\n\tScale: " + metaData.getScale(i + 1);
            dump += "\n\tSchema: " + metaData.getSchemaName(i + 1);
            dump += "\n\tTable Name: " + metaData.getTableName(i + 1);
            dump += "\n\tAuto Increment: " + metaData.isAutoIncrement(i + 1);
            dump += "\n\tCase Sensitive: " + metaData.isCaseSensitive(i + 1);
            dump += "\n\tCurrency: " + metaData.isCurrency(i + 1);
            dump += "\n\tDef Writable: " + metaData.isDefinitelyWritable(i + 1);
            dump += "\n\tNullable: " + metaData.isNullable(i + 1);
            dump += "\n\tReadOnly: " + metaData.isReadOnly(i + 1);
            dump += "\n\tSearchable: " + metaData.isSearchable(i + 1);
            dump += "\n\tSigned: " + metaData.isSigned(i + 1);
            dump += "\n\tWritable: " + metaData.isWritable(i + 1);
            dump += "\n";
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

      } else if (mManagedType instanceof PrimitiveType) {

      } else
        throw new UnsupportedOperationException(
            "Unknown managed type during dumping detected!");

      return dump;
    }
  }

  /**
   * This class is used to reload a {@link BPlusIndexedSet} (wrapper for {@link BPlusTree}) from a
   * previous session. <br/>
   * <br/>
   * <b>Example</b> This example shows how to reload the indexed set from the previous session which
   * stores the data of a <i>timestamp</i> managing index set to "<code>myStorage/myTree/</code>".
   * <code><pre>
   * BPlusTreeBuilder builder = new BPlusConfiguration.Loader("myStorage/myTree/TimestampTableMeta.json").getBuilder();
   * BPlusIndexedSet<Long> myRestoredTree = builder.create();
   * </pre></code>
   * 
   * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
   * 
   */
  public static class Loader {

    /*
     * The builder member which creates the BPlusTree after setting up the required (restored)
     * information
     */
    BPlusTreeBuilder mBuilder = null;

    /**
     * Reloads the {@link BPlusIndexedSet} from the file. Please ensure that <code>metaFile</code>
     * points to meta data. This file should be named like the given <b>table name</b> (which was
     * setup to {@link Creator} the last time) and ended with extension <b>.json</b>.
     * 
     * @param metaFile The file pointing to the meta data
     * @throws FileNotFoundException If the file was not found
     * @throws NoSuchObjectException If the file is not well formed or invalid
     */
    public Loader(File metaFile) throws FileNotFoundException,
        NoSuchObjectException {
      mBuilder =
          loadFromPropertyList(loadFromFile(metaFile),
              FileUtils.getFilePath(metaFile.getAbsolutePath()));
    }

    /**
     * Reloads the {@link BPlusIndexedSet} from a given {@link PropertyList} instance. Please ensure
     * that <code>metaInformation</code> contains the required information.
     * 
     * @param metaInformation A property list containing the meta information
     * @param storeTreeFilePath Where the tree is (and should be) stored
     * @throws NoSuchObjectException If the property list is not well formed or invalid
     */
    public Loader(PropertyList metaInformation, String storeTreeFilePath)
        throws NoSuchObjectException {
      mBuilder = loadFromPropertyList(metaInformation, storeTreeFilePath);
    }

    /**
     * Reloads the {@link BPlusIndexedSet} from the given path. Please ensure that you give the
     * <i>full</i> path to the set's meta data. This file should be named like the given <b>table
     * name</b> (which was setup to {@link Creator} the last time) and ended with extension
     * <b>.json</b>.
     * 
     * @param fullMetaFilenamePath The full path to the {@link BPlusIndexedSet} meta file
     * @throws FileNotFoundException If the file was not found
     * @throws NoSuchObjectException If the file is not well formed or invalid
     */
    public Loader(String fullMetaFilenamePath) throws FileNotFoundException,
        NoSuchObjectException {
      if (fullMetaFilenamePath == null || fullMetaFilenamePath.isEmpty())
        throw new IllegalArgumentException(
            "Given file path variable is empty or null");

      File file = new File(fullMetaFilenamePath);
      mBuilder =
          loadFromPropertyList(loadFromFile(file),
              FileUtils.getFilePath(file.getAbsolutePath()));
    }

    /**
     * Returns a builder which allows you to create a {@link BPlusTree}.
     * 
     * @return A {@link BPlusTreeBuilder} instance
     */
    public BPlusTreeBuilder getBuilder() {
      return mBuilder;
    }

    /*
     * Parses <i>file</i> by JSONReader into a PropertyList. The returned value should contain the
     * meta information about the (stored) tree. Actually this logic is done by another method.
     */
    private PropertyList loadFromFile(File file) throws FileNotFoundException {
      if (file == null)
        throw new NullPointerException("Load from file: file is null");
      if (!file.exists())
        throw new FileNotFoundException("File \"" + file.getAbsolutePath()
            + "\" not found!");
      if (!file.isFile() || !file.canRead())
        throw new IllegalArgumentException("File \"" + file.getAbsolutePath()
            + "\" is no file or not readable.");

      PropertyListReader reader = null;
      switch (FileUtils.getFileExtension(file.getName()).toLowerCase()) {
        case JSONReader.FILE_EXTENSION:
          reader = new JSONReader();
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown file extension for \"" + file.getAbsolutePath()
                  + "\". There is no reader available for this format.");
      }

      return reader.read(new FileInputStream(file));
    }

    /*
     * Restores (stored) tree by creating a builder (with given information), enables the reloading
     * flag an returns this builder. The builder information is provided by <i>metaInformation</i>
     */
    private BPlusTreeBuilder loadFromPropertyList(PropertyList metaInformation,
        String storeTreeFilePath) throws NoSuchObjectException {
      if (metaInformation == null)
        throw new NullPointerException(
            "Load from meta information from property list - object is null");
      BPlusTreeBuilder builder =
          BPlusTreeBuilder.unserialize(metaInformation, storeTreeFilePath);
      return builder.enableReload();
    }
  }

  /*
   * The file container used by the BPlus tree
   */
  Container mFileContainer = null;

  /*
   * The data type descriptor which contains type specific functions (e.g. StringConverter for a
   * String type)
   */
  protected ManagedType mManagedType;

  /**
   * Returns the measured tuple converter which should be used by the BPlus tree and is needed to
   * calculate the size for each component of the tuple.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return The converter
   * 
   * @see MeasuredTupleConverter
   */
  MeasuredConverter getDataConverter() {
    return mManagedType.getDataConverter();
  }

  /**
   * Returns the file container if the BPlus tree is stored on a storage medium or throws an
   * <code>IllegalArgumentException</code> if the location is the main memory. <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   */
  Container getFileContainer() {
    if (mLocation.equals(Location.LOCATION_MAIN_MEMORY))
      throw new IllegalArgumentException(
          "There is no file container set because the tree is stored at main memory.");
    else
      return mFileContainer;
  }

  /**
   * Returns the tuple key function factory which contains ready to use functions according to the
   * meta data and the compound key indices.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return the key function factory
   */
  KeyFunctionFactory getKeyFunctionFactory() {
    return mManagedType.getKeyFunctionFactory();
  }



  /**************************************************************************************************
   * 
   * The following class implements the creation of a new and empty BPlusTree
   * 
   *************************************************************************************************/

  /**
   * Returns the managed type defined for this configuration. The managed type specifies if a simple
   * data type is managed (e.g. Integer) or a complex type (which is actually a tuple). The managed
   * type contains further information for the content, see {@link ManagedType}.
   * 
   * @return The type information for the data managed by the tree
   */
  public ManagedType getManagedType() {
    return mManagedType;
  }

  /**************************************************************************************************
   * 
   * The following class implements the restoring of a stored BPlusTree
   * 
   *************************************************************************************************/

  /**
   * Sets the buffered container uses by the BPlusTree.
   * 
   * @param bufferedContainer The buffered container
   */
  public void setBufferedContainer(BufferedContainer bufferedContainer) {
    overrideBufferContainer(bufferedContainer);

  }

  /**
   * This package wide visible method enables the reloading of a tree.
   * 
   * @param systemFilePath The folder where the tree files are stored.
   * @return this instance of BPlusConfiguration with reload mode enabled.
   */
  BPlusConfiguration setReloadMode(String systemFilePath) {
    mLocation = Location.LOCATION_FILESYSTEM;
    mFileSystemFilePath = systemFilePath;
    return this;
  }
}
