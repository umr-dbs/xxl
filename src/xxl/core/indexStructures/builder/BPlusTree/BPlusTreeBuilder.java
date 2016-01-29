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

package xxl.core.indexStructures.builder.BPlusTree;

import static xxl.core.collections.containers.io.BlockFileContainer.CTR_FILE;
import static xxl.core.collections.containers.io.BlockFileContainer.EXTENSIONS;
import static xxl.core.collections.containers.io.BlockFileContainer.FLT_FILE;
import static xxl.core.collections.containers.io.BlockFileContainer.MTD_FILE;
import static xxl.core.collections.containers.io.BlockFileContainer.RBM_FILE;
import static xxl.core.collections.containers.io.BlockFileContainer.UBM_FILE;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.keyRanges.BooleanKeyRange;
import xxl.core.indexStructures.keyRanges.ByteKeyRange;
import xxl.core.indexStructures.keyRanges.DateKeyRange;
import xxl.core.indexStructures.keyRanges.DoubleKeyRange;
import xxl.core.indexStructures.keyRanges.FloatKeyRange;
import xxl.core.indexStructures.keyRanges.IntegerKeyRange;
import xxl.core.indexStructures.keyRanges.LongKeyRange;
import xxl.core.indexStructures.keyRanges.ShortKeyRange;
import xxl.core.indexStructures.keyRanges.TimeKeyRange;
import xxl.core.indexStructures.keyRanges.TimestampKeyRange;
import xxl.core.indexStructures.keyRanges.TupleKeyRangeFunction;
import xxl.core.indexStructures.separators.BooleanSeparator;
import xxl.core.indexStructures.separators.ByteSeparator;
import xxl.core.indexStructures.separators.DateSeparator;
import xxl.core.indexStructures.separators.DoubleSeparator;
import xxl.core.indexStructures.separators.FloatSeparator;
import xxl.core.indexStructures.separators.IntegerSeparator;
import xxl.core.indexStructures.separators.LongSeparator;
import xxl.core.indexStructures.separators.ShortSeparator;
import xxl.core.indexStructures.separators.TimeSeparator;
import xxl.core.indexStructures.separators.TimestampSeparator;
import xxl.core.indexStructures.separators.TupleSeparator;
import xxl.core.indexStructures.builder.IndexBuilder;
import xxl.core.indexStructures.builder.IndexConfiguration;
import xxl.core.indexStructures.builder.IndexConfiguration.Location;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Creator;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ByteConverter;
import xxl.core.io.converters.DateConverter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FloatConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.io.converters.TimeConverter;
import xxl.core.io.converters.TimestampConverter;
import xxl.core.io.propertyList.Property;
import xxl.core.io.propertyList.PropertyList;
import xxl.core.io.propertyList.json.JSONPrinter;
import xxl.core.io.propertyList.json.JSONReader;
import xxl.core.relational.JavaType;
import xxl.core.relational.metaData.ColumnMetaInfo;
import xxl.core.relational.metaData.TupleMetaData;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.util.Pair;
import xxl.core.util.ConvertUtils;
import xxl.core.util.FileUtils;

/**
 * A concrete implementation of {@link IndexBuilder} to easily create or restore an index with
 * {@link BPlusTree} type. The construction especially the configuration like {@link Container
 * Containers}, type management, storing in memory or persistent storage and so on depends on a
 * given configuration set which is a {@link Creator} object. <br/>
 * <br/>
 * 
 * XXL provides a set of built-in builders, see sub classes of {@link IndexBuilder}.
 * 
 * @see Creator Setting up requirements for the BPlus tree
 * @see IndexBuilder Base class for all index builders
 * @see BPlusTree The index structure itself
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class BPlusTreeBuilder<E>
    extends IndexBuilder<BPlusTreeBuilder, Creator> {

  /*
   * Extends a given column with the feature of comparing to another column
   */
  static class ColumnMetaInfoItem implements Comparable<ColumnMetaInfoItem> {
    /*
     * The columns index of this column
     */
    Integer columnIndex;

    /*
     * The column it self
     */
    ColumnMetaInfo content;

    /**
     * Creates a new instance of {@link ColumnMetaInfo} which is a comparable column.
     * 
     * @param columnIndex The column index from a given table
     * @param content The column it self
     */
    public ColumnMetaInfoItem(Integer columnIndex, ColumnMetaInfo content) {
      this.columnIndex = columnIndex;
      this.content = content;
    }

    @Override
    public int compareTo(ColumnMetaInfoItem o) {
      return columnIndex.compareTo(o.columnIndex);
    }

  }

  /*
   * The file extension for meta information. In order to store meta file of the BPlus tree and
   * table information the file name path is taken from BPlusTreeConfiguration.
   */
  private static final String META_DATA_FILE_EXTENSION = "Meta.json";

  public static final String PROPERTY_BLOCK_SIZE = "Block size";


  public static final String PROPERTY_COLUMN_OBJECT = "Column";

  public static final String PROPERTY_CONTENT_TYPE = "Content Type";

  /*
   * The always required property names inside a PropertyList which contains information about a
   * BPlusTree
   */
  public static final String PROPERTY_INDEX_TYPE = "Index Type";
  public static final String PROPERTY_INDEX_TYPE_BPLUS = "BPlusTree";
  public static final String PROPERTY_KEY_INDICES = "Key indices";
  private static final String PROPERTY_TABLE_COLUMN_AUTOINCREMENT =
      "Auto Increment";
  private static final String PROPERTY_TABLE_COLUMN_CASE_SENSITIVE =
      "Case Sensitive";
  public static final String PROPERTY_TABLE_COLUMN_CATALOG = "Catalog Name";
  public static final String PROPERTY_TABLE_COLUMN_CLASS = "Column Class Name";
  public static final String PROPERTY_TABLE_COLUMN_CONTENT_LENGTH =
      "Content length";

  private static final String PROPERTY_TABLE_COLUMN_CURRENCY = "Currency";
  private static final String PROPERTY_TABLE_COLUMN_DEF_WRITABLE =
      "Def Writable";
  public static final String PROPERTY_TABLE_COLUMN_DISPLAY_SIZE =
      "Column Display Size";
  /*
   * If the tree manages tuple types the following information are additionally required inside a
   * PropertyList which contains information about a BPlusTree
   */
  public static final String PROPERTY_TABLE_COLUMN_INDEX = "Index";
  public static final String PROPERTY_TABLE_COLUMN_LABEL = "Column Label";
  public static final String PROPERTY_TABLE_COLUMN_NAME = "Column Name";
  private static final String PROPERTY_TABLE_COLUMN_NULLABLE = "Nullable";
  public static final String PROPERTY_TABLE_COLUMN_PRECISION = "Precision";
  private static final String PROPERTY_TABLE_COLUMN_READONLY = "ReadOnly";
  public static final String PROPERTY_TABLE_COLUMN_SCALE = "Scale";
  public static final String PROPERTY_TABLE_COLUMN_SCHEMA = "Schema Name";
  private static final String PROPERTY_TABLE_COLUMN_SEARCHABLE = "Searchable";
  private static final String PROPERTY_TABLE_COLUMN_SINGED = "Signed";
  public static final String PROPERTY_TABLE_COLUMN_TYPE = "Column Type";
  public static final String PROPERTY_TABLE_COLUMN_TYPE_NAME =
      "Column Type Name";
  private static final String PROPERTY_TABLE_COLUMN_WRITABLE = "Writable";
  public static final String PROPERTY_TABLE_COLUMNS = "Table columns";
  public static final String PROPERTY_TABLE_NAME = "Table name";
  /*
   * Unpacks the meta data of a BPlusTree from a given PropertyList instance and returns the
   * corresponding tree builder
   */
  public static BPlusTreeBuilder unserialize(PropertyList serializedInstance,
      String storeTreeFilePath) throws NoSuchObjectException {

    if (serializedInstance == null)
      throw new NullPointerException(
          "Unable to unserialize BPlusTree, given serializedInstance is null");
    /*
     * Check if property list describes a BPlus tree
     */
    List<String> treeProperties = new ArrayList<>();
    treeProperties.add(PROPERTY_INDEX_TYPE);
    treeProperties.add(PROPERTY_TABLE_NAME);
    treeProperties.add(PROPERTY_CONTENT_TYPE);
    treeProperties.add(PROPERTY_BLOCK_SIZE);

    if (!serializedInstance.containsAllProperties(treeProperties))
      throw new NoSuchObjectException(
          "Given property list does not describe a BPlus tree. At least on property name is missing (required: "
              + Arrays.toString(treeProperties
                  .toArray(new String[treeProperties.size()])) + ")");

    String indexType =
        (String) serializedInstance.getProperty(PROPERTY_INDEX_TYPE);
    String contentType =
        (String) serializedInstance.getProperty(PROPERTY_CONTENT_TYPE);

    if (indexType.compareToIgnoreCase(PROPERTY_INDEX_TYPE_BPLUS) != 0)
      throw new NoSuchObjectException(
          "Given property list does not describe a BPlus tree. (type is \""
              + indexType + "\")");

    /*
     * It's a BPlus tree, now check the type if it's for primitive or tuple data. Load tree meta
     * data, which is stored by original BPlusTree save schema
     */
    if (contentType.startsWith(PrimitiveType.TYPE_PREFIX)) {
      return unserializePrimitiveType(serializedInstance, storeTreeFilePath);
    } else if (contentType.startsWith(TupleType.TYPE_NAME)) {
      return unserializeTupleType(serializedInstance, storeTreeFilePath);
    } else
      throw new UnsupportedOperationException(
          "Unknown content-type for index structure (what is \"" + contentType
              + "\")");
  }
  /*
   * Unpacks the required information for a builder from a given PropertyList and returns a tree
   * builder with this information. Note: this method restores trees which manages primitive data
   * types like Integer.
   */
  private static BPlusTreeBuilder unserializePrimitiveType(
      PropertyList serializedInstance, String storeTreeFilePath) {
    JavaType restoreType =
        ConvertUtils.toJavaType(((String) serializedInstance
            .getProperty(PROPERTY_CONTENT_TYPE))
            .substring(PrimitiveType.TYPE_PREFIX.length()));
    String tableName =
        (String) serializedInstance.getProperty(PROPERTY_TABLE_NAME);
    int blockSize = (int) serializedInstance.getProperty(PROPERTY_BLOCK_SIZE);

    Creator requirements =
        new Creator(new PrimitiveType(restoreType, tableName));
    requirements.setBlockSize(blockSize);

    return (BPlusTreeBuilder) requirements.setReloadMode(storeTreeFilePath)
        .getBuilder();
  }
  /*
   * Unpacks the required information for a builder from a given PropertyList and returns a tree
   * builder with this information. Note: this method restores trees which manages tuple data types.
   */
  private static BPlusTreeBuilder unserializeTupleType(
      PropertyList serializedInstance, String storeTreeFilePath)
      throws NoSuchObjectException {

    /*
     * The stored table name
     */
    String tableName =
        (String) serializedInstance.getProperty(PROPERTY_TABLE_NAME);

    /*
     * The stored block size
     */
    int blockSize = (int) serializedInstance.getProperty(PROPERTY_BLOCK_SIZE);

    /*
     * Check if property list is well formed
     */
    List<String> tupleTreeProperties = new ArrayList<String>();
    tupleTreeProperties.add(PROPERTY_KEY_INDICES);
    tupleTreeProperties.add(PROPERTY_TABLE_COLUMNS);
    if (!serializedInstance.containsAllProperties(tupleTreeProperties))
      throw new NoSuchObjectException(
          "Given property list does not describe a BPlus tree for tuples. At least on property name is missing (required: "
              + Arrays.toString(tupleTreeProperties
                  .toArray(new String[tupleTreeProperties.size()])) + ")");

    /*
     * Load data
     */
    ArrayList<Object> keyIndicesObj =
        (ArrayList<Object>) serializedInstance
            .getProperty(PROPERTY_KEY_INDICES);
    int[] keyIndices = new int[keyIndicesObj.size()];
    for (int i = 0; i < keyIndices.length; i++)
      keyIndices[i] = (int) keyIndicesObj.get(i);

    ArrayList<Object> columnsObj =
        (ArrayList<Object>) serializedInstance
            .getProperty(PROPERTY_TABLE_COLUMNS);
    String[] columns = new String[columnsObj.size()];
    for (int i = 0; i < columns.length; i++)
      columns[i] = (String) columnsObj.get(i);

    List<String> columnProperties = new ArrayList<String>();
    columnProperties.add(PROPERTY_TABLE_COLUMN_CATALOG);
    columnProperties.add(PROPERTY_TABLE_COLUMN_CLASS);
    columnProperties.add(PROPERTY_TABLE_COLUMN_CONTENT_LENGTH);
    columnProperties.add(PROPERTY_TABLE_COLUMN_DISPLAY_SIZE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_INDEX);
    columnProperties.add(PROPERTY_TABLE_COLUMN_LABEL);
    columnProperties.add(PROPERTY_TABLE_COLUMN_NAME);
    columnProperties.add(PROPERTY_TABLE_COLUMN_PRECISION);
    columnProperties.add(PROPERTY_TABLE_COLUMN_SCALE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_SCHEMA);
    columnProperties.add(PROPERTY_TABLE_COLUMN_TYPE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_TYPE_NAME);
    columnProperties.add(PROPERTY_TABLE_COLUMN_AUTOINCREMENT);
    columnProperties.add(PROPERTY_TABLE_COLUMN_CASE_SENSITIVE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_CURRENCY);
    columnProperties.add(PROPERTY_TABLE_COLUMN_DEF_WRITABLE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_NULLABLE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_READONLY);
    columnProperties.add(PROPERTY_TABLE_COLUMN_SEARCHABLE);
    columnProperties.add(PROPERTY_TABLE_COLUMN_SINGED);
    columnProperties.add(PROPERTY_TABLE_COLUMN_WRITABLE);

    List<ColumnMetaInfoItem> sortedColumnMetaInfos = new ArrayList<>();

    for (String column : columns) {
      PropertyList columnPropertyList =
          serializedInstance.getPropertyList(column);

      if (!columnPropertyList.containsAllProperties(columnProperties))
        throw new NoSuchObjectException(
            "Given property list does not describe a column. At least on property name is missing (required: "
                + Arrays.toString(columnProperties
                    .toArray(new String[columnProperties.size()])) + ")");

      int columnIndex =
          (int) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_INDEX);

      String catalog =
          (String) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_CATALOG);
      String clazz =
          (String) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_CLASS);
      int contentLength =
          (int) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_CONTENT_LENGTH);
      int displaySize =
          (int) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_DISPLAY_SIZE);
      String columnLabel =
          (String) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_LABEL);
      String columnName =
          (String) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_NAME);
      int precision =
          (int) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_PRECISION);
      int scale =
          (int) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_SCALE);
      String schema =
          (String) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_SCHEMA);
      int type =
          (int) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_TYPE);
      String columnTypeName =
          (String) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_TYPE_NAME);
      boolean autoIncrementEnabled =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_AUTOINCREMENT);
      boolean caseSensitive =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_CASE_SENSITIVE);
      boolean currency =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_CURRENCY);
      boolean definitelyWritable =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_DEF_WRITABLE);
      int nullable =
          (int) columnPropertyList.getProperty(PROPERTY_TABLE_COLUMN_NULLABLE);
      boolean readOnly =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_READONLY);
      boolean searchable =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_SEARCHABLE);
      boolean singed =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_SINGED);
      boolean writable =
          (Boolean) columnPropertyList
              .getProperty(PROPERTY_TABLE_COLUMN_WRITABLE);

      ColumnMetaInfo columnMetaInfo = null;

      if (ConvertUtils.toJavaType(ConvertUtils.toRelationalType(type)) == JavaType.STRING)
        columnMetaInfo =
            new ColumnMetaInfo(ConvertUtils.toRelationalType(type),
                contentLength, columnName);
      else
        columnMetaInfo =
            new ColumnMetaInfo(ConvertUtils.toRelationalType(type), columnName);


      columnMetaInfo.setAutoIncrementEnabled(autoIncrementEnabled);
      columnMetaInfo.setCaseSensitive(caseSensitive);
      columnMetaInfo.setCatalogName(catalog);
      columnMetaInfo.setColumnClassName(clazz);
      columnMetaInfo.setColumnDisplaySize(displaySize);
      columnMetaInfo.setColumnLabel(columnLabel);
      columnMetaInfo.setColumnTypeName(columnTypeName);
      columnMetaInfo.setCurrency(currency);
      columnMetaInfo.setDefinitelyWritable(definitelyWritable);
      columnMetaInfo.setNullable(nullable);
      columnMetaInfo.setPrecision(precision);
      columnMetaInfo.setReadOnly(readOnly);
      columnMetaInfo.setScale(scale);
      columnMetaInfo.setSchemaName(schema);
      columnMetaInfo.setSearchable(searchable);
      columnMetaInfo.setSinged(singed);
      columnMetaInfo.setWritable(writable);

      sortedColumnMetaInfos.add(new ColumnMetaInfoItem(columnIndex,
          columnMetaInfo));

    }

    /*
     * Build columns list in the column order given by property list
     */
    List<ColumnMetaInfo> columnMeta = new ArrayList<>();
    Collections.sort(sortedColumnMetaInfos);
    for (ColumnMetaInfoItem item : sortedColumnMetaInfos)
      columnMeta.add(item.content);

    try {
      TupleMetaData tupleMetaData =
          new TupleMetaData(tableName,
              columnMeta.toArray(new ColumnMetaInfo[columnMeta.size()]));
      Creator requirements = new Creator(new TupleType(tupleMetaData));
      requirements.setBlockSize(blockSize);
      requirements.setCompoundKey(keyIndices);

      return (BPlusTreeBuilder) requirements.setReloadMode(storeTreeFilePath)
          .getBuilder();

    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  /*
   * Enables or disables the reloading mode. By default a new tree should be created and not loaded.
   */
  protected boolean mReload = false;

  /*
   * Flag to indicate if this builder is in reloading mode or not. By default it is not.
   */
  private boolean mRestoredInstance = false;

  /**
   * Setup the builder with the given configuration.
   * 
   * @param configuration Your BPlus tree requirements
   */
  public BPlusTreeBuilder(BPlusConfiguration.Creator configuration) {
    super(configuration);
  }

  /**
   * Constructs, initialize and returns a BPlusTree which fits your requirements given by
   * <code>BPlusTreeConfiguration</code> in the constructor of this object.
   */
  @Override
  public BPlusIndexedSet<E> create() {

    BPlusConfiguration configuration = (BPlusConfiguration) mIndexConfiguration;

    BPlusTree retval = new BPlusTree(configuration.getBlockSize());

    Container bufferedContainer = null;
    Container fileContainer = null;

    if (mReload) {
      String tableDomain =
          configuration.getFileSystemFilePath() + File.separatorChar
              + configuration.getManagedType().getTableName();
      File checkCtr = new File(tableDomain + EXTENSIONS[CTR_FILE]);
      File checkFlt = new File(tableDomain + EXTENSIONS[FLT_FILE]);
      File checkMtd = new File(tableDomain + EXTENSIONS[MTD_FILE]);
      File checkRbm = new File(tableDomain + EXTENSIONS[RBM_FILE]);
      File checkUbm = new File(tableDomain + EXTENSIONS[UBM_FILE]);
      File checkMeta =
          new File(tableDomain + BPlusIndexedSet.META_FILE_EXTENSION);
      File checkJson =
          new File(tableDomain + "Meta." + JSONReader.FILE_EXTENSION);

      List<String> missingFileList = new ArrayList<>();

      if (!checkCtr.exists() || checkCtr.length() == 0)
        missingFileList.add(checkCtr.getName());

      if (!checkFlt.exists()) missingFileList.add(checkFlt.getName());

      if (!checkMtd.exists() || checkMtd.length() == 0)
        missingFileList.add(checkMtd.getName());

      if (!checkRbm.exists() || checkRbm.length() == 0)
        missingFileList.add(checkUbm.getName());

      if (!checkUbm.exists() || checkUbm.length() == 0)
        missingFileList.add(checkUbm.getName());

      if (!checkMeta.exists() || checkMeta.length() == 0)
        missingFileList.add(checkMeta.getName());

      if (!checkJson.exists() || checkJson.length() == 0)
        missingFileList.add(checkJson.getName());

      if (!missingFileList.isEmpty())
        throw new IllegalAccessError(
            "At least one required file is missing or empty in \""
                + FileUtils.getFilePath(tableDomain)
                + "\". Check \n"
                + Arrays.toString(missingFileList
                    .toArray(new String[missingFileList.size()])));

      fileContainer = new BlockFileContainer(tableDomain);
    } else {
      if (configuration.storeAtFileSystem())
        fileContainer = configuration.getFileContainer();
    }

    if (configuration.getLocation().equals(Location.LOCATION_FILESYSTEM)) {
      Container converterContainer =
          new ConverterContainer(fileContainer, retval.nodeConverter());
      bufferedContainer =
          new BufferedContainer(converterContainer, configuration.getBuffer());
      configuration.setBufferedContainer((BufferedContainer) bufferedContainer);
    } else
      bufferedContainer = configuration.getBufferedContainer();
    configuration.setBufferedContainer((BufferedContainer) bufferedContainer);

    try {
      if (configuration.storeAtFileSystem()
          && (configuration instanceof BPlusConfiguration.Creator))
        storeMetaData((Creator) configuration);
    } catch (Exception e) {
      throw new RuntimeException(Arrays.toString(e.getStackTrace()) + "\n"
          + e.getMessage());
    }

    if (mReload) {
      try {
        Pair<IndexEntry, KeyRange> restoredTreeMeta =
            readTree(retval, configuration.getFileSystemFilePath()
                + File.separatorChar
                + configuration.getManagedType().getTableName()
                + BPlusIndexedSet.META_FILE_EXTENSION, configuration);
        retval.initialize(restoredTreeMeta.getElement1(), restoredTreeMeta
            .getElement2(), configuration.getKeyFunctionFactory()
            .getKeyFunction(), bufferedContainer, configuration
            .getKeyFunctionFactory().getKeyConverter(), configuration
            .getDataConverter(), configuration.getKeyFunctionFactory()
            .getKeyValueSeparatorFunction(), configuration
            .getKeyFunctionFactory().getKeyRangeFunction(1));

      } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalArgumentException();
      }
    } else {
      retval.initialize(null, null, configuration.getKeyFunctionFactory()
          .getKeyFunction(), bufferedContainer, configuration
          .getKeyFunctionFactory().getKeyConverter(), configuration
          .getDataConverter(), configuration.getKeyFunctionFactory()
          .getKeyValueSeparatorFunction(), configuration
          .getKeyFunctionFactory().getKeyRangeFunction(1));
    }

    return new BPlusIndexedSet(retval, this);
  }

  /*
   * This package wide visible method enables the reloading mode and returns the current instance.
   */
  BPlusTreeBuilder enableLoadingMode() {
    mRestoredInstance = true;
    return this;
  }


  /**
   * Enables or disables the reloading mode.
   * 
   * @return BPlusTreeBuilder
   */
  public BPlusTreeBuilder enableReload() {
    mReload = true;
    return this;
  }

  /*
   * Reloads the required IndexEntry and KeyRange of the previously stored BPlusTree. Please note
   * reloading for primitive types is done with XXL built-in converter (which loads the information
   * from a binary file (reading order matters!)) but tuple types is done this
   * JSONReader/PropertyList (which loads the information from a plain text file (reading order does
   * not matter)).
   * 
   * In general the reading requires * a source where the meta data is taken from (String in, a
   * folder) * the tree's level information * the tree's root key id * the tree's root separator *
   * the tree's key range * an instance of BPlusTree which is initialized with this information
   * (BPlusTree tree) * a configuration which specifies which files from "in" should be loaded an
   * which kind of data should be managed
   * 
   * the return value is a pair of the tree's IndexEntry (built with level, key id and root
   * separator) and the key range.
   */
  private Pair<IndexEntry, KeyRange> readTree(BPlusTree tree, String in,
      BPlusConfiguration configuration) throws IOException {
    DataInputStream input =
        new DataInputStream(new FileInputStream(new File(in)));
    int level = IntegerConverter.DEFAULT_INSTANCE.readInt(input);

    Long id = -1l;

    Separator rootSeparator = null;
    IndexEntry rootEntry = null;
    KeyRange keyRange = null;

    switch (configuration.getManagedType().getContentClass()) {
      case CONTENT_CLASS_PRIMITIVE: {
        JavaType subclass =
            configuration.getManagedType().getContentClassSubType();
        switch (subclass) {
          case BOOLEAN: {
            Boolean rootKey = BooleanConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Boolean minKey = BooleanConverter.DEFAULT_INSTANCE.read(input);
            Boolean maxKey = BooleanConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new BooleanSeparator(rootKey);
            keyRange = new BooleanKeyRange(minKey, maxKey);
          }
            break;
          case BYTE: {
            Byte rootKey = ByteConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Byte minKey = ByteConverter.DEFAULT_INSTANCE.read(input);
            Byte maxKey = ByteConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new ByteSeparator(rootKey);
            keyRange = new ByteKeyRange(minKey, maxKey);
          }
            break;
          case DATE: {
            Date rootKey = DateConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Date minKey = DateConverter.DEFAULT_INSTANCE.read(input);
            Date maxKey = DateConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new DateSeparator(rootKey.getTime());
            keyRange = new DateKeyRange(minKey.getTime(), maxKey.getTime());
          }
            break;
          case DOUBLE: {
            Double rootKey = DoubleConverter.DEFAULT_INSTANCE.readDouble(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Double minKey = DoubleConverter.DEFAULT_INSTANCE.readDouble(input);
            Double maxKey = DoubleConverter.DEFAULT_INSTANCE.readDouble(input);
            rootSeparator = new DoubleSeparator(rootKey);
            keyRange = new DoubleKeyRange(minKey, maxKey);
          }
            break;
          case FLOAT: {
            Float rootKey = FloatConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Float minKey = FloatConverter.DEFAULT_INSTANCE.read(input);
            Float maxKey = FloatConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new FloatSeparator(rootKey);
            keyRange = new FloatKeyRange(minKey, maxKey);
          }
            break;
          case INT: {
            Integer rootKey = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Integer minKey = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
            Integer maxKey = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
            rootSeparator = new IntegerSeparator(rootKey);
            keyRange = new IntegerKeyRange(minKey, maxKey);
          }
            break;
          case LONG: {
            Long rootKey = LongConverter.DEFAULT_INSTANCE.readLong(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Long minKey = LongConverter.DEFAULT_INSTANCE.readLong(input);
            Long maxKey = LongConverter.DEFAULT_INSTANCE.readLong(input);
            rootSeparator = new LongSeparator(rootKey);
            keyRange = new LongKeyRange(minKey, maxKey);
          }
            break;
          case SHORT: {
            Short rootKey = ShortConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Short minKey = ShortConverter.DEFAULT_INSTANCE.read(input);
            Short maxKey = ShortConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new ShortSeparator(rootKey);
            keyRange = new ShortKeyRange(minKey, maxKey);
          }
            break;
          case TIME: {
            Time rootKey = TimeConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Time minKey = TimeConverter.DEFAULT_INSTANCE.read(input);
            Time maxKey = TimeConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new TimeSeparator(rootKey);
            keyRange = new TimeKeyRange(minKey, maxKey);
          }
            break;
          case TIMESTAMP: {
            Timestamp rootKey = TimestampConverter.DEFAULT_INSTANCE.read(input);
            id = LongConverter.DEFAULT_INSTANCE.readLong(input);

            Timestamp minKey = TimestampConverter.DEFAULT_INSTANCE.read(input);
            Timestamp maxKey = TimestampConverter.DEFAULT_INSTANCE.read(input);
            rootSeparator = new TimestampSeparator(rootKey);
            keyRange = new TimestampKeyRange(minKey, maxKey);
          }
            break;
          default:
            throw new UnsupportedOperationException(
                "Not implemented yet for \"" + subclass + "\"");
        }
      }
        break;
      case CONTENT_CLASS_COMPLEX: {
        try {
          BPlusConfiguration config = (BPlusConfiguration) mIndexConfiguration;
          ResultSetMetaData metaData = config.getManagedType().getMetaData();

          String filename =
              config.getFileSystemFilePath() + "/" + metaData.getTableName(0)
                  + BPlusIndexedSet.BPLUS_TUPLE_MET_EXTENSION;

          JSONReader reader = new JSONReader();
          PropertyList treeInformation =
              reader
                  .read(new BufferedInputStream(new FileInputStream(filename)));

          // Check if format is valid
          List<String> keys = new ArrayList<>();
          keys.add("RootKey");
          keys.add("RootID");
          keys.add("MinKey");
          keys.add("MaxKey");
          if (!treeInformation.containsAllProperties(keys))
            throw new IllegalArgumentException("File \"" + filename
                + "\" is not a tree meta file or it is damaged.");

          ArrayList<Object> rootKey =
              (ArrayList) treeInformation.getProperty("RootKey");
          id =
              Long.valueOf(String.valueOf(treeInformation.getProperty("RootID")));

          ArrayList<Object> minKey =
              (ArrayList) treeInformation.getProperty("MinKey");
          ArrayList<Object> maxKey =
              (ArrayList) treeInformation.getProperty("MaxKey");

          rootSeparator =
              new TupleSeparator(new ColumnComparableArrayTuple(
                  rootKey.toArray(new Object[rootKey.size()])));
          keyRange =
              new TupleKeyRangeFunction(new ColumnComparableArrayTuple(
                  minKey.toArray(new Object[minKey.size()])),
                  new ColumnComparableArrayTuple(maxKey
                      .toArray(new Object[maxKey.size()])));

        } catch (SQLException error) {};

      }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown Content class: Not supported yet");
    }

    rootEntry =
        ((IndexEntry) tree.createIndexEntry(level).initialize(id))
            .initialize(rootSeparator);

    return new Pair<>(rootEntry, keyRange);
  }

  /*
   * Packs the meta data of a BPlusTree (e.g. table name, content type etc.) into a PropertyList
   * object.
   * 
   * @see xxl.core.io.propertyList.IPropertyListSerializable#serialize(java.lang.Object)
   */
  @Override
  public PropertyList serialize(Creator config) throws Exception {
    PropertyList treeInfo = new PropertyList();
    treeInfo.add(new Property(PROPERTY_INDEX_TYPE, PROPERTY_INDEX_TYPE_BPLUS));
    treeInfo.add(new Property(PROPERTY_TABLE_NAME, config.getTableName()));
    treeInfo.add(new Property(PROPERTY_CONTENT_TYPE, config.getContentType()));
    treeInfo.add(new Property(PROPERTY_BLOCK_SIZE, config.getBlockSize()));

    /*
     * Write additional meta data for columns etc. if the the BPlus tree stores tuples
     */
    if (config.getManagedType() instanceof TupleType) {

      int[] ki = config.getCompoundKeyIndices();
      Object[] keyIndices = new Object[ki.length];
      for (int i = 0; i < ki.length; i++)
        keyIndices[i] = ki[i];

      ExtendedResultSetMetaData tableMetaData = config.getMetaData();

      treeInfo.add(new Property(PROPERTY_KEY_INDICES, keyIndices));

      int columnCount = tableMetaData.getColumnCount();
      String[] columnNameArray = new String[columnCount];
      for (int i = 0; i < columnCount; i++)
        columnNameArray[i] = PROPERTY_COLUMN_OBJECT + (i + 1);

      treeInfo.add(new Property(PROPERTY_TABLE_COLUMNS, columnNameArray));
      // Object[] columnInformation = new Object[columnCount];

      for (int i = 1; i <= columnCount; i++) {
        PropertyList columnInfo = new PropertyList();
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_INDEX, i));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_CATALOG,
            tableMetaData.getCatalogName(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_CLASS, tableMetaData
            .getColumnClassName(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_DISPLAY_SIZE,
            tableMetaData.getColumnDisplaySize(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_LABEL, tableMetaData
            .getColumnLabel(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_NAME, tableMetaData
            .getColumnName(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_TYPE, tableMetaData
            .getColumnType(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_TYPE_NAME,
            tableMetaData.getColumnTypeName(i)));

        int columnType = tableMetaData.getColumnType(i);
        JavaType javaType =
            ConvertUtils.toJavaType(ConvertUtils.toRelationalType(columnType));
        if (javaType.equals(JavaType.STRING))
          columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_CONTENT_LENGTH,
              tableMetaData.getContentLength(i)));
        else
          columnInfo
              .add(new Property(PROPERTY_TABLE_COLUMN_CONTENT_LENGTH, -1));

        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_PRECISION,
            tableMetaData.getPrecision(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_SCALE, tableMetaData
            .getScale(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_SCHEMA, tableMetaData
            .getSchemaName(i)));

        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_AUTOINCREMENT,
            tableMetaData.isAutoIncrement(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_CASE_SENSITIVE,
            tableMetaData.isCaseSensitive(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_CURRENCY,
            tableMetaData.isCurrency(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_DEF_WRITABLE,
            tableMetaData.isDefinitelyWritable(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_NULLABLE,
            tableMetaData.isNullable(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_READONLY,
            tableMetaData.isReadOnly(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_SEARCHABLE,
            tableMetaData.isSearchable(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_SINGED, tableMetaData
            .isSigned(i)));
        columnInfo.add(new Property(PROPERTY_TABLE_COLUMN_WRITABLE,
            tableMetaData.isWritable(i)));


        treeInfo.add(new Property(PROPERTY_COLUMN_OBJECT + i, columnInfo));
      }
    }

    return treeInfo;
  }

  /**
   * Store BPlus meta data
   * 
   * @param config Source configuration which contains the information
   * @throws Exception
   */
  private void storeMetaData(Creator config) throws Exception {
    File metaDataFile =
        new File(config.getFileSystemFilePath() + META_DATA_FILE_EXTENSION);
    FileOutputStream metaDataOutput = new FileOutputStream(metaDataFile);

    PropertyList treeInfo = serialize(config);

    JSONPrinter treeInfoPrinter = new JSONPrinter(treeInfo);
    treeInfoPrinter.print(metaDataOutput);

  }

  /**
   * Prints the underlying index configuration
   */
  public String toString() {
    return ((IndexConfiguration) mIndexConfiguration).toString();
  }


}
