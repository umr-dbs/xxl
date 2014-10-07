package xxl.core.indexStructures.indexBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.indexStructures.builder.IndexConfiguration.Location;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Creator;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Loader;
import xxl.core.indexStructures.builder.BPlusTree.ManagedType;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.relational.metaData.ExtendedColumnMetaData;
import xxl.core.relational.schema.Schemas;
import xxl.core.util.FileUtils;

/**
 * Tests if default settings will be correctly restored
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class IndexedSetSaveReloadRelationalTest {

  private static final String PATH_TO_STORE_DATA = System
      .getProperty("java.io.tmpdir")
      + "temp/tests/IndexedSetSaveReloadPrimitivesTest/"
      + System.currentTimeMillis() + "/";
  boolean isNewDirectory = false;
  final int MAX_ITEMS_TO_INSERT = 100_000;
  final String MY_TABLE_NAME = "TableTest";

  BPlusIndexedSet mySet;

  int oldBlockSize;
  String oldContentType;
  Location oldLocation;
  ManagedType oldManagedType;
  String oldTableName;

  BPlusConfiguration restoredConfiguration;
  Loader restoredCreator;

  @Test
  public void checkColumnMetaContentLength() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getContentLength(i),
          oldMeta.getContentLength(i));
  }

  @Test
  public void checkColumnMetaDataCatalogName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getCatalogName(i),
          oldMeta.getCatalogName(i));
  }

  @Test
  public void checkColumnMetaDataColumnClassName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnClassName(i),
          oldMeta.getColumnClassName(i));
  }

  @Test
  public void checkColumnMetaDataColumnCount() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    Assert
        .assertEquals(restoredMeta.getColumnCount(), oldMeta.getColumnCount());
  }


  @Test
  public void checkColumnMetaDataColumnDisplaySize() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnDisplaySize(i),
          oldMeta.getColumnDisplaySize(i));
  }

  @Test
  public void checkColumnMetaDataColumnLabel() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnLabel(i),
          oldMeta.getColumnLabel(i));
  }

  // -----

  @Test
  public void checkColumnMetaDataColumnName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnName(i),
          oldMeta.getColumnName(i));
  }

  @Test
  public void checkColumnMetaDataColumnType() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnType(i),
          oldMeta.getColumnType(i));
  }

  @Test
  public void checkColumnMetaDataColumnTypeName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getColumnTypeName(i),
          oldMeta.getColumnTypeName(i));
  }

  @Test
  public void checkColumnMetaDataPrecision() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert
          .assertEquals(restoredMeta.getPrecision(i), oldMeta.getPrecision(i));
  }

  @Test
  public void checkColumnMetaDataScale() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getScale(i), oldMeta.getScale(i));
  }

  @Test
  public void checkColumnMetaDataSchemaName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert.assertEquals(restoredMeta.getSchemaName(i),
          oldMeta.getSchemaName(i));
  }

  @Test
  public void checkColumnMetaDataSingleColumnAutoIncrement()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isAutoIncrement(),
          oldColumnData.isAutoIncrement());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnCaseSensitive()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isCaseSensitive(),
          oldColumnData.isCaseSensitive());
    }
  }

  // ++++++++
  @Test
  public void checkColumnMetaDataSingleColumnColumnClassName()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnClassName(),
          oldColumnData.getColumnClassName());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnData() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getScale(),
          oldColumnData.getScale());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnDisplaySize()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnDisplaySize(),
          oldColumnData.getColumnDisplaySize());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnLabel() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnLabel(),
          oldColumnData.getColumnLabel());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnName(),
          oldColumnData.getColumnName());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnType() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnType(),
          oldColumnData.getColumnType());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnColumnTypeName()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getColumnTypeName(),
          oldColumnData.getColumnTypeName());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnCurrency() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isCurrency(),
          oldColumnData.isCurrency());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnDefinitelyWritable()
      throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isDefinitelyWritable(),
          oldColumnData.isDefinitelyWritable());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnMaxLength() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getMaxContainingStringLength(),
          oldColumnData.getMaxContainingStringLength());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnNullable() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isNullable(),
          oldColumnData.isNullable());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnPrecision() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getPrecision(),
          oldColumnData.getPrecision());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnReadOnly() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isReadOnly(),
          oldColumnData.isReadOnly());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnSchemaName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getSchemaName(),
          oldColumnData.getSchemaName());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnSearchable() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isSearchable(),
          oldColumnData.isSearchable());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnSigned() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isSigned(),
          oldColumnData.isSigned());
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkColumnMetaDataSingleColumnTableName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.getTableName(),
          oldColumnData.getTableName());
    }
  }

  @Test
  public void checkColumnMetaDataSingleColumnWritable() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++) {
      ExtendedColumnMetaData restoredColumnData =
          restoredMeta.getColumnMetaData(i);
      ExtendedColumnMetaData oldColumnData = oldMeta.getColumnMetaData(i);

      Assert.assertEquals(restoredColumnData.isWritable(),
          oldColumnData.isWritable());
    }
  }

  @Test
  public void checkColumnMetaDataTableName() throws SQLException {
    ExtendedResultSetMetaData restoredMeta =
        restoredConfiguration.getManagedType().getMetaData();
    ExtendedResultSetMetaData oldMeta = oldManagedType.getMetaData();

    for (int i = 1; i <= restoredMeta.getColumnCount(); i++)
      Assert
          .assertEquals(restoredMeta.getTableName(i), oldMeta.getTableName(i));
  }

  @Test
  public void checkContentType() {
    Assert.assertEquals(oldContentType, restoredConfiguration.getManagedType()
        .getContentType());
  }

  @Test
  public void checkKeyIndices() {
    Assert.assertEquals(oldManagedType.getCompoundKeyIndices(),
        oldManagedType.getCompoundKeyIndices());
  }

  @Test
  public void checkLocation() {
    Assert.assertEquals(oldLocation, restoredConfiguration.getLocation());
  }

  @Test
  public void checkManagedType() {
    Assert.assertEquals(oldManagedType.getContentType(), restoredConfiguration
        .getManagedType().getContentType());
    Assert.assertEquals(oldManagedType.getTableName(), restoredConfiguration
        .getManagedType().getTableName());
    Assert.assertEquals(oldManagedType.getContentClass(), restoredConfiguration
        .getManagedType().getContentClass());
  }

  @Test
  public void checkReloadedBlockSize() {
    Assert.assertEquals(oldBlockSize, restoredConfiguration.getBlockSize());
  }

  @Test
  public void checkTableName() {
    Assert.assertEquals(oldTableName, restoredConfiguration.getManagedType()
        .getTableName());
  }

  @BeforeTest
  public void createIndexedSetAndSave() throws IOException, SQLException {
    if (!(new File(PATH_TO_STORE_DATA).exists())) {
      isNewDirectory = true;
      System.out.print("Make dir: \t" + PATH_TO_STORE_DATA);
      if (new File(PATH_TO_STORE_DATA).mkdirs())
        System.out.println("\t[OK]");
      else {
        System.out.println("\t[FAILED]");
        throw new IOException();
      }

    }

    Creator creator =
        Builders.createBPlusTree.Tuples(Schemas.createSchema(MY_TABLE_NAME)
            .addInteger("INT_KEY1_COLUMN").addInteger("INT_KEY2_COLUMN")
            .addVarChar("STR_COLUMN", 10));
    // creator.setBlockSize(2048);
    creator.storeAt(PATH_TO_STORE_DATA);

    oldBlockSize = creator.getBlockSize();
    oldContentType = creator.getContentType();
    oldLocation = creator.getLocation();
    oldManagedType = creator.getManagedType();
    oldTableName = creator.getTableName();

    mySet = creator.getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT; i++)
      mySet.add(new Entry(i, i * i, "Value_" + i % 10));

    mySet.save();
  }

  @Test
  public void recreateIndexedSet() {
    mySet = restoredCreator.getBuilder().create();
  }


  @BeforeMethod
  public void reloadMetaData() throws NoSuchObjectException,
      FileNotFoundException {
    restoredCreator =
        Builders.loadBPlusTree.from(PATH_TO_STORE_DATA, MY_TABLE_NAME);
    restoredConfiguration =
        (BPlusConfiguration) restoredCreator.getBuilder()
            .getIndexConfiguration();
  }

  @AfterTest
  public void removeTempData() throws FileNotFoundException {
    if (isNewDirectory) {
      System.out.print("Remove dir: \t" + PATH_TO_STORE_DATA);
      if (FileUtils.removeFile(new File(PATH_TO_STORE_DATA)))
        System.out.println("\t[OK]");
      else
        System.out.println("\t[FAILED]");
    }
  }



  public String toString() {
    return "IndexedSet save and reload (relational)";
  }
}
