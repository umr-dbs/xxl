package xxl.core.indexStructures.indexBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NoSuchObjectException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.indexStructures.builder.IndexConfiguration.Location;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Creator;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Loader;
import xxl.core.indexStructures.builder.BPlusTree.ManagedType;
import xxl.core.util.FileUtils;

/**
 * Tests if default settings will be correctly restored
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class IndexedSetSaveReloadPrimitivesTestBlockSize {

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
  public void checkContentType() {
    Assert.assertEquals(oldContentType, oldManagedType.getContentType());
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
  public void checkReloadedTreeContent() {
    Cursor c =
        ((BPlusTree) mySet.getIndexStructure()).rangeQuery(0,
            MAX_ITEMS_TO_INSERT);
    int size = 0;
    while (c.hasNext()) {
      c.next();
      size++;
    }

    Assert.assertEquals(size, MAX_ITEMS_TO_INSERT);
  }

  @Test
  public void checkTableName() {
    Assert.assertEquals(oldTableName, restoredConfiguration.getManagedType()
        .getTableName());
  }

  @BeforeTest
  public void createIndexedSetAndSave() throws IOException {
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

    Creator creator = Builders.createBPlusTree.Integer(MY_TABLE_NAME);

    /*
     * To test
     */
    creator.setBlockSize(2048);
    /*
     * 
     */

    creator.storeAt(PATH_TO_STORE_DATA);

    oldBlockSize = creator.getBlockSize();
    oldContentType = creator.getContentType();
    oldLocation = creator.getLocation();
    oldManagedType = creator.getManagedType();
    oldTableName = creator.getTableName();

    mySet = creator.getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT; i++)
      mySet.add(i);

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
    return "IndexedSet save and reload (primitives)";
  }
}
