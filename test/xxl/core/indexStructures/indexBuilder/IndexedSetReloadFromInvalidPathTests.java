package xxl.core.indexStructures.indexBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.IndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;
import xxl.core.util.FileUtils;

/**
 * Tests if default settings will be correctly restored
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class IndexedSetReloadFromInvalidPathTests {

  final String MY_TABLE_NAME = "TableTest";

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadFromEmptyPathFails() throws Exception {
    String dirname = makeTempDir();

    IndexedSet set =
        Builders.loadBPlusTree.from(dirname, MY_TABLE_NAME).getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test
  public void checkReloadWithCaseSensetivePathIsOkay() throws Exception {
    String dirname = makeTempDir();

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer("MyTable")
            .storeAt(dirname.toUpperCase()).getBuilder().create();
    set1.add(1);
    set1.save();

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname.toLowerCase(), "MyTable")
            .getBuilder().create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test
  public void checkReloadWithCaseSensetiveTableNameIsOkay() throws Exception {
    String dirname = makeTempDir();

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer("MYTABLE")
            .storeAt(dirname.toLowerCase()).getBuilder().create();
    set1.add(1);
    set1.save();

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname.toLowerCase(), "mytable")
            .getBuilder().create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_CTR() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".ctr");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_FLT() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".flt");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_JSON() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".json");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_MET2() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(tableName).addInteger("IntColumn"))
            .storeAt(dirname).getBuilder().create();
    set1.add(new Entry(1));
    set1.save();

    File f = new File(dirname + tableName + ".met2");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_META() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".meta");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_MTD() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".mtd");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_RBM() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".rbm");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithDeletedFilesFails_UBM() throws Exception {
    String dirname = makeTempDir();
    String tableName = "MyTable";

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer(tableName).storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    File f = new File(dirname + tableName + ".ubm");
    if (f.exists())
      if (!f.delete())
        throw new Exception(
            "Test requires file manipulation. But is not able to do it.");

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithEmptyPathNameFails() throws Exception {
    IndexedSet set =
        Builders.loadBPlusTree.from("", MY_TABLE_NAME).getBuilder().create();
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithEmptyTableNameFails() throws Exception {
    IndexedSet set = Builders.loadBPlusTree.from("/", "").getBuilder().create();
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithNullPathNameFails() throws Exception {
    IndexedSet set =
        Builders.loadBPlusTree.from(null, MY_TABLE_NAME).getBuilder().create();
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithNullTableNameFails() throws Exception {
    IndexedSet set =
        Builders.loadBPlusTree.from("/", null).getBuilder().create();
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void checkReloadWithWrongTableNameExtensionFails() throws Exception {
    String dirname = makeTempDir();

    BPlusIndexedSet set1 =
        Builders.createBPlusTree.Integer("MyTable").storeAt(dirname)
            .getBuilder().create();
    set1.add(1);
    set1.save();

    BPlusIndexedSet set2 =
        Builders.loadBPlusTree.from(dirname, "MyTable.json").getBuilder()
            .create();

    FileUtils.removeFile(new File(dirname));
  }

  private String makeTempDir() throws Exception {
    String randomTempPath =
        System.getProperty("java.io.tmpdir") + "temp/tests/"
            + System.currentTimeMillis() + "/";

    if (!(new File(randomTempPath).exists())) {
      System.out.print("Make dir: \t" + randomTempPath);
      if (new File(randomTempPath).mkdirs())
        System.out.println("\t[OK]");
      else {
        System.out.println("\t[FAILED]");
        throw new IOException();
      }
    } else
      throw new Exception("Test requires to have a empty director. But \""
          + randomTempPath + "\" exists already.");

    return randomTempPath;
  }


  public String toString() {
    return "IndexedSet reloading with invalid parameters test";
  }
}
