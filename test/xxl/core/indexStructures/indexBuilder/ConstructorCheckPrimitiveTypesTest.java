package xxl.core.indexStructures.indexBuilder;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.builder.Builders;

public class ConstructorCheckPrimitiveTypesTest {

  final String MY_COLUMN_1 = "column1";
  final String MY_TABLE_NAME = "table";

  @Test
  public void checkBoolean() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Boolean(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkDouble() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Double(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkFloat() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Float(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkInteger() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Integer(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkLong() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Long(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkShort() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Short(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkTime() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Time(MY_COLUMN_1).getBuilder().create();

  }

  @Test
  public void checkTimestamp() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Timestamp(MY_COLUMN_1).getBuilder().create();

  }


  public String toString() {
    return "Builder checks column type implementation (primitive)";
  }
}
