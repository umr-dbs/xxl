package xxl.core.indexStructures.indexBuilder;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.builder.Builders;

public class IndexedSetOverflowTest {

  final int MAX_ITEMS_TO_INSERT = 100_000;
  final String MY_TABLE_NAME = "MyTable";
  BPlusIndexedSet mySet;

  /**
   * Insert items from 0-MAX_ITEMS_TO_INSERT into the tree and checks via range query (of the
   * underlying tree) if all items correctly added
   */
  @Test
  public void checkOverflowBySizeOf() {
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

  @BeforeMethod
  public void prepareSet() {
    mySet =
        Builders.createBPlusTree.Integer(MY_TABLE_NAME).getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT; i++)
      mySet.add(i);
  }



  public String toString() {
    return "IndexedSet overflow test";
  }
}
