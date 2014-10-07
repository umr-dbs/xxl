package xxl.core.indexStructures.indexBuilder;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusIndexedSetView;
import xxl.core.indexStructures.builder.Builders;

public class BPlusIndexedSetSubSubSetOperationPrimitiveTest {

  final int MAX_ITEMS_TO_INSERT1 = 100_000;
  /*
   * Set A
   */
  final String MY_TABLE_NAME1 = "MyTable1";
  BPlusIndexedSet mySet1;
  BPlusIndexedSetView subSet;

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void add() {
    subSet.add(MAX_ITEMS_TO_INSERT1);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addAll() {
    subSet.retainAll(java.util.Arrays.asList(10, 20, 30));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void clear() {
    subSet.clear();
  }

  public void containsAllFalse() {
    boolean result1 = subSet.containsAll(java.util.Arrays.asList(-1));
    Assert.assertEquals(result1, true);

    boolean result2 =
        subSet.containsAll(java.util.Arrays.asList(MAX_ITEMS_TO_INSERT1));
    Assert.assertEquals(result2, true);

    boolean result3 = subSet.containsAll(java.util.Arrays.asList("A"));
    Assert.assertEquals(result3, true);
  }

  public void containsAllTrue() {
    boolean result =
        subSet
            .containsAll(java.util.Arrays.asList(0, MAX_ITEMS_TO_INSERT1 - 1));
    Assert.assertEquals(result, true);
  }

  @BeforeMethod
  public void prepareSet() {
    mySet1 =
        Builders.createBPlusTree.Integer(MY_TABLE_NAME1).getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; i++)
      mySet1.add(i);


    int lowerBound = MAX_ITEMS_TO_INSERT1 / 2 - MAX_ITEMS_TO_INSERT1 / 4;
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2 + MAX_ITEMS_TO_INSERT1 / 4;
    subSet = (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void remove() {
    subSet.remove(MAX_ITEMS_TO_INSERT1 / 2);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void removeAll() {
    subSet.removeAll(java.util.Arrays.asList(10, 20, 30));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void retainAll() {
    subSet.retainAll(java.util.Arrays.asList(10, 20, 30));
  }

  public String toString() {
    return "BPlusIndexedSet Operation Test for subsets of subsets";
  }
}
