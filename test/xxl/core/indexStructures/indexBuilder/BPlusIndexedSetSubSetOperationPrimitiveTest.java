package xxl.core.indexStructures.indexBuilder;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusIndexedSetView;
import xxl.core.indexStructures.builder.Builders;

public class BPlusIndexedSetSubSetOperationPrimitiveTest {

  final int MAX_ITEMS_TO_INSERT1 = 100_000;
  /*
   * Set A
   */
  final String MY_TABLE_NAME1 = "MyTable1";
  BPlusIndexedSet mySet1;

  @BeforeMethod
  public void prepareSet() {
    mySet1 =
        Builders.createBPlusTree.Integer(MY_TABLE_NAME1).getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; i++)
      mySet1.add(i);
  }


  @Test
  public void subSetContentTest() {
    /*
     * Create subset (which lower bound is greater than the super sets lower bound and which upper
     * bound is less than the super sets upper bound
     */
    int lowerBound = MAX_ITEMS_TO_INSERT1 / 3;
    int upperBound = 2 * MAX_ITEMS_TO_INSERT1 / 3;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
    Assert.assertEquals(lowerBound, content[0]);
    Assert.assertEquals(upperBound - 1, content[content.length - 1]);
  }

  /*
   * Test if subsetting is forbidden if maxbound < minbound
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void subSetCrossingBoundsTest() {
    int lowerBound = MAX_ITEMS_TO_INSERT1;
    int upperBound = 0;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
  }

  @Test
  public void subSetEmptyTest1() {
    int lowerBound = MAX_ITEMS_TO_INSERT1 - 1;
    int upperBound = MAX_ITEMS_TO_INSERT1 - 1;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
    Assert.assertEquals(content.length, 0);
  }

  @Test
  public void subSetEmptyTest2() {
    int lowerBound = 50;
    int upperBound = 50;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
    Assert.assertEquals(content.length, 0);
  }

  /*
   * Test if subset is empty if minbound == maxbound
   */
  @Test
  public void subSetEqualBoundsTest() {
    int lowerBound = MAX_ITEMS_TO_INSERT1 / 2;
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
    Assert.assertEquals(content.length, 0);
  }

  @Test
  public void subSetIdentityTest() {
    int lowerBound = 0;
    int upperBound = MAX_ITEMS_TO_INSERT1 - 1;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
    Object[] content = subSet.toArray();
    // subset excludes the toElement bound, which causes
    // you can not create an identically set but one which last
    // element is removed
    Assert.assertEquals(content.length, mySet1.toArray().length - 1);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void subSetIdentityTest2() {
    int lowerBound = 0;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, null);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetMaxBoundOutOfBoundsTest() {
    int lowerBound = 0;
    int upperBound = MAX_ITEMS_TO_INSERT1 + 100;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void subSetMaxBoundsNullpointerTest() {
    int lowerBound = MAX_ITEMS_TO_INSERT1 / 2;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, null);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetMinBoundOutOfBoundsTest() {
    int lowerBound = -100;
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(lowerBound, upperBound);
  }

  /*
   * Nullpointer Test
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void subSetMinBoundsNullpointerTest() {
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.subSet(null, upperBound);
  }

  public String toString() {
    return "BPlusIndexedSet Operation Test for primitive types";
  }
}
