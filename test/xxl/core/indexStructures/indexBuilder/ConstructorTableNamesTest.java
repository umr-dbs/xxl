package xxl.core.indexStructures.indexBuilder;

import java.sql.SQLException;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;

public class ConstructorTableNamesTest {

  final String MY_TABLE_NAME = "";

  /*
   * Table name for primitive managing BPlusTree is empty
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createPrimitiveWithEmptyTableName() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Integer(MY_TABLE_NAME).getBuilder().create();

  }

  /*
   * Table name for primitive managing BPlusTree is null
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createPrimitiveWithNullPointer() {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Integer(null).getBuilder().create();

  }

  /*
   * Table name for relational managing BPlusTree is empty
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createTupleWithEmptyTableName() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Tuples(Schemas.createSchema(MY_TABLE_NAME))
            .getBuilder().create();

  }

  /*
   * Table name for relational managing BPlusTree is null
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createTupleWithNullPointer() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Tuples(Schemas.createSchema(null))
            .getBuilder().create();

  }

  public String toString() {
    return "Builder constructor Test: Table names";
  }
}
