package xxl.core.indexStructures.indexBuilder;

import java.sql.SQLException;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;

public class ConstructorRelationalTableColumnsTest {

  final String MY_COLUMN_1 = "column1";
  final String MY_COLUMN_2 = "column1";
  final String MY_COLUMN_CORRECT_1 = "column1";

  final String MY_COLUMN_CORRECT_2 = "column2";
  final String MY_TABLE_NAME = "table";

  @Test
  public void createTableWithCorrectColumnNames() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_NAME)
                    .addInteger(MY_COLUMN_CORRECT_1)
                    .addInteger(MY_COLUMN_CORRECT_2)).getBuilder().create();

  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createTableWithDoubleColumnNames() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_NAME).addInteger(MY_COLUMN_1)
                    .addInteger(MY_COLUMN_2)).getBuilder().create();

  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void createTableWithoutColumns() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree.Tuples(Schemas.createSchema(MY_TABLE_NAME))
            .getBuilder().create();

  }

  public String toString() {
    return "Builder checks column table configuration";
  }
}
