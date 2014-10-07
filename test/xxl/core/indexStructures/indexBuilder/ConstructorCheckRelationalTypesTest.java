package xxl.core.indexStructures.indexBuilder;

import java.sql.SQLException;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;

public class ConstructorCheckRelationalTypesTest {

  final String MY_COLUMN_1 = "column1";
  final String MY_TABLE_NAME = "table";

  @Test
  public void addBigInt() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addBigInt(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addBit() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addBit(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addBoolean() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addBoolean(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addChar() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addChar(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addDate() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addDate(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addDouble() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addDouble(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addFloat() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addFloat(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addInteger() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addInteger(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addLongNVarChar() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addLongNVarChar(MY_COLUMN_1,
                    100)).getBuilder().create();

  }

  @Test
  public void addNChar() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addNChar(MY_COLUMN_1, 100))
            .getBuilder().create();

  }

  @Test
  public void addNVarChar() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addNVarChar(MY_COLUMN_1, 100))
            .getBuilder().create();

  }

  @Test
  public void addReal() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addReal(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addSmallInt() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addSmallInt(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addTime() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addTime(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addTimestamp() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addTimestamp(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addTinyInt() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addTinyInt(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test
  public void addVarChar() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addVarChar(MY_COLUMN_1, 100))
            .getBuilder().create();

  }

  public String toString() {
    return "Builder checks column type implementation (relational, supported)";
  }
}
