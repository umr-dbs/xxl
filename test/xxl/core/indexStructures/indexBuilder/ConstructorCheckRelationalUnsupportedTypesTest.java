package xxl.core.indexStructures.indexBuilder;

import java.sql.SQLException;

import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;

public class ConstructorCheckRelationalUnsupportedTypesTest {

  final String MY_COLUMN_1 = "column1";
  final String MY_TABLE_NAME = "table";

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addArray() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addArray(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addBinary() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addBinary(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addBlob() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addBlob(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addClob() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addClob(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addDataLink() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addDataLink(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addDecimal() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addDecimal(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addDistinct() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addDistinct(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addJavaObject() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addJavaObject(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addLongVarBinary() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_COLUMN_1).addLongVarBinary(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addNClob() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addNClob(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addNull() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addNull(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addNumeric() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addNumeric(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addOther() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addOther(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addRef() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addRef(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addRowId() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addRowId(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addSqlXml() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addSqlXml(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addStruct() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addStruct(MY_COLUMN_1))
            .getBuilder().create();

  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void addVarBinary() throws SQLException {

    BPlusIndexedSet mySet =
        Builders.createBPlusTree
            .Tuples(Schemas.createSchema(MY_COLUMN_1).addVarBinary(MY_COLUMN_1))
            .getBuilder().create();

  }


  public String toString() {
    return "Builder checks column type implementation (relational, unsupported)";
  }
}
