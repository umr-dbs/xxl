package xxl.core.indexStructures.indexBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.IndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;
import xxl.core.util.FileUtils;

public class IndexedSetExample {

  /**
   * @param args
   */
  public static void main(String[] args) {
    /*
     * The java.util.SortedSet interface acts like a regular java.util.Set with the exception that
     * all elements inside the set will be sorted automatically. XXL provides an implementation of
     * this interface which uses a BPlus tree index to sort the elements. Here are now two examples
     * how to create a XXL sorted set for primitive types. One special feature is that the data is
     * stored in the computers main memory and swapped out to disk if the main memory buffers limit
     * is exceeded and vice versa if data is required which was swapped before. With this the size
     * of a XXL sorted set is only limited by disk space contrary to standard java sets which
     * capacity is limited by the main memory.
     * 
     * The following example shows how to create a XXL sorted set for Integer types and add some
     * data to it. The entire content is then printed to system standard out. Please note, that this
     * example make no use of the persistent storage ability and the set size is limited by main
     * memory.
     */
    IndexedSet set =
        Builders.createBPlusTree.Integer("Table").getBuilder().create();

    List<Integer> ints = new ArrayList<>();
    for (int i = 0; i < 100_000; ++i)
      ints.add(i);

    set.addAll(ints);
    System.out.println(set); // [0, 1, 2, 3, ..., 99998, 99999]

    /*
     * Suppose you want to manage more data than your computer's main memory can contain. The XXL
     * sorted set offers disk swapping. To enable this feature you just have to set this up while
     * creating the set with the builder.
     * 
     * The following example shows how to use persistent storage. For this a temporary directory is
     * created with the XXL built-in FileUtils tool. Inside this directory there will be a sub
     * directory created which is equal to the sets table name ("Table"). This directory contains
     * the sets swap files.
     */
    try {
      final String tempPath = FileUtils.mkTempDir();

      IndexedSet diskSet =
          Builders.createBPlusTree.Integer("Table").storeAt(tempPath)
              .getBuilder().create();

      diskSet.addAll(ints);
      System.out.println(diskSet); // [0, 1, 2, 3, ..., 99998, 99999]



      /*
       * The previous example used the swapping ability to overcome the size limitation of memory.
       * Of course you can also save the entire set to disk and reload it later.
       * 
       * The following method call stores the content to disk:
       */

      diskSet.save();

      /*
       * Please note, that it is not guaranteed that each data is swapped to disk if you don't call
       * the save()-method. Only if there is not enough space inside the buffers the set will
       * automatically perform a disk write. By default the set uses a LRU-Buffer for 20 Items with
       * a block size of 4KB. Consequently most data is written to disk by the set itself. But if
       * you want to ensure to storage each data set you have to call the save()-method to flush the
       * buffers.
       * 
       * Although you can program against the interface SortedSet instead of IndexedSet you should
       * note that the save()-method is an extension in IndexedSet and will not be visible for
       * SortedSet types only.
       * 
       * To reload a set from disk it is recommended to use the loading functionality of the
       * built-in builder class. The following example shows how to reload the set of the previous
       * example where tempPath is the
       */
      IndexedSet loadedSet =
          Builders.loadBPlusTree.from(tempPath, "Table").getBuilder().create();
      System.out.println(loadedSet);

      /*
       * Let's have a look how to query some data inside the set. Because XXL uses a BPlus index to
       * sort the data you can perform a exact match or a range query. Both are wrapped by the
       * IndexedSet class.
       * 
       * The following example shows how to check if a given Integer is inside the set
       */
      boolean resultInside = diskSet.contains(23);
      boolean resultOutside = diskSet.contains(-23);

      /*
       * As suspected resultInside will be true whereas resultOutside will be false.
       * 
       * If you want to query for an interval of elements you can call the subSet, headSet or
       * tailSet method which returns an instance of FixedQuery class. This class also implements
       * the SortedSet interface but is read-only. That means you can neither add nor remove data
       * from a sub set. But if the parent set changes the content of the sub set will also be
       * changed.
       */
      SortedSet<Integer> query = diskSet.subSet(23, 42);
      for (Integer i : query)
        System.out.println(i);

      /*
       * Please ensure that you upper and lower bound is inside the set. You can check this with
       * SortedSet#contains or SortedSet#first/SortedSet#last. If you call a sub setting method whit
       * bounds outside the set this will lead to an IndexOutOfBoundsException.
       * 
       * If you want to refine the query (e.g. to perform a nested range query) you can call the sub
       * setting methods of "query" again. The following example shows how to perform a range query
       * from 30 to less than 37.
       */
      SortedSet<Integer> refineQuery = query.subSet(30, 37);
      for (Integer i : refineQuery)
        System.out.println(i);

      /*
       * To perform a exact match query you can set the lower and upper bound to hit the element.
       * Depending on the kind of sub set method you call the upper bound will be inclusive or
       * exclusive. The following example shows how to hit element 33. sub set methods according to
       */
      SortedSet<Integer> exactQuery = refineQuery.subSet(33, 34);
      for (Integer i : exactQuery)
        System.out.println(i);

      /*
       * Please note, that if the parent set changes the content the sub set will may be differ. The
       * following example demonstrate this behavior.
       */
      IndexedSet even =
          Builders.createBPlusTree.Integer("Even").getBuilder().create();
      IndexedSet odd =
          Builders.createBPlusTree.Integer("Odd").getBuilder().create();

      for (int i = 0; i < 100; ++i) {
        if (i % 2 == 0)
          even.add(i);
        else
          odd.add(i);
      }

      // Create a sub set between 20 and 40 will contain only even numbers 20, 22, ..., 38.
      SortedSet<Integer> firstQuery = even.subSet(20, 40);

      // Now merge even and odd values to the even set.
      even.addAll(odd);

      // The query "firstQuery" now contains all numbers from 20 up to 40 although the query itself
      // doesn't change

      /*
       * Let's take a look at intersection of two given sets. The sorted set "even" contains
       * currently the List [0,1,...,99] because we merged it with the "odd" set. Now let "oddMore"
       * be a sorted set with a range of odd values from 51 up to 149. To intersect "even" and
       * "oddMore" just use the Java SortedSet#retainAll(Collection). The following example
       * demonstrate how to do this. The intersection between the given sets is stored in "even" and
       * is [51, 53,..., 99].
       */
      IndexedSet oddMore =
          Builders.createBPlusTree.Integer("OddMore").getBuilder().create();
      for (int i = 51; i < 151; i += 2)
        oddMore.add(i);

      even.retainAll(oddMore);

      /*
       * In addition to use primitive types like above there is the possibility to manage relational
       * data. A relation is more or less a table with a name and a (ordered) set of columns. Each
       * column has it's own data type. The entire description of the table's columns is called the
       * schema. The data of a relation is a collection of entries called tuples. A single tuple is
       * an array of objects which size is equal to the column count of the relation and which data
       * type for each component matches to the schema.
       * 
       * With the built-in XXL builder patterns it is easy to switch from a primitive data managing
       * sorted set to a relational managing sorted set. The following example shows how to create a
       * table "Students" which stores the matriculation number as primary key for a set of
       * students. For each students there is also the name stored next to the primary key. To
       * create a schema for this table we use the ready-to-use Schemas builder. To ensure
       * compatibility with JDBC, the string type for the students name is with a fixed length.
       */
      IndexedSet students =
          Builders.createBPlusTree
              .Tuples(
                  Schemas.createSchema("Students").addInteger("MatNr")
                      .addVarChar("Name", 200)).getBuilder().create();

      /*
       * Similar to the previous approach you can perform sub setting, intersection etc. Adding data
       * to this kind of SortedSet it is recommended to use the XXL built-in Entry class. Let's add
       * some students to out table. This is always done similar to this
       */

      students.add(new Entry(11223344, "Doe"));
      // ...

      /*
       * tuple is set to the first column index. In the example above "MatNr" will be the primary
       * key which identifies uniquely an tuple inside the table. It is also possible to use
       * compound keys. Suppose you have another set, let's say "Books", in which each item is
       * identified by a unique "Book_ID". If you want to model an M:N relationship between
       * "Students" and "Books" you can create another set "HasBorrowed" with a compound key {
       * FK_MatNr, FK_Book_ID } which refer to the items in the Students and Book table.
       * 
       * The following example shows this. Please not that this construction does not implement the
       * need that the foreign keys FK_Matr, FK_Book_ID really exists in Students or Books.
       */
      IndexedSet books =
          Builders.createBPlusTree
              .Tuples(
                  Schemas.createSchema("Books").addInteger("Book_ID")
                      .addVarChar("Name", 50)).getBuilder().create();
      // add some data to books set
      books.add(new Entry(17, "Under the dome"));
      // ...

      IndexedSet hasBorrowed =
          Builders.createBPlusTree
              .Tuples(
                  Schemas.createSchema("HasBorrowed").addInteger("FK_MatrNr")
                      .addInteger("FK_Book_ID").addVarChar("Info", 200))
              .setCompoundKey(new int[] {1, 2}).getBuilder().create();

      // add some relations to hasBorrowed set
      hasBorrowed.add(new Entry(11223344, 17, "Yesterday"));
      // ...

      /*
       * If you are looking for details about the loan of book 17 by student 11223344 you can
       * perform a exact query. To handle the key subset of each entry you can use the Entry.WithKey
       * class which is an array of comparable objects. XXL will automatically match the values of
       * Entry.WithKey against the table schema. Please note that the first column has the
       * index 1, the second the index 2 and so on.
       */
      SortedSet details =
          hasBorrowed.subSet(new Entry.WithKey(11223344, 17),
              new Entry.WithKey(11223344, 18));
      String information = (String) details.first();

      /*
       * You can also perform a range query to find each book which is borrowed by student 11223345.
       */
      SortedSet allBooksOfStudent =
          hasBorrowed.subSet(new Entry.WithKey(11223344), new Entry.WithKey(
              11223345));

      /*
       * For refine the query above to match the books between book number 23 and 43 (exclusive) if
       * any.
       */
      SortedSet allBooksOfStudentFiner =
          hasBorrowed.subSet(new Entry.WithKey(11223344, 23),
              new Entry.WithKey(11223345, 43));
      
      /*
       * Please note that in case of a compound key the order of the key indices count. By setting the
       * key to {1,2} XXL first compares the column 1 and if two tuples are equal in column 1 they
       * are compared with their second column. Whereas the key {2,1} forces XXL to compare the
       * second column before the first column.  
       * 
       * Relational sets can also be stored and reloaded with the same procedure as above.
       */


    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
