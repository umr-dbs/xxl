# XXL
The eXtensible and fleXible Library XXL is a Java library that contains a rich infrastructure for implementing advanced query processing functionality.
The library offers low-level components like access to raw disks as well as high-level ones like a query optimizer.
On the intermediate levels, XXL provides a demand-driven cursor algebra, a framework for indexing and a powerful package for supporting aggregation.
The XXL project provides various packages.
See the longer introduction to XXL for an explanation of the packages.


## Main Features
+ A demand-driven cursor algebra including efficient implementations of object-relational operators such as joins, difference, MergeSort etc.
+ An extended relational algebra based on java.sql.ResultSet
+ A powerful framework of index-structures, e.g. B+tree, R-tree (linear and quadratic split Guttman et al), R\*tree, RR\*tree, 
Hilbert R-tree, R-tree (linear split Tan et al), X-tree, M-tree etc.
+ A framework for processing multi-way joins including spatial, temporal, and similarity joins
+ The support of raw-I/O (using JNI), an own file system implementation, and a record manager
+ Skyline query R-tree extension
+ MVBT (Multiversion B+Tree)-Index Implementation
+ MVBT+ bulk-loading approach
+ Sort-based bulk-loading methods for R-trees (including STR and GOPT approach)
+ Top-down buffer tree bulk-loading of R-trees


## Building
You can add the XXL library into your local maven repository by calling `mvn install -DskipTests`.

If you just want to generate jar files you can execute `mvn package`.
You should find the output files in the *target* directory.

In order to just generate JavaDoc you can execute `mvn javadoc:javadoc`.
The *target/apidocs* directory should now contain the JavaDoc HTML pages.

To execute the tests you can call `mvn test`.
Now maven will run all testNG testcases for XXL.


## Using
All classes of XXL are documented in detail, containing examples in the JavaDoc.
The latest version of the documentation is [here](http://umr-dbs.github.io/xxl/docs/).
You can also look at the test cases how the different data structures were used there.
Additionally, you can find use cases separately in [xxl-usecases](http://github.com/umr-dbs/xxl-usecases) that simplify their understanding.


## Papers
* Michael Cammert, Christoph Heinz, Jürgen Krämer, Martin Schneider, Bernhard Seeger: <br />
[**A Status Report on XXL - a Software Infrastructure for Efficient Query Processing.**](http://dbs.mathematik.uni-marburg.de/publications/myPapers/2003/CHKSS03.pdf) IEEE Data Eng. Bull. 26(2): 12-18 (2003)

* Jochen Van den Bercken, Björn Blohsfeld, Jens-Peter Dittrich, Jürgen Krämer, Tobias Schäfer, Martin Schneider, Bernhard Seeger: <br />
[**XXL - A Library Approach to Supporting Efficient Implementations of Advanced Database Queries.**](http://www.vldb.org/conf/2001/P039.pdf) VLDB 2001: 39-48

* Jochen Van den Bercken, Jens-Peter Dittrich, Bernhard Seeger: <br />
[**javax.XXL: A prototype for a Library of Query processing Algorithms.**](http://dl.acm.org/citation.cfm?id=336562) SIGMOD Conference 2000: 588


## Contributing to the XXL-Project
Since we consider XXL as a toolbox for the entire community, we would be pleased to get feed-back from research projects that use the functionality of XXL.
In particular, we encourage people, who have extended the functionality of XXL, to attach their code to our release in a supplement package.


## License
All versions of XXL are freely available under the terms of the GNU Lesser General Public License.
