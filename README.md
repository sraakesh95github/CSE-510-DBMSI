# CSE-510-DBMSI
## A java project on the "minibase" project for handling bigtable databases and query processing on the bigtables

1 Goal
The version of the MiniBase I have distributed to you implements various modules of a relational database management
system. Our goal this semester is to use these modules of MiniBase as building blocks for implementing a Bigtable-like
DBMS.
2 Project Description
The following is a list of tasks that you need to perform for this final phase of the project:
• Implement a command-line program getCounts. Given the commandline invocation
getCounts NUMBUF
the program will return the numbers of maps, ditinct row labels, and distinct column labels. If these were not already
computed during the batch insertion, then at most NUMBUF many buffers need to be used to compute them.
• Modify the bigDB class such that the constructor does not take type as input. In other words, in the same bigDB
database there may be data stored according to different storage types.
• Modify the bigT class such that the bigt method does not take type as input. In other words, in the same bigT
table there may be maps stored according to different storage types.
• Modify the storage type definitions as follows:
– Type 1: No index
– Type 2:
∗ one btree to index row labels
∗ maps are row label sorted (in increasing order)
– Type 3:
∗ one btree to index column labels
∗ maps are column label sorted (in increasing order)
– Type 4:
∗ one btree to index column label and row label (combined key) and
1∗ maps are sorted on combined key (in increasing order)
– Type 5:
∗ one btree to index row label and value (combined key) and
∗ maps are sorted on combined key (in increasing order)
• Modify the batchInsert program from the previous phase as follows: Given the command line invocation
batchinsert DATAFILENAME TYPE BIGTABLENAME
where DATAFILENAME and BIGTABLENAME are strings and TYPE is an integer (between 1 and 5), the program
will store all the maps in a bigtable. The name of the bigtable that will be created in the database will be
BIGTABLENAME. If this bigtable already exists in the database, the tuples will be (logically) inserted into the
existing bigtable.
Note that
– in the same bigT table, different maps may be stored according to different storage types, based on the batch
in which they were inserted;
– the insertMap method ensures that there are at most three maps with the same row and column labels, but
different timestamps, in the entire bigtable – irrespective of which batch they were inserted in and which
storage type they are subjected to;
– the openStream methods creates a unified stream by pulling maps (potentially stored using different storage
types) and ordering them according to the orderType parameter.
As before, at the end of the batch insertion process, the program should also output the number of disk pages that
were read and written (separately).
• Implement a mapInsert program, which given the command line invocation
mapinsert RL CL VAL TS TYPE BIGTABLENAME
where BIGTABLENAME are strings and TYPE is an integer (between 1 and 5), the program will store the map woth
row label RL, column label CL, value VAL, and timestamp TS in a bigtable. The name of the bigtable that will be
created in the database will be BIGTABLENAME. If this bigtable already exists in the database, the tuples will
be (logically) inserted into the existing bigtable.
• Implement a program query. Given the command line invocation
query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
the program will access the database and printout the matching maps in the requested order.
Each filter canbe
– “*”: meaning unspecified (need to be returned),
– a single value, or
2– a range specified by two column seperated values in square brackets (e.g. “[56, 78]”)
Minibase will use at most NUMBUF buffer pages to run the query (see the Class BufMgr).
At the end of the query, the program should also output the number of disk pages that were read and written
(separately).
• Implement a BigT join operator
– RowJoin(int amt of mem, Stream leftStream, java.lang.String RightBigTName, attrString ColumnName)
amt_of_mem - IN PAGES
leftStream - a stream for the left data source
RightBigTName - name of the BigTable at the right side of the join
ColumnName - condition to match the column labels
The output is a stream of maps corresponding to a BigT consisting of the maps of the matching rows based on
the given conditions, such that
∗ Two rows match only if they have the same column and the most recent values for the two columns are
the same.
∗ The resulting rowlabel is the concatenation of the two input rowlabels, seperated with a “:”
∗ The resulting row has all the columnlabels of the two input rows, except for the joined column which
occurs only once in the bigtable – and only with the most recent three values.
• Implement a command-line program join. Given the command line invocation
rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF
the program will access the database to rowjoin the two bigtables and create a new type 1 big table with the given
table name. Minibase will use at most NUMBUF buffer pages to run the query (see the Class BufMgr).
At the end of the query, the program should also output the number of disk pages that were read and written.
• Implement an external rowSort operator that results in a type 1 BigT in which the rows are sorted according to
the most recent values for the given column label:
rowSort( Stream inStream,
%RowOrder sort_order,
java.lang.String ColumnName,
int n_pages)
Here RowOrder is similar to TupleOrder of minibase.
Like its Minibase counterpart, Sort needs to support a getNext() method that returns maps of the resulting
rows in the specified order.
3• Implement a command-line program rowsort. Given the command line invocation
rowsort INBTNAME OUTBTNAME ROWORDER COLUMNNAME NUMBUF
the program will sort the big table. Minibase will use at most NUMBUF buffer pages to run the query (see the Class
BufMgr).
At the end of the query, the program should also output the number of disk pages that were read and written.
IMPORTANT: If you need to process large amounts of data (for example to sort a file), do not use the memory. Do
everything on the disk using the tools and methods provided by minibase.
