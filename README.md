# PostgreSql

Using open source relational database management system PostgreSQL, implemented Python functions that carry out various tasks in a distributed system

## data partition

Carries out loading the input data into a relational table, partition the table using different horizontal fragmentation approaches,and insert new tuples into the right fragment

Range_Partition() that takes as input:
(1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions.
Range_Partition() then generates N horizontal fragments of the Ratings table and store them in PostgreSQL.
The algorithm partitions the ratings table based on N uniform ranges of the Rating attribute.

RoundRobin_Partition() that takes as input:
(1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions.
The function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL.
The algorithm should partition the ratings table using the round robin partitioning approach.

RoundRobin_Insert() that takes as input: 
(1) Ratings table stored in PostgreSQL, (2) UserID, (3) ItemID, (4) Rating.
RoundRobin_Insert() then inserts a new tuple to the Ratings table and the right fragment based on the round robin approach

Range_Insert() that takes as input:
(1) Ratings table stored in Post- greSQL (2) UserID, (3) ItemID, (4) Rating.
Range_Insert() then inserts a new tuple to the Ratings table and the correct fragment (of the partitioned ratings table) based upon the Rating value.


## Range and Point Query

RangeQuery()-Implements a Python function RangeQuery that takes as input:
(1) Ratings table stored in PostgreSQL, (2) RatingMinValue (3) RatingMaxValue (4) openconnection
RangeQuery() then returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or equal to RatingMaxValue.

PointQuery() – Implements a Python functionPointQuery that takes as input:
(1) Ratings table stored in PostgreSQL, (2) RatingValue. (3) openconnection
PointQuery() then returns all tuples for which the rating value is equal to RatingValue.


## Parallel Sort and Join
Implements a Python function ParallelSort() that takes as input: 
(1) InputTable stored in a PostgreSQL database, (2) SortingColumnName the name of the column used to order the tuples by.
ParallelSort() then sorts all tuples (using five parallelized threads) and stores the sorted tuples for in a table named OutputTable (the output table name is passed to the function).
The OutputTable contains all the tuple present in InputTable sorted in ascending order

Implement a Python function ParallelJoin() that takes as input:
(1) InputTable1 and InputTable2 table stored in a PostgreSQL database, (2) Table1JoinColumn and Table2JoinColumn that represent the join key in each input table respectively.
ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized threads) and stored the resulting joined tuples in a table named OutputTable (the output table name is passed to the function).
The schema of OutputTable should be similar to the schema of both InputTable1 and InputTable2 combined.
