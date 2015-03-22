olap4cloud is OLAP engine working on top of Hadoop/HBase/MapReduce.

It's constructed to serve queries with grouping and aggregations like 'select sum(m1), min(m2) from table where d1 in (1,2) and d2 in (3,4) group by d3, d4' against large data sets (tens and hundreds of billions rows). olap4cloud intensively uses Map/Reduce approach and designed to be horizontally linearly scalable.

olap4cloud is supposed to work faster then competitors(Hive, Pig) because of the following features:

* data defragmentation
* intensive indexing
* preaggregations
