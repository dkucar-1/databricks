##Comparison Utility Functions

This repo contains utility functions to compare Spark data structures such as schemas and dataframes

------
**def compareSchemas** -> Boolean
```
df1: org.apache.spark.sql.DataFrame, 
df2: org.apache.spark.sql.DataFrame, 
noNullable: Boolean = true, 
noMetadata: Boolean = true
```
   
Spark schemas actually contain 4 properties: name, type, nullable and metadata.

------
**def compareDFs** -> Boolean
```
df1: org.apache.spark.sql.DataFrame, 
df2: org.apache.spark.sql.DataFrame
```

Uses a `reduceByKey` operation to do the comparison on each worker. This requires a conversion from dataframe to rdd.  The final AND reducer is done on the driver

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html

This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce.

------

**def compareDFs_fast** -> Boolean
```
df1: org.apache.spark.sql.DataFrame, 
df2: org.apache.spark.sql.DataFrame
```

Computes the symmetric difference between dataframe 1 and dataframe 2 to see if it is empty. 


