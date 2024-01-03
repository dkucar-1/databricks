// Databricks notebook source
def compareSchemas(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame, noNullable: Boolean = true, noMetadata: Boolean = true): Boolean = {
   /* Spark schemas actually contain 4 properties:
   * name, type, nullable and metadata. 
   */
  
  import org.apache.spark.sql.types.{StructType, Metadata}

  val schema1 = df1.schema
  val schema2 = df2.schema
  
  // set Metadata to be empty
  if (noMetadata) 
    StructType(schema1.map(_.copy(metadata = Metadata.empty))) == StructType(schema2.map(_.copy(metadata = Metadata.empty))) 
  else schema1 == schema2
 
  // set nullables to be true all around
  if (noNullable) {
    StructType(schema1.map(_.copy(nullable = true))) == StructType(schema2.map(_.copy(nullable = true)))
  } else schema1 == schema2
}

// COMMAND ----------

def compareDFs(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
  // USAGE: compareDFs(dataFrame1, dataFrame2)
  // returns: Boolean value
  
  val df1Cnt = df1.count
  val df2Cnt = df2.count
  
  // compare schema
  if (compareSchemas(df1, df2) == false) {
    println("schemas are different")
    false
  }
  
  else if (df1Cnt == 0 | df2Cnt == 0) {
    println(s"row count 1 is ${df1Cnt}; row count 2 is ${df2Cnt}")
    df1Cnt == df2Cnt
  }

  // compare row counts
  else if (df1Cnt != df2Cnt) {
    println("row counts are different")
    false
  }

  else {
    // reverse order of index and contents 
    val rdd1 = df1.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))
    val rdd2 = df2.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))

    //true
    /* reduce by key on the index i.e. r._1
       grab only the value (a boolean)
       then we have a list of booleans 
       ensure all booleans are true */
 
       rdd1.union(rdd2)
           .reduceByKey(_ == _)
           .map(t => t._2.asInstanceOf[Boolean])
           .reduce(_ && _)

  
  }
}

// COMMAND ----------

// this is the fastest
def compareDFs_fast(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
  // USAGE: compareDFs(dataFrame1, dataFrame2)
  // returns: Boolean value
  
  // check to see if schemas are equal using above 
  // else 
  if (compareSchemas(df1, df2) == false) {
    println("schemas are different")
    false
  }
  // step 3: compare row counts
  else if (df1.count != df2.count) {
    println("row counts are different")
    false
  }

  // verify that the symmetric difference between df1 and df2 is empty
  else
    ((df2.exceptAll(df1)).isEmpty) && ((df1.exceptAll(df2)).isEmpty)
}
