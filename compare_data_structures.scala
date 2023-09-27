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
     StructType(schema1.map(_.copy())) == StructType(schema2.map(_.copy(nullable = true)))
  } else schema1 == schema2nullable = true
}

// COMMAND ----------

def compareDFs(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
  // USAGE: compareDFs(dataFrame1, dataFrame2)
  // returns: Boolean value
  
  // step 1: see if one or both DFs are empty
  if (df1.head(1).isEmpty || df2.head(1).isEmpty) false
  
  // step 2: check to see if schemas are equal using above 
  else if (compareSchemas(df1, df2) == false) false
  
  // step 3: compare row counts
  else if (df1.count != df2.count) false
  
  else {
    // reverse order of index and contents 
    val rdd1 = df1.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))
    val rdd2 = df2.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))

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
  
  // step 1: see if one or both DFs are empty
  if (df1.head(1).isEmpty || df2.head(1).isEmpty) false
  
  // step 2: check to see if schemas are equal using above 
  else if (compareSchemas(df1, df2) == false) false
  
  // step 3: compare row counts
  else if (df1.count != df2.count) false
  
  // verify that the symmetric difference between df1 and df2 is empty
  else
    ((df2.exceptAll(df1)).isEmpty) && ((df1.exceptAll(df2)).isEmpty)
}

// COMMAND ----------

// MAGIC %scala
// MAGIC /** Given a full path string and a flag indicating whether to list leaf subdirectory
// MAGIC    *
// MAGIC    *  @param filePath      : path string starting with s3://
// MAGIC    *  @param subdirs   : list down to leaf subdirectory
// MAGIC    *  @return sequence of leaf files or subdirectories
// MAGIC    */
// MAGIC def recursiveList(filePath: String, subdirs: Boolean = false): Seq[String] = {
// MAGIC   val allFiles = ListBuffer.empty[String]
// MAGIC
// MAGIC   def recursiveListHelper(filePath: String, subdirs: Boolean = false): Seq[Any] = {
// MAGIC     dbutils.fs.ls(filePath).map { fi =>
// MAGIC         if (fi.isFile) allFiles.append(fi.path)
// MAGIC         else recursiveListHelper(fi.path, subdirs)
// MAGIC      }
// MAGIC    }
// MAGIC
// MAGIC   recursiveListHelper(filePath, subdirs)
// MAGIC
// MAGIC   if (!subdirs) allFiles
// MAGIC   else allFiles
// MAGIC     .map(_.split("/")
// MAGIC     .dropRight(1)
// MAGIC     .mkString("/")).distinct
// MAGIC   }
