// Databricks notebook source
// MAGIC %run ./compare_data_structures

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

// COMMAND ----------

val df1 = spark.sparkContext.parallelize(Range(1, 10000000)).toDF()
val df2 = spark.sparkContext.parallelize(Range(1, 10000000)).toDF()

// COMMAND ----------

compareDFs(df1, df2)

// COMMAND ----------

compareDFs_fast(df1, df2)

// COMMAND ----------

// MAGIC %md Now mess df1 up a bit so they are not equal

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, when}
val df3 = df1.withColumn("value", when($"value" === 1, lit(78)).otherwise($"value"))

// COMMAND ----------

compareDFs(df1,df3)

// COMMAND ----------

compareDFs_fast(df1, df3)

// COMMAND ----------

// MAGIC %md Column with uuids.. sort of like random strings

// COMMAND ----------

import org.apache.spark.sql.functions.expr
val dfWithUuid1 = df1.withColumn("new_uuid", expr("uuid()"))
val dfWithUuid2 = df2.withColumn("new_uuid", expr("uuid()"))
val dfWithUuid3 = df3.withColumn("new_uuid", expr("uuid()"))

// COMMAND ----------

compareDFs(df1,df2)

// COMMAND ----------

compareDFs_fast(df1,df2)

// COMMAND ----------

compareDFs(df1,df3)

// COMMAND ----------

compareDFs_fast(df1,df3)
