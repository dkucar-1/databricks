// Databricks notebook source
// MAGIC %run ./compare_data_structures

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

// COMMAND ----------

val shouldWorkDf1 = Seq(1,2,3).toDF
val shouldWorkDf2 = Seq(1,2,3).toDF

shouldWorkDf1.show()
shouldWorkDf2.show()

println(s"compareDFs = ${compareDFs(shouldWorkDf1, shouldWorkDf2)} // true is expected")
println(s"compareDFs_fast = ${compareDFs_fast(shouldWorkDf1, shouldWorkDf2)} // true is expected")

// COMMAND ----------

val emptyDf1 = Seq().toDF
val emptyDf2 = Seq().toDF

emptyDf1.show()
emptyDf2.show()

println(s"compareDFs = ${compareDFs(emptyDf1, emptyDf2)} // true is expected")
println(s"compareDFs_fast = ${compareDFs_fast(emptyDf1, emptyDf2)} // true is expected")

// COMMAND ----------

val df1 = spark.sparkContext.parallelize(Range(1, 10000000)).toDF()
val df2 = spark.sparkContext.parallelize(Range(1, 10000000)).toDF()

// COMMAND ----------

// two dfs with the same data, same schema should be equal
println(s"compareDFs = ${compareDFs(df1, df2)} // true is expected")

// COMMAND ----------

// two dfs with the same data, same schema should be equal
println(s"compareDFs = ${compareDFs_fast(df1, df2)} // true is expected")

// COMMAND ----------

// MAGIC %md Now mess df1 up a bit so they are not equal

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, when}
val df3 = df1.withColumn("value", when($"value" === 1, lit(78)).otherwise($"value"))

// COMMAND ----------

println(s"compareDFs = ${compareDFs(df1, df3)} // false is expected")

// COMMAND ----------

println(s"compareDFs = ${compareDFs_fast(df1, df3)} // false is expected")

// COMMAND ----------

// MAGIC %md Column with uuids.. sort of like random strings

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, col}
val dfWithUuid1 = df1.withColumn("new_uuid", expr("uuid()"))
val dfWithUuid2 = dfWithUuid1
val dfWithUuid3 = df3.withColumn("new_uuid", expr("uuid()"))
val dfWithUuid4 = dfWithUuid1.withColumn("value", col("value").cast("Long"))

// COMMAND ----------

println(s"compareDFs = ${compareDFs(dfWithUuid1,dfWithUuid2)} // true is expected")

// COMMAND ----------

println(s"compareDFs = ${compareDFs_fast(dfWithUuid1,dfWithUuid2)} // true is expected")

// COMMAND ----------

println(s"compareDFs = ${compareDFs(dfWithUuid1,dfWithUuid3)} // false is expected")

// COMMAND ----------

println(s"compareDFs = ${compareDFs_fast(dfWithUuid1,dfWithUuid3)} // false is expected")

// COMMAND ----------

// false due to schema mismatch
println(s"compareDFs = ${compareDFs(dfWithUuid1,dfWithUuid4)} // false is expected")

// COMMAND ----------

// false due to schema mismatch
println(s"compareDFs = ${compareDFs_fast(dfWithUuid1,dfWithUuid4)} // false is expected")
