// Databricks notebook source
// MAGIC %scala
// MAGIC /** Given a full path string and a flag indicating whether to list leaf subdirectory
// MAGIC    *
// MAGIC    *  @param filePath      : path string starting with s3://
// MAGIC    *  @param subdirs   : list down to leaf subdirectory
// MAGIC    *  @return sequence of leaf files or subdirectories
// MAGIC    */
// MAGIC def recursiveList(filePath: String, subdirs: Boolean = false): Seq[String] = {
// MAGIC   import scala.collection.mutable.ListBuffer
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
