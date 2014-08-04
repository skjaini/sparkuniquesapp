/**
 * App to compute Uniques using Spark.
 */

package com.wpcom.sparkuniques

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Date

object SparkUniquesApp {
  case class StatsPixel(date: String, userId: String, blogId: Long)

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkUniquesApp <source dir> <use parquet>")
      System.err.println("Example: SparkUniquesApp /data/2014/07/01 true")
      System.exit(1)
    }

    val Array(sourceDirectory, doParquet) = args

    // Create basic objects to access the cluster
    val sc = new SparkContext(new SparkConf().setAppName("SparkUniques"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // Date Formatter to convert timestamp field
    val df = new SimpleDateFormat("yyyy-MM-dd")

    // Create StatsPixel RDD 
    import sqlContext.createSchemaRDD
    val statsPixelsRDD = sc.textFile(sourceDirectory).map(_.split("\t")).map(p => StatsPixel(df.format(new Date( p(0).trim.toLong*1000L )), p(1), p(2).trim.toLong))

    val now = System.currentTimeMillis / 1000
    val tableName = "statspixels_" + now.toString

    // Convert to Parquet format and query the registered table
    if (doParquet.toBoolean) {

      // Write out an RDD as a parquet file
      statsPixelsRDD.saveAsParquetFile(tableName + ".parquet")

      // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
      val statsPixelParquetFile = sqlContext.parquetFile(tableName + ".parquet")

      //Parquet files can also be registered as tables and then used in SQL statements.
      statsPixelParquetFile.registerAsTable(tableName)

      // Construct the uniques query
      val query = sqlContext.sql("select blogId, count(distinct(userId)) from " + tableName + " group by blogId")
   
      query.saveAsTextFile(tableName)

      // System.out.println(query.collect().foreach(println))
    } else {

      // Register the RDD as table
      statsPixelsRDD.registerAsTable(tableName)

      // Construct the uniques query
      val query = sqlContext.sql("select blogId, count(distinct(userId)) from " + tableName + " group by blogId")
   
      // Save as local file
      query.saveAsTextFile(tableName)

      // System.out.println(query.collect().foreach(println))
    }

  }
}
