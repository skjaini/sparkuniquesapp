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
  case class StatsPixel(date: String, userId: String, blogId: Int)

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkUniquesApp <raw input files> <outout> ")
      System.err.println("Example: SparkUniquesApp /data/2014/07/01/stats-uniques* sparkuniques.out ")
      System.exit(1)
    }

    val Array(input_files, output_file) = args

    // Create basic objects to access the cluster
    val sc = new SparkContext(new SparkConf().setAppName("SparkUniques"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // Date Formatter to convert timestamp field
    val df = new SimpleDateFormat("yyyy-MM-dd")

    // Create StatsPixel RDD 
    import sqlContext.createSchemaRDD
    val statsPixels = sc.textFile(input_files).map(_.split("\t")).map(p => StatsPixel(df.format(new Date( p(0).trim.toLong*1000L )), p(1), p(2).trim.toInt))
    
    // Register the RDD as table
    statsPixels.registerAsTable("statspixels")

    // Construct the uniques query
    val query = sqlContext.sql("select blogId, count(distinct(userId)) from statspixels group by blogId")

    // Save as local file
    // query.saveAsTextFile(output_file)

    System.out.println(query.collect().foreach(println))
  }
}
