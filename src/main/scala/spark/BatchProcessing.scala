/**
 * Author              : Marina Olina
 * Author email        : marina.olina@inbox.lv
 * Object Name         : BatchProcessing
 * Script Creation Date: 20.06.2021
 * Description         : Designed to read json files and persist locally
 */


package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.types.{IntegerType}
import org.apache.spark.sql.expressions.Window


object BatchProcessing {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .config("spark.master", "local[2]")
      .appName("BatchProcessingETL")
      .getOrCreate()

    // Assume argument passed from oozie
//    val currentDay = args(0)
    val currentDay = "2021-06-20"
    runProcessingHomeWork(spark, currentDay)

    // To show what is saved
    spark.table("t_further_analytics").show(3, truncate = false)

    //Extracts most viewed video per category id and per trending date
    //I haven't got where to put the extracted result, so added as column and shown to std
    spark.sql("select category_id, video_id as most_viewed_per_cat, views from " +
      "(select category_id, views, video_id, ROW_NUMBER() OVER(PARTITION BY category_id ORDER BY views desc)" +
      " as rn from parquet.`further_analytics` ) as a where rn = 1 order by category_ID").show(truncate = false)

    spark.sql("select trending_date, video_id as most_viewed_per_trend, views from " +
      "(select trending_date, views, video_id, ROW_NUMBER() OVER(PARTITION BY trending_date ORDER BY views desc)" +
      " as rn from parquet.`further_analytics` ) as a where rn = 1 order by trending_date").show(truncate = false)

    spark.stop()
  }

  private def runProcessingHomeWork(spark: SparkSession, currentDay: String): Unit = {
    // Reads and parses Youtube trending video data from provided JSON file
    val df = spark.read.json(s"data_in/${currentDay}/USvideos/")

    // Formats data to have required columns for analytics
    // - video_id, trending_date, category_id, title, views, likes, dislikes
    val dfAnalytics = df.select(
        "video_id",
      "trending_date",
        "category_id",
        "title",
        "views",
        "likes",
        "dislikes")
      .withColumn("trending_date",to_date(col("trending_date"),"yy.dd.MM"))
      .withColumn("views", col("views").cast(IntegerType))
      .withColumn("likes", col("likes").cast(IntegerType))
      .withColumn("dislikes", col("dislikes").cast(IntegerType))
      .cache()  // may be repartition as well if skewed data, maybe with salted column

    // Extracts most viewed video per category id ...
      val dfCategory = dfAnalytics
        .select("video_id", "views", "category_id")
        .withColumn("most_per_category",
          max(col("views"))
            .over(Window.partitionBy("category_id")))
        .filter(col("views") === col("most_per_category"))
        .drop("most_per_category", "views")
        .withColumnRenamed("video_id", "most_per_category")

      // ... and per trending date
      val dfTrending = dfAnalytics
        .select("video_id", "views", "trending_date")
        .withColumn("most_per_trending",
          max(col("views"))
            .over(Window.partitionBy("trending_date")))
        .filter(col("views") === col("most_per_trending"))
        .drop("most_per_trending", "views")
        .withColumnRenamed("video_id", "most_per_trending")

      // Adding most viewed back to table
      // Select for proper sequence of columns
      //Save data to partitioned table to be used in further analysis
      dfAnalytics
        .join(dfCategory.hint("broadcast"), usingColumn = "category_id")
        .join(dfTrending.hint("broadcast"), usingColumn = "trending_date")
        .select(
            "video_id",
            "trending_date",
            "category_id",
            "title",
            "views",
            "likes",
            "dislikes",
            "most_per_category",
            "most_per_trending"
        )
        .coalesce(1) // for this example for few GB table must be 100-200
        .write
        .partitionBy("category_id")
        .option("path", "further_analytics")
        .mode(SaveMode.Append)   // depends on table options might be overwrite
        .format("parquet")
        .saveAsTable("t_further_analytics")
  }
}
