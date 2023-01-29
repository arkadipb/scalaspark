package org.example


import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.DataFrame

object NewDay {

  System.setProperty("hadoop.home.dir", "C:\\Users\\alway\\IdeaProjects\\Hadoop\\hadoop-3.2.1\\hadoop-2.7.1\\bin")


  def execute_movieRatings(moviesRenamed: DataFrame, ratingsRenamed: DataFrame): (DataFrame) = {

    val movieRatings = moviesRenamed.join(ratingsRenamed, Seq("movie_id"))
      .groupBy("movie_id", "title")
      .agg(
        min("rating").as("min_rating"),
        max("rating").as("max_rating"),
        round(avg("rating"), 3).as("avg_rating")
      )

    movieRatings.show()

    movieRatings
  }

  def execute_userRatings(moviesRenamed: DataFrame, ratingsRenamed: DataFrame): (DataFrame) = {


    val userRatings = moviesRenamed.join(ratingsRenamed, Seq("movie_id"))
      .groupBy("movie_id", "title", "genre", "user_id", "rating")
      .agg(max("timestamp").as("timestamp"))
      .withColumn("row_num", row_number.over(partitionBy("user_id").orderBy(col("rating").desc, col("timestamp"))))
      .filter(col("row_num") <= 3)

    userRatings.show()

    userRatings
  }

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
    .master("local")
    .appName("Spark")
    .getOrCreate()


    val movies = spark.read.option("path", "C:\\Users\\alway\\IdeaProjects\\data\\ml-1m\\movies.dat")
      .option("inferSchema",true)
      .option("delimiter", "::")
      .option("header", false)
      .format("csv")
      .load()

    val ratings = spark.read.option("path", "C:\\Users\\alway\\IdeaProjects\\data\\ml-1m\\ratings.dat")
      .option("delimiter", "::")
      .option("header", false)
      .format("csv")
      .load()

    val moviesRenamed = movies.withColumnRenamed("_c0", "movie_id")
      .withColumnRenamed("_c1", "title")
      .withColumnRenamed("_c2", "genre")

    val ratingsRenamed = ratings.withColumnRenamed("_c0", "user_id")
      .withColumnRenamed("_c1", "movie_id")
      .withColumnRenamed("_c2", "rating")
      .withColumnRenamed("_c3", "timestamp")

    val movieRatings = execute_movieRatings(moviesRenamed, ratingsRenamed)

    val userRatings = execute_userRatings(moviesRenamed, ratingsRenamed)

    movies.write
      .option("path","C:\\Users\\alway\\IdeaProjects\\data\\output\\movies\\")
      .option("header",true)
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("movies")

    ratings.write
      .option("path", "C:\\Users\\alway\\IdeaProjects\\data\\output\\ratings\\")
        .option("header",true)
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("ratings")

    movieRatings.write
      .option("path", "C:\\Users\\alway\\IdeaProjects\\data\\output\\movieRatings\\")
      .option("header", true)
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("movieRatings")

    userRatings.write
      .option("path", "C:\\Users\\alway\\IdeaProjects\\data\\output\\userRatings\\")
      .option("header",true)
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("userRatings")
  }

}
