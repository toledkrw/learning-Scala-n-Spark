package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.{col, format_number}

object AverageFriendsByAge_Toledo {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AverageFriendsByAge_Toledo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    val people_age_friends = people.select("age", "friends")


    val avgFriendsByAge = people_age_friends.groupBy("age").avg("friends")

    import org.apache.spark.sql.functions.ceil
    val avgFriendsByAge_Pretty = avgFriendsByAge.
      withColumnRenamed("avg(friends)", "friends").
      withColumn("friends", ceil(col("friends")).cast("integer")).
      orderBy(col("age"))

    avgFriendsByAge_Pretty.show()


  }
}
