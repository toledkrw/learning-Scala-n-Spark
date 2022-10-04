package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object UnpopularHero {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("UnpopularSuperHero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))


    val heroesWithMinimumConnections = connections.
      filter($"connections" === connections.agg(min("connections")).first().get(0))

    val heroesConnectionsWithNames =
      heroesWithMinimumConnections.join(names, "id")

    val unpopularHeroes = heroesConnectionsWithNames.orderBy(asc("connections"), asc("name"))

    println("Top 10 unpopular heroes: ")
    unpopularHeroes.show(10)

    val totalUnpopularHeroes =
    unpopularHeroes.count()

    println(f"The total of unpopular superheroes (with 1 or less connections) is: ${totalUnpopularHeroes}")
    spark.stop()

  }
}
