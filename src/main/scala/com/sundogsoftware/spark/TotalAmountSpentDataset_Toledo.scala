package com.sundogsoftware.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, FloatType}

import org.apache.log4j._

object TotalAmountSpentDataset_Toledo {

  case class Order(costumerId:String, productId:String, orderPrice:Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("TotalAmountSpentDataset_Toledo")
      .master("local[*]")
      .getOrCreate()

    val costumer_data_schema = new StructType()
      .add("costumerId",StringType)
      .add("productId",StringType)
      .add("orderPrice",FloatType)


    import spark.implicits._
    val costumer_data = spark
      .read
      .schema(costumer_data_schema)
      .csv("data/customer-orders.csv")
      .as[Order]

    val totalAmountPerCostumer = costumer_data
      .select("costumerId", "orderPrice")
      .groupBy("costumerId")
      .agg(
        round(
          sum("orderPrice"),
          2
        ).
        alias("totalAmount")
      )
      .withColumn("costumerId", col("costumerId").cast("integer"))
      .orderBy(col("costumerId").asc)


    val rankedTotalAmount = totalAmountPerCostumer.orderBy(col("totalAmount").desc)

    totalAmountPerCostumer.show()
    println("")
    rankedTotalAmount.show()

    spark.stop()
  }

}
