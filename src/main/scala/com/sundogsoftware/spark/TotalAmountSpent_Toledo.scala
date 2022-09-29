package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalAmountSpent_Toledo {

  def parseLine(line:String): (Int, Float) ={
    val fields = line.split(',')

    val costumerId = fields(0).toInt
    val purchasePrice = fields(2).toFloat

    (costumerId, purchasePrice)
  }

  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomer_Toledo")

    val lines = sc.textFile("data/customer-orders.csv")

    val ds = lines.map(parseLine)

    val totalSpentByCustomer = ds.reduceByKey((x,y) => x+y)

    val totalSpentByCustomer_sortedByCostumerID = totalSpentByCustomer.sortByKey().collect()

    val ranking_costumersThatSpentTheMost = totalSpentByCustomer.map( (x) => (x._2, x._1)).sortByKey(false).collect()

    println("Sorted by Costumer ID:")
    for(costumer <- totalSpentByCustomer_sortedByCostumerID){
      println(f"Costumer id: ${costumer._1}%02d has spent in total: $$${costumer._2}%.2f ")
    }

    println("\nSorted by Total Amount Spent:")
    for (costumer <- ranking_costumersThatSpentTheMost){
      println(f"Total Spent: $$${costumer._1}%.2f by Costumer id: ${costumer._2}%02d")
    }

  }
}
