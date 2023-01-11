package com.github.ethgraph

import etherem.EthereumGraph

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "EthGraph")

    val walletsFile = "dataset/test/wallets.tsv"
    val transactionsFile = "dataset/test/transactions.tsv"

    val graph = new EthereumGraph(sc, walletsFile, transactionsFile)
    val res = graph.shortestPath("A", "F")
    println("Short path value: " + res._1.toString)
    println("Short path hops: " + res._2.toString)
    sc.stop()
  }
}