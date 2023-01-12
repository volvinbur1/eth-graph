package com.github.ethgraph

import etherem.EthereumGraph

import java.io.{BufferedWriter, FileWriter}

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "EthGraph")

    val walletsFile = "dataset/test/wallets.tsv"
    val transactionsFile = "dataset/test/transactions.tsv"

    val graph = new EthereumGraph(sc, walletsFile, transactionsFile)
    val shortestPath = graph.shortestPath("A", "F")
    println("Shortest path value: " + shortestPath._1.toString)
    println("Shortest path hops: " + shortestPath._2.toString)

    val tolerance = 0.001
    val topRankDynamic = graph.getTopDynamicRank(tolerance, 10, ascending = false)
    println("Top rank dynamic: ")
    topRankDynamic.foreach(println)

    val staticIterCnt = 15
    val topTankStatic = graph.getTopStaticRank(staticIterCnt, 10, ascending = false)
    println("Top rank static: ")
    topTankStatic.foreach(println)

    val triangleCounts = graph.countTriangles()
    println("Triangles count for 5 subsequent subgraph: " + triangleCounts.toString)

    val connectives = graph.connectives()
    println("Connectives: " + connectives.toString())

    val propagationIterCnt = 7
    val labelPropagationResult = graph.labelPropagation(propagationIterCnt)
    println("Storing labels propagation to file `propagations.tsv`")
    val propagationWriter = new BufferedWriter(new FileWriter("propagations.tsv"))
    labelPropagationResult.foreach(propagationWriter.write)
    propagationWriter.close()

    val diameter = graph.calculateGraphDiameter()
    println(s"Diameter: ${diameter._1}. From ${diameter._2} to ${diameter._3}")
    sc.stop()
  }
}