package com.github.ethgraph

import etherem.EthereumGraph

import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.{SparkContext, SparkConf}

object Main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "EthGraph")

    var sparkConf = new SparkConf()
    sparkConf = sparkConf.set("spark.driver.memory", getSparkDriverMemory(args))
    sparkConf = sparkConf.set("spark.driver.memoryOverheadFactor", getSparkDriverMemoryOverheadFactor(args))

    val sc = new SparkContext("local[*]", "EthGraph", conf = sparkConf)

    var srcWallet = "A"
    var dstWallet = "F"
    if (args.length > 3) srcWallet = args(3)
    if (args.length > 4) dstWallet = args(4)

    val shortestPath = graph.shortestPath(srcWallet, dstWallet)
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

    val mostSpentVertex = graph.mostSpentVertex()
    println(s"Most spent vertex is ${mostSpentVertex._1} with total value ${mostSpentVertex._2}")

    val mostReceivedVertex = graph.mostReceivedVertex()
    println(s"Most received vertex is ${mostReceivedVertex._1} with total value ${mostReceivedVertex._2}")
    val mostSpentFeeVertex = graph.mostSpentFeeVertex()
    println(s"Most spent fee vertex is ${mostSpentFeeVertex._1} with total value ${mostSpentFeeVertex._2}")

    val leastSpentFeeVertex = graph.leastSpentFeeVertex()
    println(s"Least spent fee vertex is ${leastSpentFeeVertex._1} with total value ${leastSpentFeeVertex._2}")

    val maxVertexDegree = graph.maxVertexDegree()
    println(s"Max vertex ${maxVertexDegree._1} degree value ${maxVertexDegree._2}")

    val maxVertexInDegree = graph.maxVertexInDegree()
    println(s"Max vertex ${maxVertexInDegree._1} inDegree value ${maxVertexInDegree._2}")

    val maxVertexOutDegree = graph.maxVertexOutDegree()
    println(s"Max vertex ${maxVertexOutDegree._1} outDegree value ${maxVertexOutDegree._2}")

    sc.stop()
  }
}