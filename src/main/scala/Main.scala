package com.github.ethgraph

import etherem.EthereumGraph

import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.{SparkContext, SparkConf}

object Main {
  def main(args: Array[String]): Unit = {
    println("Command line params: " + args.mkString("Array(", ", ", ")"))

    var sparkConf = new SparkConf()
    sparkConf = sparkConf.set("spark.driver.memory", getSparkDriverMemory(args))
    sparkConf = sparkConf.set("spark.driver.memoryOverheadFactor", getSparkDriverMemoryOverheadFactor(args))

    val sc = new SparkContext("local[*]", "EthGraph", conf = sparkConf)

    // "file:///home/burdeinyi_v_o_gmail_com/wallets.tsv"
    // "file:///home/burdeinyi_v_o_gmail_com/transactions.tsv"
    val graph = new EthereumGraph(sc, getWalletsFile(args), getTransactionsFile(args))

    val srcWallet = getSrcWallet(args)
    val dstWallet = getDstWallet(args)
//    val shortestPath = graph.shortestPath(srcWallet, dstWallet)
//    println("Shortest path value: " + shortestPath._1.toString)
//    println("Shortest path hops: " + shortestPath._2.toString)

    val tolerance = getPageRankTolerance(args)
    val topRankDynamic = graph.getTopDynamicRank(tolerance, 10, ascending = false)
    println("Top rank dynamic: ")
    topRankDynamic.foreach(println)

    val staticIterCnt = getPageRankIterations(args)
    val topTankStatic = graph.getTopStaticRank(staticIterCnt, 10, ascending = false)
    println("Top rank static: ")
    topTankStatic.foreach(println)

    val triangleCounts = graph.countTriangles()
    println("Triangles count for 5 subsequent subgraph: " + triangleCounts.toString)

    val connectives = graph.connectives()
    println("Connectives: " + connectives.toString())

    val propagationIterCnt = getLabelPropagationIterations(args)
    val labelPropagationResult = graph.labelPropagation(propagationIterCnt)
    println("Storing labels propagation to file `propagations.tsv`")
    val propagationWriter = new BufferedWriter(new FileWriter(getLabelPropagationOutputFileIterations(args)))
    labelPropagationResult.foreach(propagationWriter.write)
    propagationWriter.close()

//    val diameter = graph.calculateGraphDiameter()
//    println(s"Diameter: ${diameter._1}. From ${diameter._2} to ${diameter._3}")

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

  private def getWalletsFile(args: Array[String]): String = {
    val value = findArgument("--wallets", args)
    if (value.isEmpty) "wallets.tsv" else value
  }

  private def getTransactionsFile(args: Array[String]): String = {
    val value = findArgument("--transactions", args)
    if (value.isEmpty) "transactions.tsv" else value
  }

  private def getSparkDriverMemory(args: Array[String]): String = {
    val value = findArgument("--driver-mem", args)
    if (value.isEmpty) "25g" else value
  }

  private def getSparkDriverMemoryOverheadFactor(args: Array[String]): String = {
    val value = findArgument("--mem-overhead-fact", args)
    if (value.isEmpty) "0.6" else value
  }

  private def getSrcWallet(args: Array[String]): String = {
    val value = findArgument("--src-wallet", args)
    if (value.isEmpty) "" else value
  }

  private def getDstWallet(args: Array[String]): String = {
    val value = findArgument("--dst-wallet", args)
    if (value.isEmpty) "" else value
  }

  private def getPageRankTolerance(args: Array[String]): Double = {
    val value = findArgument("--pr-tolerance", args)
    if (value.isEmpty) 0.001 else value.toDouble
  }

  private def getPageRankIterations(args: Array[String]): Int = {
    val value = findArgument("--pr-iterations", args)
    if (value.isEmpty) 15 else value.toInt
  }

  private def getLabelPropagationIterations(args: Array[String]): Int = {
    val value = findArgument("--lp-iterations", args)
    if (value.isEmpty) 7 else value.toInt
  }

  private def getLabelPropagationOutputFileIterations(args: Array[String]): String = {
    val value = findArgument("--lp-output-file", args)
    if (value.isEmpty) "labelPropagation.tsv" else value
  }

  private def findArgument(flag: String, args: Array[String]): String = {
    var returnValue = ""
    args.foreach(item => {
        if (item.startsWith(flag)) {
          val words = item.split("=")
          if (words.length == 2) returnValue = words(1)
        }
      })
    returnValue
  }
}