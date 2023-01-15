package com.github.ethgraph

import etherem.EthereumGraph

import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object Main {
  private val separator = "\n\n\n\n"

  def main(args: Array[String]): Unit = {
    println("Command line params: " + args.mkString("Array(", ", ", ")"))

    var sparkConf = new SparkConf()
    sparkConf = sparkConf.set("spark.driver.memory", getSparkDriverMemory(args))
    sparkConf = sparkConf.set("spark.driver.memoryOverheadFactor", getSparkDriverMemoryOverheadFactor(args))

    val sc = new SparkContext("local[*]", "EthGraph", conf = sparkConf)
    // "file:///home/burdeinyi_v_o_gmail_com/wallets.tsv"
    // "file:///home/burdeinyi_v_o_gmail_com/transactions.tsv"
    val graph = new EthereumGraph(sc, getWalletsFile(args), getTransactionsFile(args))

    var resultingOutput: String = ""
    resultingOutput += processGraphDiameter(graph)
    resultingOutput += separator
    resultingOutput += processShortestPath(graph, getSrcWallet(args), getDstWallet(args))
    resultingOutput += separator
    resultingOutput += processLabelPropagation(graph, getLabelPropagationIterations(args), getLabelPropagationOutputFile(args))
    resultingOutput += separator
    resultingOutput += processAnalytics(graph)
    resultingOutput += separator
    resultingOutput += processPageRank(graph, getPageRankTolerance(args), getPageRankIterations(args))
    resultingOutput += separator
    resultingOutput += processTriangleCount(graph)
    resultingOutput += separator
    resultingOutput += processConnectives(graph, getConnectivesOutputFile(args))

    val reportFile = getReportOutputFile(args)
    Files.write(Paths.get(reportFile), resultingOutput.getBytes(StandardCharsets.UTF_8))
    println(s"Processing report saved at $reportFile")

    sc.stop()
  }

  private def processPageRank(graph: EthereumGraph, tolerance: Double, iterations: Int): String = {
    var outputStr = "####### Page Rank #######\n"

    val topRankDynamic = graph.getTopDynamicRank(tolerance, 10, ascending = false)
    outputStr += "Top 10 rank (dynamic):\n"
    topRankDynamic.foreach(item => {
      outputStr += item + "\n"
    })

    outputStr += "\n"

    val topTankStatic = graph.getTopStaticRank(iterations, 10, ascending = false)
    outputStr += "Top 10 rank (static):\n"
    topTankStatic.foreach(item => {
      outputStr += item + "\n"
    })

    println(outputStr)
    outputStr
  }

  private def processTriangleCount(graph: EthereumGraph): String = {
    var outputStr = "####### Count Triangles #######\n"
    val triangleCounts = graph.countTriangles()
    outputStr += "Triangles count for 5 subsequent subgraph: " + triangleCounts.toString + "\n"
    println(outputStr)
    outputStr
  }

  private def processConnectives(graph: EthereumGraph, outputFile: String): String = {
    var outputStr = "####### Connectives #######\n"
    val connectives = graph.connectives()
    outputStr += s"Total connectives count is ${connectives.length}\n"

    val writer = new BufferedWriter(new FileWriter(outputFile))
    connectives.foreach(writer.write)
    writer.close()

    outputStr += s"Total connectives exact result saved in $outputFile\n"
    println(outputStr)
    outputStr
  }

  private def processAnalytics(graph: EthereumGraph): String = {
    var outputStr = "####### Vertices Analytics #######\n"

    val mostSpentVertex = graph.mostSpentVertex()
    outputStr += s"Most spent vertex is ${mostSpentVertex._1} with total value ${mostSpentVertex._2}\n"
    val mostReceivedVertex = graph.mostReceivedVertex()
    outputStr += s"Most received vertex is ${mostReceivedVertex._1} with total value ${mostReceivedVertex._2}\n"

    val mostSpentFeeVertex = graph.mostSpentFeeVertex()
    outputStr += s"Most spent fee vertex is ${mostSpentFeeVertex._1} with total value ${mostSpentFeeVertex._2}\n"
    val leastSpentFeeVertex = graph.leastSpentFeeVertex()
    outputStr += s"Least spent fee vertex is ${leastSpentFeeVertex._1} with total value ${leastSpentFeeVertex._2}\n"

    val maxVertexDegree = graph.maxVertexDegree()
    outputStr += s"Max vertex ${maxVertexDegree._1} degree value ${maxVertexDegree._2}\n"
    val maxVertexInDegree = graph.maxVertexInDegree()
    outputStr += s"Max vertex ${maxVertexInDegree._1} inDegree value ${maxVertexInDegree._2}\n"
    val maxVertexOutDegree = graph.maxVertexOutDegree()
    outputStr += s"Max vertex ${maxVertexOutDegree._1} outDegree value ${maxVertexOutDegree._2}\n"

    println(outputStr)
    outputStr
  }

  private def processShortestPath(graph: EthereumGraph, srcWallet: String, dstWallet: String): String = {
    var outputStr = "####### Shortest Path In The Graph #######\n"

    val shortestPath = graph.shortestPath(srcWallet, dstWallet)
    outputStr += s"Shortest path from $srcWallet to $dstWallet is ${shortestPath._1}\n"
    outputStr += "Path hops:\n"
    shortestPath._2.foreach(item => {
      outputStr += item + "\n"
    })
    println(outputStr)
    outputStr
  }

  private def processGraphDiameter(graph: EthereumGraph): String = {
    var outputStr = "####### Graph Diameter #######\n"
    val diameter = graph.calculateGraphDiameter()
    outputStr += s"Diameter: ${diameter._1}. From ${diameter._2} to ${diameter._3}\n"
    println(outputStr)
    outputStr
  }

  private def processLabelPropagation(graph: EthereumGraph, iterations: Int, outputFile: String): String = {
    var outputStr = "####### Label Propagation #######\n"
    val labelPropagationResult = graph.labelPropagation(iterations)
    outputStr += s"Label propagation result count ${labelPropagationResult.length}\n"

    val writer = new BufferedWriter(new FileWriter(outputFile))
    labelPropagationResult.foreach(writer.write)
    writer.close()

    outputStr += s"Label propagation exact values saved in $outputFile\n"
    println(outputStr)
    outputStr
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

  private def getConnectivesOutputFile(args: Array[String]): String = {
    val value = findArgument("--con-output-file", args)
    if (value.isEmpty) "connectives.tsv" else value
  }

  private def getLabelPropagationIterations(args: Array[String]): Int = {
    val value = findArgument("--lp-iterations", args)
    if (value.isEmpty) 7 else value.toInt
  }

  private def getLabelPropagationOutputFile(args: Array[String]): String = {
    val value = findArgument("--lp-output-file", args)
    if (value.isEmpty) "labelPropagation.tsv" else value
  }

  private def getReportOutputFile(args: Array[String]): String = {
    val value = findArgument("--report-output-file", args)
    if (value.isEmpty) "report.txt" else value
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