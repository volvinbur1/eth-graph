package com.github.ethgraph.jobs.spp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import com.github.ethgraph.ethereum.EthereumGraph

object ShortestPathJob extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "GraphX")

    val walletsFile = "wallets.tsv"
    val transactionsFile = "transactions.tsv"

    val graph = new EthereumGraph(sc, walletsFile, transactionsFile)
    val res = graph.shortestPath(args(0), args(1))
    println("Short path value: " + res._1)
    println("Short path hops: " + res._2)
    sc.stop()
}