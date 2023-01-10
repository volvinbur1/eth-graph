package com.github.ethgraph.jobs.ssp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ShortestPathJob {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "GraphX")

        val graph = new EthereumGraph(sc)
        val res = graph.shortestPath(args(0), args(1))
        println("Short path value: " + res._1)
        println("Short path hops: " + res._2)
        sc.stop()
    }
}