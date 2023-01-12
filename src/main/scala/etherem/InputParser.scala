package com.github.ethgraph
package etherem

import org.apache.spark.graphx.{Edge, VertexId}

import java.io.IOException
import scala.collection.mutable.ListBuffer

object InputParser {

  def getWallets(line: String): Option[(VertexId, String)] = {
    val fields = line.split('\t')
    try {
      Some(fields(0).toLong, fields(1))
    } catch {
      case _: Throwable => println("ERROR: WRONG INPUT for wallets -> " + line)
      Some(Long.MaxValue, "NotKnownValue")
    }
  }

  def getTransaction(line: String): Option[Edge[(Double, Double, String)]] = {
    val fields = line.split('\t')
    Some(Edge(fields(0).toLong, fields(1).toLong, (fields(2).toDouble, fields(3).toDouble, fields(4))))
  }
}
