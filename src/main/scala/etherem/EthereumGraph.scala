package com.github.ethgraph
package etherem

import org.apache.spark._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class EthereumGraph (sc: SparkContext, walletsFile: String, transactionsFile: String) {

  private def vertices: RDD[(VertexId, String)] = sc.textFile(walletsFile).flatMap(InputParser.getWallets)
  private def edges: RDD[Edge[(Double, Double, String)]] = sc.textFile(transactionsFile).flatMap(InputParser.getTransaction)

  private val graph = Graph(vertices, edges)

  private def dijkstra(srcId: VertexId) = {
    var distancesGraph = graph.mapVertices((vId,_) => (
      false,
      if (vId == srcId) 0.0 else Double.PositiveInfinity,
      if (vId == srcId) vId :: Nil else Nil)
    )

    for (i <- 1L to graph.vertices.count-1) {
      val zeroValue = (0L, (false, Double.MaxValue, Nil))
      val currentVertexId = distancesGraph.vertices
        .filter(pred => !pred._2._1)
        .fold(zeroValue)((a,b) => if (a._2._2 < b._2._2) a else b)
        ._1

      val newDistances = distancesGraph
        .aggregateMessages[(Double, List[VertexId])] (
          ctx => if (ctx.srcId == currentVertexId)
            ctx.sendToDst((ctx.srcAttr._2 + ctx.attr._2, ctx.srcAttr._3 :+ ctx.dstId)),
          (a,b) => if (a._1 < b._1) a else b)

      distancesGraph = distancesGraph
        .outerJoinVertices(newDistances) ((vId, vd, newDist) => (
          vd._1 || vId == currentVertexId,
          math.min(vd._2, newDist.getOrElse((Double.MaxValue, Nil))._1),
          if (vd._2 < newDist.getOrElse((Double.MaxValue, Nil))._1) vd._3 else newDist.getOrElse((Double.MaxValue, Nil))._2
        ))
    }

    distancesGraph
  }

  def shortestPath(srcWallet: String, dstWallet: String): (Double, List[String]) = {
    val srcId = vertices.filter(_._2 == srcWallet).first()._1
    val dstId = vertices.filter(_._2 == dstWallet).first()._1

    val distances = dijkstra(srcId)
    val resultingVertex = distances.vertices.filter(pred => pred._1 == dstId).first()

    val hops = new ListBuffer[String]()
    resultingVertex._2._3.foreach(x => hops += vertices.filter(_._1 == x).first()._2)

    (resultingVertex._2._2, hops.toList)
  }

  private def dynamicRank(tolerance: Double): VertexRDD[Double] = {
    graph.pageRank(tolerance).vertices
  }

  private def staticRank(iterNumber: Int): VertexRDD[Double] = {
    graph.staticPageRank(iterNumber).vertices
  }

  private def formatRankResult(rankVertices: RDD[(VertexId, Double)], topCnt: Int) = {
    val result = new ListBuffer[(String, Double)]()
    rankVertices.take(topCnt).foreach({ case (vId, vd) => result += ((vertices.filter(_._1 == vId).first()._2, vd)) })

    result.toList
  }
  def getTopDynamicRank(tolerance: Double, topCnt: Int, ascending: Boolean): List[(String, Double)] = {
    val rankVertices = dynamicRank(tolerance).sortBy({ case (_, vd) => vd}, ascending = ascending)
    formatRankResult(rankVertices, topCnt)
  }

  def getTopStaticRank(iterNumber: Int, topCnt: Int, ascending: Boolean): List[(String, Double)] = {
    val rankVertices = staticRank(iterNumber).sortBy({ case (_, vd) => vd }, ascending = ascending)
    formatRankResult(rankVertices, topCnt)
  }

  def countTriangles(): List[PartitionID] = {
    val newGraph = graph.partitionBy(PartitionStrategy.RandomVertexCut)

    val multiplier = vertices.collect().length / 5
    val result = new ListBuffer[PartitionID]()
    (0 to 4).map(idx => {
      val triangleCnt = newGraph.subgraph(vpred = {
        case (vId, _) => (vId >= idx * multiplier && vId <= (idx + 1) * multiplier)
      }).triangleCount().vertices.map(_._2).reduce(_ + _)
      result += triangleCnt
    })
    result.toList
  }

  def connectives(): List[String] = {
    implicit val connectivesOrdering: Ordering[(VertexId, VertexId)] = Ordering.by(_._2)
    val connectedGraph = graph.connectedComponents()
    val maxSubGraph = connectedGraph.vertices.max()(connectivesOrdering)._2

    val result = new ListBuffer[String]()
    (1 to maxSubGraph.toInt).foreach(idx => {
      val subgraphRanks = connectedGraph
        .subgraph(vpred = (_, cnt) => idx == cnt)
        .pageRank(0.001)
        .vertices
        .sortBy({ case (_, vd) => vd }, ascending = true).collect()

      val firstVertexAddress = vertices.filter(_._1 == subgraphRanks.head._1).first()._2
      val lastVertexAddress = vertices.filter(_._1 == subgraphRanks.last._1).first()._2
      result += s"$firstVertexAddress   ...   $lastVertexAddress"
    })

    result.toList
  }

  def labelPropagation(iterCnt: Int): List[String] = {
    val result = new ListBuffer[String]()
    LabelPropagation.run(graph, iterCnt)
      .vertices
      .collect()
      .foreach(item => {
        val firstWallet = vertices.filter(_._1 == item._1).first()._2
        val secondWallet = vertices.filter(_._1 == item._2).first()._2
        result += s"${item._1}\t($firstWallet)\t${item._2}\t($secondWallet)\n"
      })
    result.toList
  }
}
