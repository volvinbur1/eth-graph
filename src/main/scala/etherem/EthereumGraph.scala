package com.github.ethgraph
package etherem

import org.apache.spark._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class EthereumGraph (sc: SparkContext, walletsFile: String, transactionsFile: String) {

  private def vertices: RDD[(VertexId, String)] = sc.textFile(walletsFile).flatMap(InputParser.getWallets).cache()
  private def edges: RDD[Edge[(Double, Double, String)]] = sc.textFile(transactionsFile).flatMap(InputParser.getTransaction).cache()

  private val graph = Graph(vertices, edges).cache()

  private def getVertexDesc(vertexId: VertexId) = {
    vertices.filter(_._1 == vertexId).first()._2
  }

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
    resultingVertex._2._3.foreach(x => hops += getVertexDesc(x))

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
    rankVertices.take(topCnt).foreach({ case (vId, vd) => result += ((getVertexDesc(vId), vd)) })

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
    val maxSubgraph = connectedGraph.vertices.max()(connectivesOrdering)._2

    println(s"[ETH-GRAPH][INFO] Max value of connected subgraphs is $maxSubgraph")

    val result = new ListBuffer[String]()
    (1 to maxSubgraph.toInt).foreach(idx => {
      val subgraphVertices = connectedGraph
        .subgraph(vpred = (_, cnt) => idx == cnt)
        .vertices
//        .sortBy({ case (_, vd) => vd }, ascending = true)

      if (subgraphVertices.count() == 0) {
        println(s"[ETH-GRAPH][INFO] Subgraph at $idx is empty")
      } else {
        val firstVertexAddress = getVertexDesc(subgraphVertices.first()._1)
        val lastVertexAddress = getVertexDesc(subgraphVertices.top(1).head._1)
        result += s"$firstVertexAddress   ...   $lastVertexAddress"
      }
    })

    result.toList
  }

  def labelPropagation(iterCnt: Int): List[String] = {
    val result = new ListBuffer[String]()
    LabelPropagation.run(graph, iterCnt)
      .vertices
      .collect()
      .foreach(item => {
        val firstWallet = getVertexDesc(item._1)
        val secondWallet = getVertexDesc(item._2)
        result += s"${item._1}\t($firstWallet)\t${item._2}\t($secondWallet)\n"
      })
    result.toList
  }

  def calculateGraphDiameter(): (Double, String, String) = {
    var maxDist = 0.0
    var srcWallet = ""
    var dstWalletId = 1L

    vertices.collect().foreach(item => {
      implicit val pathOrdering: Ordering[(VertexId, (Boolean, Double, List[VertexId]))] = Ordering.by(_._2._2)
      val max = dijkstra(item._1).vertices.filter(_._2._2 != Double.MaxValue).max()(pathOrdering)
      if (maxDist < max._2._2) {
        maxDist = max._2._2
        srcWallet = item._2
        dstWalletId = max._1
      }
    })
    val dstWallet = getVertexDesc(dstWalletId)
    (maxDist, srcWallet, dstWallet)
  }

  implicit val vertexOrdering: Ordering[(VertexId, Double)] = Ordering.by(_._2)
  def mostSpentVertex(): (String, Double) = {
    val maxVertex = graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr._1), _ + _).max()(vertexOrdering)
    (getVertexDesc(maxVertex._1), maxVertex._2)
  }

  def mostReceivedVertex(): (String, Double) = {
    val maxVertex = graph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.attr._1), _ + _).max()(vertexOrdering)
    (getVertexDesc(maxVertex._1), maxVertex._2)
  }

  def mostSpentFeeVertex(): (String, Double) = {
    val maxVertex = graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr._2), _ + _).max()(vertexOrdering)
    (getVertexDesc(maxVertex._1), maxVertex._2)
  }

  def leastSpentFeeVertex(): (String, Double) = {
    val maxVertex = graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr._2), _ + _).min()(vertexOrdering)
    (getVertexDesc(maxVertex._1), maxVertex._2)
  }

  implicit val degreeOrdering: Ordering[(VertexId, Int)] = Ordering.by(_._2)
  def maxVertexDegree(): (String, PartitionID) = {
    val degrees = graph.degrees.max()(degreeOrdering)
    (getVertexDesc(degrees._1), degrees._2)
  }

  def maxVertexInDegree(): (String, PartitionID) = {
    val degrees = graph.inDegrees.max()(degreeOrdering)
    (getVertexDesc(degrees._1), degrees._2)
  }

  def maxVertexOutDegree(): (String, PartitionID) = {
    val degrees = graph.outDegrees.max()(degreeOrdering)
    (getVertexDesc(degrees._1), degrees._2)
  }
}
