package com.github.ethgraph.ethereum

import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

class EthereumGraph(sc: SparkContext, walletsFile: String, transactionsFile: String) {
    
    def vertexs: RDD[(VertexId, String)] = sc.textFile(walletsFile).flatMap(InputParser.getWallets)
    def edges: RDD[Edge[(Double, Double, String)]] = sc.textFile(transactionsFile).flatMap(InputParser.getTransaction)

    val graph = Graph(vertexs, edges)

    def dijkstra(srcId: VertexId) = {
        var distancesGraph = graph.mapVertices((vId,_) => 
            (false, if (vId == sourceId) 0.0 else Double.PositiveInfinity))

        for (i <- 1L to graph.vertices.count-1) {
            val zeroValue = (0L,(false,Double.MaxValue))
            val currentVertexId = distancesGraph.vertices
                    .filter((vId, vd) => !vd._1)
                    .fold(zeroValue)((a,b) => math.min(a._2._2, b._2._2))
                    ._1

            val newDistances = distancesGraph
                .aggregateMessages[Double](
                    ctx => if (ctx.srcId == currentVertexId) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr._1),
                    (a,b) => math.min(a,b))

            distancesGraph = distancesGraph
                .outerJoinVertices(newDistances)((vId, vd, newSum) =>
                    (vd._1 || vId == currentVertexId, math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
        }

        graph.outerJoinVertices(distancesGraph.vertices)((_, vd, dist) =>
            (vd, dist.getOrElse((false,Double.MaxValue))._2))
    }
}