package com.github.ethgraph.ethereum

import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

class EthereumGraph(sc: SparkContext, walletsFile: String, transactionsFile: String) {
    
    def vertexs: RDD[(VertexId, String)] = sc.textFile(walletsFile).flatMap(InputParser.getWallets)
    def edges: RDD[Edge[(Double, Double, String)]] = sc.textFile(transactionsFile).flatMap(InputParser.getTransaction)

    val graph = Graph(vertexs, edges)

    def dijkstra(srcId: VertexId) = {
        var distancesGraph = graph.mapVertices((vId,_) => (
                false, 
                if (vId == sourceId) 0.0 else Double.PositiveInfinity), 
                if (vId == sourceId) vId :: Nil else Nil
            )

        for (i <- 1L to graph.vertices.count-1) {
            val zeroValue = (0L, (false, Double.MaxValue, Nil))
            val currentVertexId = distancesGraph.vertices
                    .filter((vId, vd) => !vd._1)
                    .fold(zeroValue)((a,b) => math.min(a._2._2, b._2._2))
                    ._1

            val newDistances = distancesGraph
                .aggregateMessages[(Double, List[VertexId])] (
                    ctx => if (ctx.srcId == currentVertexId) 
                                ctx.sendToDst((ctx.srcAttr._2 + ctx.attr._1, ctx.srcAttr._3 :+ ctx.dstId)),
                    (a,b) => math.min(a._1, b._1))

            distancesGraph = distancesGraph
                .outerJoinVertices(newDistances) ((vId, vd, newDist) => (
                    vd._1 || vId == currentVertexId, 
                    math.min(vd._2, newDist.getOrElse((Double.MaxValue, Nil))._1),
                    if (vd._2 < newDist.getOrElse((Double.MaxValue, Nil))._1) vd._2 else newDist.getOrElse((Double.MaxValue, Nil))._2
                ))
        }

        distancesGraph
    }
}