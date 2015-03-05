package se.sics.concepts.examples

import org.apache.spark.{SparkContext, SparkConf}
import se.sics.concepts.core.{CGraph, GraphBuilder, CKryoRegistrator}

/**
 * Running this program will create a json file as output, TODO(thvasilo): where?
 * which you can visualize using D3.js TODO(thvasilo): Will we include vis tool?
 * The output should be consistent with Figure 1 of the paper.
 */

object FigureOne {

  def main (args: Array[String]) {
    // Configure and initialize Spark
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CGraph")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
    val sc = new SparkContext(conf)

    val experimentPath = "src/main/scala/se/sics/concepts/examples/resources/"
    val bigramsFile = experimentPath + "figure1_pairs"

    val bigramCounts = sc
      .textFile(bigramsFile)
      .map(s => s.split(",").map(_.trim))
      .map(s => ((s(0), s(1)), s(2).toLong))

    // Allowed ranges of edge and vertex weights
    val vtxRange = (0.0, 1.0)
    val edgeRange = (0.0, 1.0)

    // Maximum in-degree
    val maxDegree: Long = 100

    val similarityGraph: CGraph = {
      println("Building co-occurrence graph")
      val graphBuilder = GraphBuilder(sc)
      // Builds co-occurrence graph from bigram counts
      val (vertices, edges) = graphBuilder.vtxEdgesFromBigram(bigramCounts)
      // Map co-occurrence graph to similarity graph
      CGraph(vertices, edges, maxDegree, vtxRange, edgeRange, sc)
    }

    // TODO(thvasilo): Decide whether we want to include any clustering
    val clusterAssignments = similarityGraph.clustersByConnectivityGraphX()

    similarityGraph.exportSimilarityGraphToJSON("figure1_sim_graph.json", 0.0, clusterAssignments)

    sc.stop
  }

}
