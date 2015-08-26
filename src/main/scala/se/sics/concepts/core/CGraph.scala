package se.sics.concepts.core

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import play.api.libs.json._

import scala.math._
import scala.util.Random

/**
 * Creates the similarity graph using a correlation graph as input.
 *
 *
 *
 * @param vertices An RDD containing the vertices of the correlation graph.
 * @param edges An RDD containing the edges of the correlation graph.
 * @param inDegreeThreshold The maximum number of incoming edges a vertex is allowed to have.
 * @param vtxWRange The range of vertex weights that we keep in the correlation graph.
 * @param edgWRange The range of edge weights that we keep in the correlation graph.
 * @param sc The [[org.apache.spark.SparkContext]] in which the application is run.
 */
class CGraph(
    vertices:  RDD[(Long, String, Double)],
    edges: RDD[((Long, Long), Double)],
    inDegreeThreshold: Long,
    vtxWRange: (Double, Double),
    edgWRange: (Double, Double),
    @transient sc: SparkContext) extends Serializable {

  // Discard vertices
  lazy val prunedVertices = vertices.filter { case (i, s, wi) => wi >= vtxWRange._1 && wi <= vtxWRange._2}

  // Key-value pairs from indices to weights
  lazy val vertexIndexWeights = vertices.map { case (i, s, wi) => (i, wi)}

  lazy val edgeWeightTuples = edges
    .map { case ((i, j), wij) => (i, (j, wij))}.join(vertexIndexWeights)
    .map { case (i, ((j, wij), wi)) => (j, ((i, wi), wij))}.join(vertexIndexWeights)
    .map { case (j, (((i, wi), wij), wj)) => (i, j, wi, wj, wij)}.cache

  lazy val outDegrees: RDD[(Long, Int)] = edges.map { case ((i, j), wij) => (i, 1)}.reduceByKey(_ + _)

  /**
   * Prunes the graph using the provided edge weight thresholds, but not the inDegreeThreshold.
   *
   * @return A tuple containing RDDs of the pruned edges as its first element, the sums of the
   *         discarded edge weights as its second element, and the sums of the remaining edge weights.
   */
  def pruneByWeightThresholds = {

    // Edges with weights within allowed ranges
    val prunedEdges = edgeWeightTuples
      .filter { case (i, j, wi, wj, wij) =>
      wi >= vtxWRange._1 && wi <= vtxWRange._2 &&
        wj >= vtxWRange._1 && wj <= vtxWRange._2 &&
        wij >= edgWRange._1 && wij <= edgWRange._2
    }
      .map { case (i, j, wi, wj, wij) => ((i, j), wij)}

    // Sums of weights of discarded outgoing edges
    val discardedEdgeWeightSums = edgeWeightTuples
      .filter { case (i, j, wi, wj, wij) =>
      wi < vtxWRange._1 || wi > vtxWRange._2 ||
        wj < vtxWRange._1 || wj > vtxWRange._2 ||
        wij < edgWRange._1 || wij > edgWRange._2
    }
      .map { case (i, j, wi, wj, wij) => (i, math.abs(wij))}
      .reduceByKey(_ + _)

    // Sums of weights of remaining outgoing edges
    val remainingEdgeWeightSums: RDD[(Long, Double)] = prunedEdges
      .map { case ((i, j), wij) => (i, math.abs(wij))}
      .reduceByKey(_ + _)

    (prunedEdges, discardedEdgeWeightSums, remainingEdgeWeightSums)
  }

  /**
   * Prunes the graph using the provided inDegreeThreshold.
   *
   * Here for each vertex we keep the top-k highest weight incoming edges, where k is
   * given by the inDegreeThreshold.
   *
   * @return A tuple containing RDDs of the pruned edges as its first element, the sums of the
   *         discarded edge weights as its second element, and the sums of the remaining edge weights.
   */
  def pruneByInDegree = {
    // Top in-edges w r t weight
    def edgesToKeep(indexAndEdges: (Long, Iterable[(Long, Double)])) = {
      val j = indexAndEdges._1
      indexAndEdges._2
        // Sort in reverse order
        .toArray.sortBy(e => -math.abs(e._2))
        // Keep top edges
        .take(inDegreeThreshold.toInt)
        // Set j as sink
        .map { case (i, w) => ((i, j), w)}
    }

    val prunedEdges = edgeWeightTuples
      // Keep vertices with valid edges
      .filter { case (i, j, wi, wj, wij) =>
      wi >= vtxWRange._1 && wi <= vtxWRange._2 &&
        wj >= vtxWRange._1 && wj <= vtxWRange._2
    }
      // Collect incoming edges per vertex
      .map { case (i, j, wi, wj, wij) => (j, (i, wij))}.groupByKey
      // Bound in-degree
      .flatMap(edgesToKeep(_))

    val remainingEdgeWeightSums: RDD[(Long, Double)] = prunedEdges
      .map { case ((i, j), wij) => (i, math.abs(wij))}.reduceByKey(_ + _)

    val discardedEdgeWeightSums: RDD[(Long, Double)] = edges
      // Keep edges that have valid source vertices
      .map { case ((i, j), wij) => (i, (j, wij))}.join(vertexIndexWeights)
      .filter { case (i, ((j, wij), wi)) => wi >= vtxWRange._1 && wi <= vtxWRange._2}
      // Calculate weight sums of all edges (prior to pruning)
      .map { case (i, ((j, wij), wi)) => (i, math.abs(wij))}.reduceByKey(_ + _)
      // Subtract weight sums of kept edges
      .leftOuterJoin(remainingEdgeWeightSums)
      .map { case (i, (all, remaining)) => (i, all - remaining.getOrElse(0.0))}

    (prunedEdges, discardedEdgeWeightSums, remainingEdgeWeightSums)
  }

  // Prune by in-degree if threshold value is given, otherwise prune edges
  // using weight threshold
  lazy val edgesAndWeightSums =
    if (inDegreeThreshold == Long.MaxValue) pruneByWeightThresholds
    else pruneByInDegree

  edgeWeightTuples.unpersist()

  lazy val prunedEdges = edgesAndWeightSums._1.cache
  lazy val discardedEdgeWeightSums = edgesAndWeightSums._2.cache
  lazy val remainingEdgeWeightSums = edgesAndWeightSums._3.cache

  /**
   * Calculates remaining and discarded weight sums for each pair after the pruning is done.
   *
   * Specifically the resulting RDD has the format (i, j, sij, ri, rj, di, dj) where:
   * i and j are the vertex pair ids; sij is the contribution to the L1 norm where i and j share
   * neighbors; ri, rj are the remaining weight sums of vertex i and j respectively and di, dj
   * are the discarded weight sums for i and j respectively.
   *
   */
  lazy val sharedTermsWithSums: RDD[(Long, Long, Double, Double, Double, Double, Double)] = {
    // Collect incoming edges per vertex
    val inEdgesPerVertex = prunedEdges
      .map { case ((i, j), w) => (j, (i, w))}
      .partitionBy(new HashPartitioner(1000))
      .cache

    // Get all pairs of incoming edges through self-join
    val edgePairs = inEdgesPerVertex.join(inEdgesPerVertex)
      // Discard duplicates and self-referentials
      .filter { case (k, ((i: Long, wi), (j: Long, wj))) => i < j}

    // Contribution to L1 norm where i and j share neighbours
    val sharedTerms: RDD[((Long, Long), Double)] = edgePairs
      .map { case (k, ((i: Long, wi: Double), (j: Long, wj: Double))) => ((i, j), math.abs(wi - wj) - math.abs(wi) - math.abs(wj))}
      .reduceByKey(_ + _)

    // Join with sums of kept and discarded weights per vertex
    sharedTerms
      // First add i's sums
      .map { case ((i: Long, j: Long), sij: Double) => (i, (j, sij))}
      // Note: Uses leftOuterJoin instead of join since otherwise we may loose vertices that
      // do not have vertex weights below threshold
      .join(remainingEdgeWeightSums).leftOuterJoin(discardedEdgeWeightSums)
      // Add j's sums (ri = "remaining weight sum of i", di = "discarded weight sum of i")
      .map { case (i, (((j, sij), ri), di: Option[Double])) => (j, (i, sij, ri, di.getOrElse(0.0)))}
      .join(remainingEdgeWeightSums).leftOuterJoin(discardedEdgeWeightSums)
      // Reformat and fetch optional sums in dj
      .map { case (j, (((i, sij, ri, di), rj), dj: Option[Double])) => (i, j, sij, ri, rj, di, dj.getOrElse(0.0))}
  }


  /**
   * Calculates the max relative L1 norm and the max error for each pair.
   */
  lazy val relativeL1NormWithErrorBound: RDD[((Long, Long), Double, Double)] = {
    // Sum terms and normalize with respect to maximum possible L1 norm
    // The maximum possible error is given by the sum of discarded edge weighs of i and j
    sharedTermsWithSums
      .map { case (i, j, sij, ri, rj, di, dj) => {
        val tot = ri + rj + di + dj
        ((i, j), (sij + tot) / tot, (di + dj) / tot)
        }
      }
  }

  /** Similarities, where S(i, j) is one subtracted by the relative L1 norm */
  lazy val simEdges: RDD[((Long, Long), Double)] = {
    relativeL1NormWithErrorBound.map { case ((i, j), relL1, maxError) => ((i, j), 1.0 - relL1)}
  }

  def directedSimilarityEdges = simEdges.flatMap{case ((i, j), w) =>  Iterable(((i, j), w), ((j, i), w))}

  lazy val relativeL1NormWithLabels: RDD[((String, String), Double, Double)] = {
    // Key-value pairs from indices to strings
    val vertexIndexStrings = prunedVertices.map{case (i, si, wi) => (i, si)}
    // Map indices to labels
    relativeL1NormWithErrorBound
      .map{case ((i, j), wij, d) => (i, (j, wij, d))}
      .join(vertexIndexStrings)
      .map{case (i, ((j, wij, d), si)) => (j, (si, wij, d))}
      .join(vertexIndexStrings)
      .map{case (j, ((si, wij, d), sj)) => ((si, sj), wij, d)}
  }

  // Key-value pairs from indices to strings
  lazy val vertexIndexStrings = prunedVertices.map { case (i, si, wi) => (i, si) }

  // Similarities with labels
  lazy val similarityEdgesWithLabels: RDD[((String, String), Double)] = {
    // Map indices to labels
    simEdges
      .map { case ((i, j), wij) => (i, (j, wij)) }
      .join(vertexIndexStrings)
      .map { case (i, ((j, wij), si)) => (j, (si, wij)) }
      .join(vertexIndexStrings)
      .map { case (j, ((si, wij), sj)) => ((si, sj), wij) }
  }

  /**
   * Finds the connected components in the graph and uses those as clusters.
   * @return The vertices RDD of the connected components graph, which maps vertex id to component id.
   */
  def clustersByConnectivityGraphX(): RDD[(Long, Long)] = {
    val edges: RDD[Edge[Double]] = directedSimilarityEdges.map{case ((i, j), w) => Edge(i, j, w)}
    val similarityGraph: Graph[String, Double] = Graph(vertexIndexStrings, edges)
    ConnectedComponents.run(similarityGraph).vertices
  }


  /**
   * Exports similarities to JSON for D3 visualization
   * Note: Edges are weighted with 1 - L, where L is the relative L_1 norm
   * Vertices without edges are discarded
   * @param fileName The filename for the resulting JSON file.
   * @param weightThreshold Edges with weights below weightThreshold are discarded
   * @param clusterAssignments The cluster assignments RDD as returned by [[CGraph.clustersByConnectivityGraphX()]]
   */
  def exportSimilarityGraphToJSON(fileName: String, weightThreshold: Double, clusterAssignments: RDD[(Long, Long)]): Unit = {
    // Discard pairs with similarity below threshold
    val prunedSimilarityEdges = relativeL1NormWithErrorBound
      .filter{case ((iOld, jOld), n, e) => 1.0 - n > weightThreshold}
      .map{case ((iOld, jOld), n, e) => (iOld, (jOld, n))}

    val prunedSimilarityVertices = prunedSimilarityEdges
      // Collect remaining vertex indices
      .flatMap{case (iOld, (jOld, n)) => Iterable(iOld, jOld)}.distinct
      // Join with labels (note: possible to skip tmp string here?)
      .map((_, "")).join(vertexIndexStrings).map{case (iOld, (tmp, s)) => (iOld, s)}
      // Add cluster assignments
      .join(clusterAssignments)
      // Re-index and sort (note: possibly already sorted)
      .zipWithIndex.map{case ((iOld, (s, c)), iNew) => (iNew, (iOld, s, c))}.sortBy(_._1)

    val oldToNewIndices = prunedSimilarityVertices.map{case (iNew, (iOld, s, c)) => (iOld, iNew)}.cache

    // Map to new indices
    val indexEdges = prunedSimilarityEdges.join(oldToNewIndices)
      .map{case (iOld, ((jOld, n), iNew)) => (jOld, (iNew, n))}.join(oldToNewIndices)
      // Set similarity as one subtracted by relative L1 norm
      .map{case (jOld, ((iNew, n), jNew)) => (iNew, jNew, 1.0 - n)}

    oldToNewIndices.unpersist()

    // Convert to JSON
    val jsonVertices = prunedSimilarityVertices
      .map{case (iNew, (iOld, s, c)) => JsObject(Seq("name" -> JsString(s), "group" -> JsNumber(c.toInt)))}.collect
    val jsonEdges = indexEdges
      .map{case (i, j, n) => JsObject(Seq("source" -> JsNumber(i), "target" -> JsNumber(j), "value" -> JsNumber(n)))}.collect
    val jsonGraph = JsObject(Seq("nodes" -> JsArray(jsonVertices), "links" -> JsArray(jsonEdges)))

    // Explicitly set encoding
    val file = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(fileName)), "UTF-8"), false)
    file.write(Json.prettyPrint(jsonGraph))
    file.close()
  }

  /** Print information about the graph */
  def printInfo: Unit = {
    println("\nedges:")
    edges.collect.foreach(println)
    println("\nvertices:")
    vertices.collect.foreach(println)
    println("\nprunedEdges:")
    prunedEdges.collect.foreach(println)
    println("\nprunedVertices:")
    prunedVertices.collect.foreach(println)
  }
}

/**
 * Factory for [[CGraph]] instances.
 */
object CGraph {

  def readVtxFile(@transient sc: SparkContext, vtxFile: String) = {
    sc.textFile(vtxFile).map(s => s.replaceAll("\\(|\\)","").split(",")).map(s => (s(0).trim.toLong, s(1).trim, s(2).trim.toDouble))
  }

  def readEdgeFile(@transient sc: SparkContext, edgFile: String) = {
    sc.textFile(edgFile).map(s => s.replaceAll("\\(|\\)","").split(",")).map(s => ((s(0).trim.toLong, s(1).trim.toLong), s(2).trim.toDouble))
  }

  def apply(
    vtxFile: String, edgFile: String,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(readVtxFile(sc, vtxFile), readEdgeFile(sc, edgFile), Long.MaxValue, vtxWRange, edgWRange, sc)

  def apply(
    vertices:  RDD[(Long, String, Double)], edges: RDD[((Long, Long), Double)],
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(vertices, edges, Long.MaxValue, vtxWRange, edgWRange, sc)

  def apply(
    vtxFile: String, edgFile: String,
    maxDegree: Long,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(readVtxFile(sc, vtxFile), readEdgeFile(sc, edgFile), maxDegree, vtxWRange, edgWRange, sc)

  def apply(
    vertices:  RDD[(Long, String, Double)], edges: RDD[((Long, Long), Double)],
    maxDegree: Long,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(vertices, edges, maxDegree, vtxWRange, edgWRange, sc)

}
