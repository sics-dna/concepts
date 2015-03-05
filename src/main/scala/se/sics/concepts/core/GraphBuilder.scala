package se.sics.concepts.core

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * Used to create the correlation graph.
 *
 * This class uses a directed pair count RDD as input, where by pair count we mean that the RDD maps
 * a (directed) pair of concepts to a co-occurrence count. It creates the correlation graph by creating
 * a vertex for each unique item, and adding edges between items that have co-occurred according
 * to the pair count RDD. The vertices are assigned a weight that corresponds to their relative
 * frequency in the data. The edges  have the count of the pair co-occurrences divided by the
 * count of the source vertex as weight. In other words, the edge weight for pair (i, j) is
 * the probability of encountering j, given that we have encountered i.
 *
 * @param sc The [[org.apache.spark.SparkContext]] in which the application is run.
 */
class GraphBuilder(@transient sc: SparkContext) extends Serializable {

  /**
   * Creates an edges and a vertices [[org.apache.spark.RDD]] from a pair count RDD input.
   * @param bigramsRDD An RDD containing pairs of concepts and the number of times they co-occur.
   * @return A tuple containing the vertices RDD as its first element and the edges RDD as its second element.
   */
  def vtxEdgesFromBigram(bigramsRDD: RDD[((String, String), Long)]) = {
    // Token indices: ("token", "index")
    val indices = bigramsRDD.flatMap{case ((w, v), c) => Iterable(w, v)}.distinct.zipWithIndex

    // Map bigram count tokens to indices
    val adjacencyCounts = bigramsRDD
      .map{case ((w, v), c) => (w, (v, c))}.join(indices)
      // Map first token to index
      .map{case (w, ((v, c), i)) => (v, (i, c))}.join(indices)
      // Map second token to index
      .map{case (v, ((i, c), j)) => ((i, j), c)}

    // ("token index", "count") tuples
    val vertexCounts = adjacencyCounts.map{case ((v, w), c) => (w, c)}.reduceByKey(_ + _)

    // Total number of token occurrences
    val totalAdjacencyCount = adjacencyCounts.map{case ((i, j), c) => c}.reduce(_ + _).toDouble

    // ("token index", "fraction") tuples
    // Count both sources and destinations so that we do not exclude vertices that lack outgoing edges
    val vertexFrequencies = adjacencyCounts
      .flatMap{case ((w, v), c) => Iterable((w, c), (v, c))}.reduceByKey(_ + _)
      .map{case (vw, csum) => (vw, csum.toDouble/(2.0 * totalAdjacencyCount))}

    // (("focus token index", "context token index"), "probability") tuples
    val edges: RDD[((Long, Long), Double)] = adjacencyCounts
      .map{case ((v, w), c) => (w, (v, c))}
      .join(vertexCounts).map{case (v, ((w, c), cc)) => ((v, w), c.toDouble/cc.toDouble)}

    // Create vertices RDD
    val vertices: RDD[(Long, String, Double)] =
      indices
      .map{case (s, i) => (i, s)}
      .join(vertexFrequencies)
      .map{case (i, (s, w)) => (i, s, w)}

    (vertices, edges)
  }

  /**
   * Builds a co-occurrence graph from a pair count [[org.apache.spark.RDD]] and writes it to disk
   * @param bigramsRDD An RDD containing pairs of concepts and the number of times they co-occur.
   * @param graphFilePrefix The full prefix path under which the intermediate files will saved
   */
  def adjacentFromBigrams(bigramsRDD: RDD[((String, String), Long)], graphFilePrefix: String) {

    bigramsRDD.saveAsTextFile(graphFilePrefix + "-bigrams")
    val (vertices, edges) = vtxEdgesFromBigram(bigramsRDD)

    // Write edges and vertices to text file
    edges.saveAsTextFile(graphFilePrefix + "-edges")
    vertices.saveAsTextFile(graphFilePrefix + "-vertices")

    println(s"#vertices:    ${vertices.count}")
    println(s"#edges:       ${edges.count}")
    // println(s"#occurrences: ${totalVertexCount}")
  }

  /**
   * Builds a co-occurrence graph from a bigrams file and writes it to files
   * @param bigramsFile A path pointing to a pairs file, where each line has the count,item1,item2 format
   * @param graphFilePrefix The full prefix path under which the intermediate files will saved
   */
  def adjacentFromBigramsFile(bigramsFile: String, graphFilePrefix: String) = {

    // Assumes a comma separated file with (count,item1,item2) entries and generates (("first token", "second token"), "count") tuples
    val bigramCounts = sc
      .textFile(bigramsFile)
      .map(s => s.split(",").map(_.trim))
      .map(s => ((s(1), s(2)), s(0).toLong))

    adjacentFromBigrams(bigramCounts, graphFilePrefix)
  }

}

/**
 * Factory for GraphBuilder instances
 */
object GraphBuilder {
  def apply(@transient sc: SparkContext) = new GraphBuilder(sc)
}
