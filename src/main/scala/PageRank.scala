import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/*
   Both Shruti and David both pair programmed this entire file.
 */
object PageRank {

  def runPageRank(graph: Graph[String, Double]) : Graph[Double, Double] = {
    // Make a graph whose vertex property is its out degree
    val outDegreeGraph : Graph[Int, Double] = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank

    // create a graph whose vertices contain initial page rank and edges contain initial weights (1/outDegree)

    var rankGraph: Graph[Double, Double] = outDegreeGraph.
      // store edge weights
      mapTriplets(edgeTriplet => 1.0 / edgeTriplet.srcAttr).
      // store initial page rank
      mapVertices((id, _) => 1.0)


    var prevRankGraph: Graph[Double, Double] = null

    for (i <- 1 to 10) {

      rankGraph.cache() // force nodes to store graph to increase runtime

      val rankUpdates : RDD[(Long, Double)] = rankGraph.aggregateMessages[Double] (
        // Send message to each triplet in graph containing pageRank * edgeWeight
        edgeTriplet => edgeTriplet.sendToDst(edgeTriplet.srcAttr * edgeTriplet.attr),
        // Merge message. Accumulate page ranks
        _ + _,
        // Which triplet fields to include
        TripletFields.Src)

      prevRankGraph = rankGraph

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        // Outer join combines the rankGraph (id and old rank) with the computed rankUpdates (id and new page rank)
        // Compute the new page rank (Random page landing) + (Page ranks from its neighbors)
        (id, oldRank, msgSumOpt) => .15 + (1.0 - .15) * msgSumOpt.getOrElse(0.0)
        // Now rankGraph contains (id, newPageRank)
      }.cache()

      // remove references to previous iteration of page rank from memory
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      // Force vertices to cache
      rankGraph.edges.foreachPartition(x => {})

      println("Pagerank finished iteration $i")

    }

    // apparently this returns rankGraph
    rankGraph

  }

  def main(args: Array[String]): Unit = {

    // TODO remove local
//    val conf = new SparkConf().setAppName("PageRankCalculator")
    val conf = new SparkConf().setMaster("local").setAppName("PageRankCalculator")
    val sc = new SparkContext(conf)

    // TODO remove this
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", args(0))
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", args(1))

//    val vertex_data: RDD[String] = sc.textFile("data/shorter_vertices.txt")
    val vertex_data: RDD[String] = sc.textFile("s3n://dcomp-pagerank/shorter_vertices.txt")
    val vertices: RDD[(Long, String)] = vertex_data.map(_.split(' ')).
      map(line => (line(0).trim.toLong, line(1).trim))

    vertices.take(10).foreach(println)

    val edge_data: RDD[String] = sc.textFile("s3n://dcomp-pagerank/shorter_edges.txt")
    val edges: RDD[Edge[Double]] = edge_data
      // Split on spaces
      .map(_.split(' '))
      // Create edge objects
      .map(l => Edge(l(0).toLong, l(1).toLong, 1.0))

    val graph: Graph[String, Double] = Graph(vertices, edges, null)
    val pageRanks : Graph[Double, Double] = runPageRank(graph)

    val graph_urls_pagerank : Graph[(String, Double), Double] = graph.outerJoinVertices(pageRanks.vertices) (
      (id, url, pagerank) => (url, pagerank.getOrElse(0.0))
    )

    val strings_to_save: RDD[String] = sc.parallelize(graph_urls_pagerank.vertices.top(50) {
      Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2)
    }.map(t => t._2._1 + ": " + t._2._2))
    strings_to_save.saveAsTextFile("s3n://dcomp-pagerank/page_rank_output.txt")

    // This prints the pageranks of pages using builtin algorithm

//        val prGraph = graph.staticPageRank(10).cache
//    graph.outerJoinVertices(prGraph.vertices) {
//      (v, title, r) => (r.getOrElse(0.0), title)
//    }.vertices.top(10) {
//      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
//    }.foreach(t => println(t._2._2 + ": " + t._2._1))



    sc.stop()

  }
}
