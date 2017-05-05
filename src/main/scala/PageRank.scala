import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object PageRank {

  def runPageRank(graph: Graph[String, Double]) = {
    // Make a graph whose vertex property is its out degree
    val outDegreeGraph : Graph[Int, Double] = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank

    // create a graph whose vertices contain initial page rank and edges contain initial weights (1/outDegree)
    val outputGraph: Graph[Double, Double] = outDegreeGraph.
      // store edge weights
      mapTriplets(edgeTriplet => 1.0 / edgeTriplet.srcAttr).
      // store initial page rank
      mapVertices((id, _) => 1.0 / graph.numVertices)

    for (i <- 1 to 10) {
      val rankUpdates : RDD[(Long, Double)] = outputGraph.aggregateMessages[Double] (
        // Send message to each triplet in graph containing pageRank * edgeWeight
        edgeTriplet => edgeTriplet.sendToDst(edgeTriplet.srcAttr * edgeTriplet.attr),
        // Merge message. Accumulate page ranks
        _ + _,
        // Which triplet fields to include
        TripletFields.Src)


    }









    // TODO
    // Run loop and update page ranks!

  }

  def main(args: Array[String]): Unit = {

    // TODO remove local
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

//    val prGraph = graph.staticPageRank(5).cache
//    graph.outerJoinVertices(prGraph.vertices) {
//      (v, title, r) => (r.getOrElse(0.0), title)
//    }.vertices.top(10) {
//      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
//    }.foreach(t => println(t._2._2 + ": " + t._2._1))
//    sc.stop()

  }
}
