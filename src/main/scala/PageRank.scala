import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


object PageRank {

  def runPageRank(graph: Graph[String, Double]) = {
    val oldPageRanks : RDD[(Long, Double)] = graph.vertices.map(v => (v._1, 1.0 / graph.numVertices))
    val newPageRanks : RDD[(Long, Double)] = graph.vertices.map(v => (v._1, 1.0 / graph.numVertices))
    var ranks = graph.vertices.mapValues((v : String) => v)
    for (i <- 1 to 10) {
      val s = oldPageRanks.map((t: (Long,Double)) => (t._1, 0.15/graph.numVertices))
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
    val vertices: RDD[(VertexId, String)] = vertex_data.map(_.split(' ')).
      map(line => (line(0).trim.toLong, line(1).trim))

    vertices.take(10).foreach(println)

    val edge_data: RDD[String] = sc.textFile("s3n://dcomp-pagerank/shorter_edges.txt")
    val edges: RDD[Edge[Double]] = edge_data.map(_.split(' ')).map(l => Edge(l(0).toLong, l(1).toLong, 1.0))

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
