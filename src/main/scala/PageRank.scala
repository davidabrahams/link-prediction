import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object PageRank {

  case class WebPage(id: String, url: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("POOP")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", args(0))
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", args(1))

//    val vertex_data: RDD[String] = sc.textFile("data/shorter_vertices.txt")
    val vertex_data: RDD[String] = sc.textFile("s3n://dcomp-pagerank/shorter_vertices.txt")
    val pages: RDD[WebPage] = vertex_data.map(_.split(' ')).
      map(line => WebPage(line(0).trim, line(1).trim))
    val vertices: RDD[(VertexId, String)] = pages.map(a => (a.id.toLong, a.url))

    vertices.take(10).foreach(println)

    val edge_data: RDD[String] = sc.textFile("s3n://dcomp-pagerank/shorter_edges.txt")
    val edges: RDD[Edge[Double]] = edge_data.map(_.split(' ')).map(l => Edge(l(0).toLong, l(1).toLong, 1.0))

    val graph = Graph(vertices, edges, "")

    val prBerkeley = graph.staticPageRank(5).cache
    graph.outerJoinVertices(prBerkeley.vertices) {
      (v, title, r) => (r.getOrElse(0.0), title)
    }.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))

  }
}
