import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object LinkPrediction {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("POOP")
    val sc = new SparkContext(conf)
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "data/Amazon0302.txt")
  }
}
