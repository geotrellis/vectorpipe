package vectorpipe

import java.io.{FileInputStream, InputStream}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success}

import geotrellis.spark.util.SparkUtils
import org.apache.spark.SparkContext
import vectorpipe.osm._

// --- //

object Vectorpipe extends App {
  override def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext =
      SparkUtils.createLocalSparkContext("local[*]", "vectorpipe")

    /* Open OSM data stream */
    val xml: InputStream = new FileInputStream("8shapedmultipolygon.osm")

    /* Parse OSM data */
    Element.elements.parse(xml) match {
      case Failure(e) => println(e)
      case Success((ns, ws, rs)) => {
        val nodes: RDD[Element] = sc.parallelize(ns)
        val ways: RDD[Element] = sc.parallelize(ws)
        val relations: RDD[Element] = sc.parallelize(rs)

        val elements: RDD[Element] = nodes ++ ways ++ relations

        val c = elements.toFeatures.count()

        println(s"TOTAL FEATURES: ${c}")
      }
    }

    /* Safely shut down Spark */
    sc.stop()
  }
}
