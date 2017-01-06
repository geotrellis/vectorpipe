package vectorpipe

import java.io._

import scala.util.{Failure, Success, Try}

import geotrellis.spark.util.SparkUtils
import geotrellis.vector.io.json.Implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import vectorpipe.osm._

// --- //

object Vectorpipe extends App {

  /** Given a file of OSM XML data, convert it into GeoTrellis geometries. */
  def convert(sc: SparkContext, file: String): Either[String, Array[OSMFeature]] = {
    /* Open OSM data stream */
    val xml: InputStream = new FileInputStream(file)

    /* Parse OSM data */
    Element.elements.parse(xml) match {
      case Failure(e) => Left(e.toString)
      case Success((ns, ws, rs)) => {
        val nodes: RDD[Element] = sc.parallelize(ns)
        val ways: RDD[Element] = sc.parallelize(ws)
        val relations: RDD[Element] = sc.parallelize(rs)

        val elements: RDD[Element] = nodes ++ ways ++ relations
        val features: RDD[OSMFeature] = elements.toFeatures

        Try(features.collect) match {
          case Failure(e) => Left(e.toString)
          case Success(c) => Right(c)
        }
      }
    }

  }

  override def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext =
      SparkUtils.createLocalSparkContext("local[*]", "vectorpipe")

    /* Silence the damn INFO logger */
    Logger.getRootLogger().setLevel(Level.ERROR)

    val files = Seq(
      "data/8shapedmultipolygon.osm",
//      "data/heidelberger-schloss.osm",
      "data/quarry-rock.osm",
//      "data/yufuin.osm"
//      "data/baarle-nassau.osm",
      "data/india-pakistan.osm",
//      "data/queen-elizabeth-park.osm"
//      "data/north-van.osm",
//      "data/stanley-park.osm"
      "data/diomede.osm"
    )

    files.map(f => (f, convert(sc, f))).foreach({
      case (f, Left(e)) => println(s"${f} failed with: ${e}")
      case (f, Right(c)) => {
        println(s"${f} : ${c.length}")

        val json = c.map(_.geom).toTraversable.toGeoJson

        val bw = new BufferedWriter(new FileWriter(new File(f ++ ".json")))
        bw.write(json)
        bw.close()
      }
    })

    /* Safely shut down Spark */
    sc.stop()
  }
}
