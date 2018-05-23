package vectorpipe.osm

import org.locationtech.geomesa.spark.jts._

import vectorpipe.osm.internal._
import vectorpipe.osm.internal.functions._
import vectorpipe.osm.internal.util._

import geotrellis.vector.{Extent, Point => GTPoint, Polygon => GTPolygon, MultiPolygon => GTMultiPolygon, Line, Feature, GeomFactory}

import com.vividsolutions.jts.geom._

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.net.URI


class OSMReader(
  val sourceDataFrame: DataFrame,
  cacheDirectory: Option[URI],
  targetExtent: Option[Extent]
)(implicit val ss: SparkSession) extends Serializable {

  import ss.implicits._

  private implicit val cache: Caching =
    cacheDirectory match {
      case Some(dir) =>
        Option(dir.getScheme) match {
          case Some("s3") => Caching.onS3(dir.toString)
          case None if dir != "" => Caching.onFs(dir.toString)
          case _ => Caching.none
        }
      case None => Caching.none
    }

  private lazy val info = sourceDataFrame.select('id, 'uid, 'user)

  private lazy val ppnodes = cache.orc("prepared_nodes.orc") {
    ProcessOSM.preprocessNodes(sourceDataFrame, targetExtent)
  }

  private lazy val ppways = cache.orc("prepared_ways.orc") {
    ProcessOSM.preprocessWays(sourceDataFrame)
  }

  private lazy val pprelations = cache.orc("prepared_relations.orc") {
    ProcessOSM.preprocessRelations(sourceDataFrame)
  }

  lazy val nodeGeoms = cache.orc("node_geoms.orc") {
    ProcessOSM.constructPointGeometries(ppnodes)
  }

  lazy val wayGeoms = cache.orc("way_geoms.orc") {
    ProcessOSM.reconstructWayGeometries(ppways, ppnodes)
  }

  lazy val relationGeoms = cache.orc("relation_geoms.orc") {
    ProcessOSM.reconstructRelationGeometries(pprelations, wayGeoms)
  }

  lazy val allGeoms = cache.orc("all_geoms.orc") {
    import sourceDataFrame.sparkSession.implicits._

    nodeGeoms
      .withColumn("minorVersion", lit(0))
      .union(wayGeoms.where(size('tags) > 0)
      .drop('geometryChanged))
      .union(relationGeoms)
  }

  private def createElementMeta(row: Row): ElementMeta =
    ElementMeta(
      id = row.getAs[Long]("id"),
      user = row.getAs[String]("user"),
      uid = row.getAs[Long]("uid"),
      changeset = row.getAs[Long]("changeset"),
      version = row.getAs[Int]("version").toLong,
      minorVersion = row.getAs[Int]("minorVersion").toLong,
      timestamp = row.getAs[java.sql.Timestamp]("updated").toInstant,
      visible = row.getAs[Boolean]("visible"),
      tags = row.getAs[Map[String, String]]("tags")
    )

  lazy val pointFeaturesRDD: RDD[Feature[GTPoint, ElementMeta]] =
    nodeGeoms
      .join(info, Seq("id"))
      .filter($"geom".isNotNull)
      .rdd
      .map { row =>
        Feature(GTPoint(row.getAs[Point]("geom")), createElementMeta(row))
      }

  lazy val lineFeaturesRDD: RDD[Feature[Line, ElementMeta]] =
    wayGeoms
      .drop("geometryChanged")
      .union(relationGeoms)
      .join(info, Seq("id"))
      .filter { row =>
        row.getAs[Geometry]("geom") match {
          case l: LineString => true
          case _ => false
        }
      }
      .rdd
      .map { row =>
          Feature(Line(row.getAs[LineString]("geom")), createElementMeta(row))
      }

  lazy val polygonFeaturesRDD: RDD[Feature[GTPolygon, ElementMeta]] =
    relationGeoms
      .union(wayGeoms.drop("geometryChanged"))
      .join(info, Seq("id"))
      .filter { row =>
        row.getAs[Geometry]("geom") match {
          case p: Polygon => true
          case _ => false
        }
      }
      .rdd
      .map { row =>
        Feature(GTPolygon(row.getAs[Polygon]("geom")), createElementMeta(row))
      }

  lazy val multiPolygonFeaturesRDD: RDD[Feature[GTMultiPolygon, ElementMeta]] =
    relationGeoms
      .union(wayGeoms.drop("geometryChanged"))
      .join(info, Seq("id"))
      .filter { row =>
        row.getAs[Geometry]("geom") match {
          case _: MultiPolygon => true
          case _ => false
        }
      }
      .rdd
      .map { row =>
        Feature(GTMultiPolygon(row.getAs[MultiPolygon]("geom")), createElementMeta(row))
      }
}


object OSMReader {
  def apply(sourceURI: URI)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), None, None)(ss)

  def apply(sourceURI: URI, cacheDirectory: String)(implicit ss: SparkSession): OSMReader =
    apply(sourceURI, new URI(cacheDirectory))(ss)

  def apply(sourceURI: URI, cacheDirectory: String, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), Some(new URI(cacheDirectory)), Some(targetExtent))(ss)

  def apply(sourceURI: URI, cacheDirectory: URI)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), Some(cacheDirectory), None)(ss)

  def apply(sourceURI: URI, cacheDirectory: URI, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), Some(cacheDirectory), Some(targetExtent))(ss)

  def apply(sourceDataFrame: DataFrame, cacheDirectory: URI, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(sourceDataFrame, Some(cacheDirectory), Some(targetExtent))(ss)

  def apply(
    sourceDataFrame: DataFrame,
    cacheDirectory: Option[URI],
    targetExtent: Option[Extent]
  )(implicit ss: SparkSession): OSMReader =
    new OSMReader(sourceDataFrame, cacheDirectory, targetExtent)(ss.withJTS)
}
