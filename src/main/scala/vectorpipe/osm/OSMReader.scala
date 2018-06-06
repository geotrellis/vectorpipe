package vectorpipe.osm

import org.locationtech.geomesa.spark.jts._

import vectorpipe.osm.internal._
import vectorpipe.osm.internal.functions._

import geotrellis.vector.{Extent, Geometry => GTGeometry, Feature}

import com.vividsolutions.jts.geom._

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.net.URI


class OSMReader(
  val sourceDataFrame: DataFrame,
  targetExtent: Option[Extent]
)(implicit val ss: SparkSession) extends Serializable {

  import ss.implicits._

  private lazy val info = sourceDataFrame.select('id, 'uid, 'user)

  private lazy val ppnodes = ProcessOSM.preprocessNodes(sourceDataFrame, targetExtent)

  private lazy val ppways = ProcessOSM.preprocessWays(sourceDataFrame)

  private lazy val pprelations = ProcessOSM.preprocessRelations(sourceDataFrame)

  lazy val nodeGeoms = ProcessOSM.constructPointGeometries(ppnodes)

  lazy val wayGeoms = ProcessOSM.reconstructWayGeometries(ppways, ppnodes)

  lazy val relationGeoms =
    ProcessOSM.reconstructMultiPolygonRelationGeometries(pprelations, wayGeoms)
      .union(ProcessOSM.reconstructRouteRelationGeometries(pprelations, wayGeoms))

  lazy val allGeoms = {
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

  lazy val nodeFeaturesRDD: RDD[Feature[GTGeometry, ElementMeta]] =
    nodeGeoms
      .filter($"geom".isNotNull)
      .join(info, Seq("id"))
      .rdd
      .map { row =>
        Feature(row.getAs[Geometry]("geom"), createElementMeta(row))
      }

  lazy val wayFeaturesRDD: RDD[Feature[GTGeometry, ElementMeta]] =
    wayGeoms
      .filter($"geom".isNotNull)
      .join(info, Seq("id"))
      .rdd
      .map { row =>
        Feature(row.getAs[Geometry]("geom"), createElementMeta(row))
      }

  lazy val relationFeaturesRDD: RDD[Feature[GTGeometry, ElementMeta]] =
    relationGeoms
      .filter($"geom".isNotNull)
      .join(info, Seq("id"))
      .rdd
      .map { row =>
        Feature(row.getAs[Geometry]("geom"), createElementMeta(row))
      }
}


object OSMReader {
  def apply(sourceURI: URI)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), None)(ss)

  def apply(source: String)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(source), None)(ss)

  def apply(sourceURI: URI, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(sourceURI.toString), Some(targetExtent))(ss)

  def apply(source: String, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(ss.read.orc(source), Some(targetExtent))(ss)

  def apply(sourceDataFrame: DataFrame, targetExtent: Extent)(implicit ss: SparkSession): OSMReader =
    apply(sourceDataFrame, Some(targetExtent))(ss)

  def apply(
    sourceDataFrame: DataFrame,
    targetExtent: Option[Extent]
  )(implicit ss: SparkSession): OSMReader =
    new OSMReader(sourceDataFrame, targetExtent)(ss.withJTS)
}
