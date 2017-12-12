package vectorpipe

import java.io.{ FileInputStream, InputStream }

import scala.util.Try

import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import vectorpipe.osm.internal.PlanetHistory

// --- //

/** Types and functions unique to working with OpenStreetMap data.
  *
  * @groupname elements Reading OSM Elements
  * @groupprio elements 0
  *
  * @groupname conversion Element Conversion
  * @groupdesc conversion Converting OSM Elements into GeoTrellis geometries.
  * @groupprio conversion 1
  */
package object osm {

  type OSMFeature = Feature[Geometry, ElementMeta]
  private[vectorpipe] type OSMPoint = Feature[Point, ElementMeta]
  private[vectorpipe] type OSMLine = Feature[Line, ElementMeta]
  private[vectorpipe] type OSMPolygon = Feature[Polygon, ElementMeta]
  private[vectorpipe] type OSMMultiPoly = Feature[MultiPolygon, ElementMeta]

  /** Given a path to an OSM XML file, parse it into usable types.
    *
    * @group elements
    */
  def fromLocalXML(
    path: String
  )(implicit sc: SparkContext): Try[(RDD[(Long, Node)], RDD[(Long, Way)], RDD[(Long, Relation)])] = {
    /* A byte stream, so as to not tax the heap */
    Try(new FileInputStream(path): InputStream)
      .flatMap(xml => Element.elements.parse(xml))
      .map { case (ns, ws, rs) => (sc.parallelize(ns), sc.parallelize(ws), sc.parallelize(rs)) }
  }

  /** Given a path to an Apache ORC file containing OSM data, read out RDDs of each Element type.
    *
    * @group elements
    */
  def fromORC(
    path: String
  )(implicit ss: SparkSession): Try[(RDD[(Long, Node)], RDD[(Long, Way)], RDD[(Long, Relation)])] = {
    Try(ss.read.orc(path)).map(fromDataFrame(_))
  }

  /** Given a [[DataFrame]] that follows [[https://github.com/mojodna/osm2orc#schema this table schema]],
    * read out RDDs of each [[Element]] type.
    *
    * @group elements
    */
  def fromDataFrame(data: DataFrame): (RDD[(Long, Node)], RDD[(Long, Way)], RDD[(Long, Relation)]) = {
    /* WARNING: Here be Reflection Dragons!
     * You may be look at the methods below and think: gee, that seems a bit verbose. You'd be right,
     * but that doesn't change what's necessary. The workings here are fairly brittle - things
     * might compile but fail mysteriously at runtime if anything is changed here (specifically regarding
     * the explicit type hand-holding).
     *
     * Changes made here might also be respected by demo code local to this library,
     * but then fail at runtime when published and used in a separate project.
     * How the `Member`s list below is handled is an example of this.
     * Moral of the story: avoid reflection and other runtime trickery.
     */
    (allNodes(data), allWays(data), allRelations(data))
  }

  /** Collect all the Nodes that exist in the given DataFrame. */
  private[this] def allNodes(data: DataFrame): RDD[(Long, Node)] = {
    import data.sparkSession.implicits._

    data
      .select("lat", "lon", "id", "user", "uid", "changeset", "version", "timestamp", "visible", "tags")
      .where("type = 'node'")
      .map { row =>
        /* ASSUMPTION: 0 and 1 here assume the order of the `select` terms won't change! */
        val lat: Double = if (row.isNullAt(0)) 0 else row.getAs[java.math.BigDecimal]("lat").doubleValue()
        val lon: Double = if (row.isNullAt(1)) 0 else row.getAs[java.math.BigDecimal]("lon").doubleValue()
        val tags: scala.collection.immutable.Map[String, String] =
          row.getAs[scala.collection.immutable.Map[String, String]]("tags")

        (lat, lon, metaFromRow(row), tags)
      }
      .rdd
      .map { case (lat, lon, rawMeta, tags) =>
        val meta: ElementMeta = makeMeta(rawMeta, tags)

        (meta.id, Node(lat, lon, meta))
      }
  }

  /** Collect all the Ways that exist in the given DataFrame. */
  private[this] def allWays(data: DataFrame): RDD[(Long, Way)] = {
    import data.sparkSession.implicits._

    data
      .select($"nds.ref".alias("nds"), $"id", $"user", $"uid", $"changeset", $"version", $"timestamp", $"visible", $"tags")
      .where("type = 'way'")
      .map { row =>
        val nodes: Vector[Long] = row.getAs[Seq[Long]]("nds").toVector
        val tags: scala.collection.immutable.Map[String, String] =
          row.getAs[scala.collection.immutable.Map[String, String]]("tags")

        (nodes, metaFromRow(row), tags)
      }
      .rdd
      .map { case (nodes, rawMeta, tags) =>
        val meta: ElementMeta = makeMeta(rawMeta, tags)

        (meta.id, Way(nodes, meta))
      }
  }

  /** Collect all the Relations that exist in the given DataFrame. */
  private[this] def allRelations(data: DataFrame): RDD[(Long, Relation)] = {
    import data.sparkSession.implicits._

    data
      .select($"members.type".alias("types"), $"members.ref".alias("refs"), $"members.role".alias("roles"), $"id", $"user", $"uid", $"changeset", $"version", $"timestamp", $"visible", $"tags")
      .where("type = 'relation'")
      .map { row =>

        /* ASSUMPTION: These three lists respect their original ordering as they
         * were stored in Orc STRUCTs. i.e. the first element of each list were
         * from the same STRUCT, as were those from the second, and third, etc.
         */
        val types: Seq[String] = row.getAs[Seq[String]]("types")
        val refs: Seq[Long] = row.getAs[Seq[Long]]("refs")
        val roles: Seq[String] = row.getAs[Seq[String]]("roles")

        val tags: scala.collection.immutable.Map[String, String] =
          row.getAs[scala.collection.immutable.Map[String, String]]("tags")

        (types, refs, roles, metaFromRow(row), tags)
      }
      .rdd
      .map { case (types, refs, roles, rawMeta, tags) =>
        /* Scala has no `zip3` or `zipWith`, so we have to combine these three Seqs
         * somewhat inefficiently. This line really needs improvement.
         *
         * The issue here is that reflection can't figure out how to read a `Seq[Member]`
         * from a `Row`, but _only when vectorpipe is used in another project_. Demo code
         * local to this project will work just fine. The exception thrown is:
         *
         * java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
         * cannot be cast to vectorpipe.osm.Member
         *
         * This was supposed to have been fixed in an earlier version of Spark,
         * and there is little mention of the issue in general on the internet.
         */
        val members: Seq[Member] =
          types.zip(refs).zip(roles).map { case ((ty, rf), ro) => Member(ty, rf, ro) }

        val meta: ElementMeta = makeMeta(rawMeta, tags)

        (meta.id, Relation(members.toList, meta))
      }
  }

  /** An unfortunate necessity to avoid reflection errors involving `java.time.Instant` */
  private[this] def makeMeta(m: (Long, String, Long, Long, Long, Long, Boolean), tags: Map[String, String]): ElementMeta =
    ElementMeta(m._1, m._2, m._3, m._4, m._5, 0, java.time.Instant.ofEpochMilli(m._6), m._7, tags)

  private[this] def metaFromRow(row: Row): (Long, String, Long, Long, Long, Long, Boolean) = {
    (
      row.getAs[Long]("id"),
      row.getAs[String]("user"),
      row.getAs[Long]("uid"),
      row.getAs[Long]("changeset"),
      row.getAs[Long]("version"),
      row.getAs[java.sql.Timestamp]("timestamp").toInstant.toEpochMilli,
      row.getAs[Boolean]("visible")
    )
  }

  /** All Lines and Polygons that could be reconstructed from a set of all OSM Elements.
    *
    * @group conversion
    */
  def features(ns: RDD[(Long, Node)], ws: RDD[(Long, Way)], rs: RDD[(Long, Relation)]): Features = {
    PlanetHistory.features(ns, ws)
  }
}
