package vectorpipe

import java.io.{ FileInputStream, InputStream }
import java.time.ZonedDateTime

import scala.util.{ Failure, Success, Try }
import scala.reflect.classTag

import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import vectorpipe.osm.internal.{ ElementToFeature => E2F }
import vectorpipe.util.Tree

// --- //

package object osm {

  type OSMFeature = Feature[Geometry, Tree[ElementData]]
  type OSMPoint = Feature[Point, Tree[ElementData]]
  type OSMLine = Feature[Line, Tree[ElementData]]
  type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  /** Given a path to an OSM XML file, parse it into usable types. */
  def fromLocalXML(path: String)(implicit sc: SparkContext): Either[String, (RDD[Node], RDD[Way], RDD[Relation])] = {
    /* A byte stream, so as to not tax the heap */
    Try(new FileInputStream(path): InputStream).flatMap(xml => Element.elements.parse(xml)) match {
      case Failure(e) => Left(e.toString)
      case Success((ns, ws, rs)) =>
        Right((sc.parallelize(ns), sc.parallelize(ws), sc.parallelize(rs)))
    }
  }

  /** Given a path to an Apache ORC file containing OSM data, read out RDDs of each Element type. */
  def fromLocalORC(path: String)(implicit hc: HiveContext): Either[String, (RDD[Node], RDD[Way], RDD[Relation])] = {
    /* Necessary for the `map` transformation below to work */
    import hc.sparkSession.implicits._

    /* WARNING: Here be Reflection Dragons!
     * You may be look at this code and think: gee, that seems a bit verbose. You'd be right,
     * but that doesn't change what's necessary. The workings here are fairly brittle - things
     * might compile but fail mysteriously if anything is changed here (specifically regarding
     * the explicit type hand-holding).
     */
    Try(hc.read.format("orc").load(path)) match {
      case Failure(e) => Left(e.toString)
      case Success(data) => {
        val nodes: RDD[Node] =
          data
            .select("lat", "lon", "id", "user", "uid", "changeset", "version", "timestamp", "tags")
            .where("type = 'node'")
            .map { row =>
              val lat: Double = row.getAs[java.math.BigDecimal]("lat").doubleValue()
              val lon: Double = row.getAs[java.math.BigDecimal]("lon").doubleValue()
              val tags: scala.collection.immutable.Map[String, String] =
                row.getAs[scala.collection.immutable.Map[String, String]]("tags")

              (lat, lon, metaFromRow(row), tags)
            }
            .rdd
            .map({ case (lat, lon, meta, tags) => Node(lat, lon, ElementData(meta, tags, Some(Right(Point(lon, lat))))) })

        val ways: RDD[Way] =
          data
            .select($"nds.ref".alias("nds"), $"id", $"user", $"uid", $"changeset", $"version", $"timestamp", $"tags")
            .where("type = 'way'")
            .map { row =>
              val nodes: Vector[Long] = row.getAs[Seq[Long]]("nds").toVector
              val tags: scala.collection.immutable.Map[String, String] =
                row.getAs[scala.collection.immutable.Map[String, String]]("tags")

              (nodes, metaFromRow(row), tags)
            }
            .rdd
            .map({ case (nodes, meta, tags) => Way(nodes, ElementData(meta, tags, None)) })

        val relations: RDD[Relation] =
          data
            .select("members", "id", "user", "uid", "changeset", "version", "timestamp", "tags")
            .where("type = 'relation'")
            .map { row =>
              val members: Seq[Member] = row.getAs[Seq[Member]]("members")
              val tags: scala.collection.immutable.Map[String, String] =
                row.getAs[scala.collection.immutable.Map[String, String]]("tags")

              (members, metaFromRow(row), tags)
            }
            .rdd 
            .map({ case (members, meta, tags) => Relation(members, ElementData(meta, tags, None)) })

        Right((nodes, ways, relations))
      }
    }
  }

  private def metaFromRow(row: Row): ElementMeta = {
    ElementMeta(
      row.getAs[Long]("id"),
      row.getAs[String]("user"),
      row.getAs[Long]("uid").toString, // TODO Use a `Long` in the datatype instead?
      row.getAs[Long]("changeset"),
      row.getAs[Long]("version"),
      row.getAs[java.sql.Timestamp]("timestamp").toString,
      true)
  }

  /**
   * Convert an RDD of raw OSM [[Element]]s into interpreted GeoTrellis
   * [[Feature]]s. In order to mix the various subtypes together, they've
   * been upcasted internally to [[Geometry]]. Note:
   * {{{
   * type OSMFeature = Feature[Geometry, Tree[ElementData]]
   * }}}
   *
   * ===Behaviour===
   * This algorithm aims to losslessly "sanitize" its input data,
   * in that it will break down malformed Relation structures, as
   * well as cull member references to Elements which no longer
   * exist (or exist outside the subset of data you're working
   * on). Mathematically speaking, there should exist a function
   * to reverse this conversion. This theoretical function and
   * `toFeatures` form an isomorphism if the source data is
   * correct. In other words, given:
   * {{{
   * parse: XML => RDD[Element]
   * toFeatures: RDD[Element] => RDD[OSMFeature]
   * restore: RDD[OSMFeature] => RDD[Element]
   * unparse: RDD[Element] => XML
   * }}}
   * then:
   * {{{
   * unparse(restore(toFeatures(parse(xml: XML))))
   * }}}
   * will yield a body of semantically correct OSM data.
   *
   * To achieve this sanity, the algorithm has the following behaviour:
   *   - Graphs of [[Relation]]s will be broken into spanning [[Tree]]s.
   *   - It doesn't make sense to represent non-multipolygon Relations as
   *     GeoTrellis `Geometry`s, so Relation metadata is disseminated
   *     across its child members. Otherwise, Relations are "dropped"
   *     from the output.
   */
  def toFeatures(nodes: RDD[Node], ways: RDD[Way], relations: RDD[Relation]): RDD[OSMFeature] = {

    /* All Geometric OSM Relations.
     * A (likely false) assumption made in the `flatTree` function is that
     * Geometric Relations never appear in Relation Graphs. Therefore we can
     * naively grab them all here.
     */
    val geomRelations: RDD[Relation] = relations.filter({ r =>
      r.data.tagMap.get("type") == Some("multipolygon")
    })

    // TODO Use the results on this!
    //val toDisseminate: ParSeq[(Long, Seq[ElementData])] = E2F.flatForest(E2F.relForest(rawRelations))

    val (points, rawLines, rawPolys) = E2F.geometries(nodes, ways)

    /* Depending on the dataset used, `Way` data may be incomplete. That is,
     * the local version of a Way may have fewer Node references that the original
     * as found on OpenStreetMap. These usually occur along "dataset bounding
     * boxes" found in OSM subregion extracts, where a Polygon is cut in half by
     * the BBOX. The resulting Polygons, with only a subset of the original Nodes,
     * are often self-intersecting. This causes Topology Exceptions during the
     * clipping stage of the pipeline. Our only recourse is to remove them here.
     *
     * See: https://github.com/geotrellis/vectorpipe/pull/16#issuecomment-290144694
     */
    val simplePolys = rawPolys.filter(_.geom.isValid)

    val (multiPolys, lines, polys) = E2F.multipolygons(rawLines, simplePolys, geomRelations)

    /* A trick to allow us to fuse the RDDs of various Geom types */
    val pnt: RDD[OSMFeature] = points.map(identity)
    val lns: RDD[OSMFeature] = lines.map(identity)
    val pls: RDD[OSMFeature] = polys.map(identity)
    val mps: RDD[OSMFeature] = multiPolys.map(identity)

    val geoms = pnt ++ lns ++ pls ++ mps

    /* Add every Feature's bounding envelope to its metadata */
    geoms.map({ f =>
      f.copy(data = f.data.copy(root = f.data.root.copy(extra = Some(Left(f.geom.envelope)))))
    })
  }

}
