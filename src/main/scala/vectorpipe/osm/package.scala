package vectorpipe

import java.io.{ FileInputStream, InputStream }

import scala.util.{ Failure, Success, Try }

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import vectorpipe.osm.internal.{ ElementToFeature => E2F }
import vectorpipe.util.Tree

// --- //

/** Types and functions unique to working with OpenStreetMap data. */
package object osm {

  type OSMFeature = Feature[Geometry, Tree[ElementData]]
  private[vectorpipe] type OSMPoint = Feature[Point, Tree[ElementData]]
  private[vectorpipe] type OSMLine = Feature[Line, Tree[ElementData]]
  private[vectorpipe] type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  private[vectorpipe] type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  /** Print any clipping errors to STDOUT. Less verbose than [[vectorpipe.VectorPipe.stdout]],
    * since only the offending element ID is printed instead of the entire metadata tree.
    *
    * The geometry itself is printed as LatLng GeoJSON, so that you can copy-paste it into
    * [[http://geojson.io geojson.io]].
    */
  def stdout[G <: Geometry](e: Extent, f: Feature[G, Tree[ElementData]]): Unit = {
    println(s"CLIP FAILURE W/ EXTENT: ${e}\nELEMENT ID: ${f.data.root.meta.id}\nGEOM: ${f.geom.reproject(WebMercator, LatLng).toGeoJson}")
  }

  /** Given a path to an OSM XML file, parse it into usable types. */
  def fromLocalXML(path: String)(implicit sc: SparkContext): Either[String, (RDD[Node], RDD[Way], RDD[Relation])] = {
    /* A byte stream, so as to not tax the heap */
    Try(new FileInputStream(path): InputStream).flatMap(xml => Element.elements.parse(xml)) match {
      case Failure(e) => Left(e.toString)
      case Success((ns, ws, rs)) =>
        Right((sc.parallelize(ns), sc.parallelize(ws), sc.parallelize(rs)))
    }
  }

  /** Given a path to an Apache ORC file containing OSM data, read out RDDs of each Element type.
    * If you want to read a file from S3, you must call [[vectorpipe.useS3]] first
    * to properly configure Hadoop to read your S3 credentials.
    */
  def fromORC(path: String)(implicit ss: SparkSession): Either[String, (RDD[(Long, Node)], RDD[(Long, Way)], RDD[Relation])] =
    Try(ss.read.orc(path)) match {
      case Failure(e) => Left(e.toString)
      case Success(data) => fromDataFrame(data)
    }

  def fromDataFrame(df: DataFrame)(implicit ss: SparkSession): Either[String, (RDD[(Long, Node)], RDD[(Long, Way)], RDD[Relation])] = {
    /* Necessary for the `map` transformation below to work */
    import ss.implicits._

    /* WARNING: Here be Reflection Dragons!
     * You may be look at this code and think: gee, that seems a bit verbose. You'd be right,
     * but that doesn't change what's necessary. The workings here are fairly brittle - things
     * might compile but fail mysteriously at runtime if anything is changed here (specifically regarding
     * the explicit type hand-holding).
     *
     * Changes made here might also be respected by demo code local to this library,
     * but then fail at runtime when published and used in a separate project.
     * How the `Member`s list below is handled is an example of this.
     * Moral of the story: avoid reflection and other runtime trickery.
     */

    // TODO: Allow for repartition as early as possible
    val nodes: RDD[(Long, Node)] =
      df
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
        .map({ case (lat, lon, meta, tags) => (meta.id, Node(lat, lon, ElementData(meta, tags, None))) })

    val ways: RDD[(Long, Way)] =
      df
        .select($"nds.ref".alias("nds"), $"id", $"user", $"uid", $"changeset", $"version", $"timestamp", $"tags")
        .where("type = 'way'")
        .map { row =>
          val nodes: Vector[Long] = row.getAs[Seq[Long]]("nds").toVector
          val tags: scala.collection.immutable.Map[String, String] =
            row.getAs[scala.collection.immutable.Map[String, String]]("tags")

          (nodes, metaFromRow(row), tags)
        }
        .rdd
        .map({ case (nodes, meta, tags) => (meta.id, Way(nodes, ElementData(meta, tags, None))) })

    val relations: RDD[Relation] =
      df
        .select($"members.type".alias("types"), $"members.ref".alias("refs"), $"members.role".alias("roles"), $"id", $"user", $"uid", $"changeset", $"version", $"timestamp", $"tags")
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
        .map { case (types, refs, roles, meta, tags) =>
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

          Relation(members, ElementData(meta, tags, None))
        }

    Right((nodes, ways, relations))
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
  def toFeatures(nodes: RDD[(Long, Node)], ways: RDD[(Long, Way)], relations: RDD[Relation]): RDD[OSMFeature] = {
    // TODO: Break this up into usable chunks. What if all I want is lines?

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
    // TODO: Use sc.union
    val pnt: RDD[OSMFeature] = points.map(identity)
    val lns: RDD[OSMFeature] = lines.map(identity)
    val pls: RDD[OSMFeature] = polys.map(identity)
    val mps: RDD[OSMFeature] = multiPolys.map(identity)

    nodes.sparkContext.union(pnt, lns, pls, mps)
  }

  def toFeatures2(nodes: RDD[(Long, Node)], ways: RDD[(Long, Way)], relations: RDD[Relation]) = {
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
    // TODO: Use sc.union
    val pnt: RDD[OSMFeature] = points.map(identity)
    val lns: RDD[OSMFeature] = lines.map(identity)
    val pls: RDD[OSMFeature] = polys.map(identity)
    val mps: RDD[OSMFeature] = multiPolys.map(identity)

    (pnt, lns, pls, mps)
  }

}
