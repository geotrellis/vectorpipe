package vectorpipe

import java.io.{FileInputStream, InputStream}

import scala.util.{Failure, Success}

import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import vectorpipe.osm.internal.{ElementToFeature => E2F}
import vectorpipe.util.Tree

// --- //

package object osm {

  import scala.util.Try


  type TagMap = Map[String, String]
  type OSMFeature = Feature[Geometry, Tree[ElementData]]
  type OSMPoint = Feature[Point, Tree[ElementData]]
  type OSMLine = Feature[Line, Tree[ElementData]]
  type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  /** Given a path to an OSM XML file, parse it into usable types. */
  def fromLocalXML(path: String)(implicit sc: SparkContext): Either[String, (RDD[Node], RDD[Way], RDD[Relation])] = {
    /* A byte stream, so as to not tax the heap */
    Try(new FileInputStream(path) : InputStream).flatMap(xml => Element.elements.parse(xml)) match {
      case Failure(e) => Left(e.toString)
      case Success((ns, ws, rs)) =>
        Right((sc.parallelize(ns), sc.parallelize(ws), sc.parallelize(rs)))
    }
  }

  /** Convert an RDD of raw OSM [[Element]]s into interpreted GeoTrellis
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
