package vectorpipe

import java.io.{FileInputStream, InputStream}

import scala.util.{Failure, Success}

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import vectorpipe.osm._
import vectorpipe.osm.internal.{ElementToFeature => E2F}

// --- //

/** VectorPipe is a library for mass conversion of OSM data into Mapbox
  * VectorTiles. It is powered by [[https://github.com/locationtech/geotrellis
  * GeoTrellis]] and [[https://spark.apache.org Apache Spark]].
  *
  * ==Usage==
  * GeoTrellis and Spark do most of our work for us. Writing a `main` that
  * uses VectorPipe need not contain much more than:
  * {{{
  * import vectorpipe.{ VectorPipe => VP }
  * import vectorpipe.osm._               /* For associated types */
  * import vectorpipe.geom.Clip           /* How should we clip Geometries? */
  * import vectorpipe.vectortile.Collate  /* How should we organize Features into VT Layers? */
  *
  * val layout: LayoutDefinition = ...
  * ... // TODO dealing with ORC
  * val (nodes, ways, relations): (RDD[Node], RDD[Way], RDD[Relation]) = ...
  * val features: RDD[OSMFeature] = VP.toFeatures(nodes, ways, relations)
  * val featGrid: RDD[(SpatialKey, Iterable[OSMFeature])] = VP.toGrid(Clip.byHybrid, layout, features)
  * val tileGrid: RDD[(SpatialKey, VectorTile)] = VP.toVectorTile(Collate.byAnalytics, layout, featGrid)
  *
  * /* How should a `SpatialKey` map to a filepath on S3? */
  * val s3PathFromKey: SpatialKey => String = SaveToS3.spatialKeyToPath(
  *   LayerId("sample", 1),  /* Whatever zoom level it is */
  *   "s3://some-bucket/catalog/{name}/{z}/{x}/{y}.mvt"
  * )
  *
  * tileGrid.saveToS3(s3PathFromKey)
  * }}}
  */
object VectorPipe {

  /** Given a path to an OSM XML file, parse it into usable types. */
  def fromLocalXML(path: String)(implicit sc: SparkContext): Either[String, (RDD[Node], RDD[Way], RDD[Relation])] = {
    /* A byte stream, so as to not tax the heap */
    val xml: InputStream = new FileInputStream(path)

    /* Parse the OSM data */
    Element.elements.parse(xml) match {
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

    val (multiPolys, lines, polys) = E2F.multipolygons(rawLines, rawPolys, geomRelations)

    /* Pair each Geometry with its bounding envelope. The envelope will be
     * stored in VectorTile Feature metadata, and can be used to aid in the
     * Geometry restitching process during analytics.
     */
    val pnt: RDD[OSMFeature] = points.map(f => f.copy(data = (f.data, f.geom.envelope)))
    val lns: RDD[OSMFeature] = lines.map(f => f.copy(data = (f.data, f.geom.envelope)))
    val pls: RDD[OSMFeature] = polys.map(f => f.copy(data = (f.data, f.geom.envelope)))
    val mps: RDD[OSMFeature] = multiPolys.map(f => f.copy(data = (f.data, f.geom.envelope)))

    pnt ++ lns ++ pls ++ mps
  }

  /** Given a particular Layout (tile grid), split a collection of [[OSMFeature]]s
    * into a grid of them indexed by [[SpatialKey]].
    *
    * ==Clipping Strategies==
    *
    * A clipping strategy defines how Geometries which stretch outside their
    * associated bounding box should be reduced to better fit it. This is
    * benefical, as it saves on storage for large, complex Geometries who
    * only partially intersect some bounding box. The excess points will be
    * cut out, but the "how" is a matter of weighing PROs and CONs in the
    * context of the user's use-case. Several strategies come to mind:
    *
    *   - Clip directly on the bounding box
    *   - Clip just outside the bounding box
    *   - Keep the nearest Point outside the bounding box, wherever it is
    *   - Custom clipping for each OSM Element type (building, etc)
    *   - Don't clip
    *
    * These clipping strategies are defined in [[vectorpipe.geom.Clip]],
    * where you can find further explanation.
    *
    * @param ld   The LayoutDefinition defining the area to gridify.
    * @param clip A function which represents a "clipping strategy".
    * @return The pair `(OSMFeature, Extent)` represents the clipped Feature
    *         along with its __original__ bounding envelope. This envelope
    *         is to be encoded into the Feature's metadata within a VT,
    *         and could be used later to aid the reconstruction of the original,
    *         unclipped Feature.
    */
  def toGrid(
    clip: (Extent, OSMFeature) => OSMFeature,
    ld: LayoutDefinition,
    rdd: RDD[OSMFeature]
  ): RDD[(SpatialKey, Iterable[OSMFeature])] = {

    val mt: MapKeyTransform = ld.mapTransform

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon

    /* Filter once to reduce later workload */
    // TODO: This may be an unnecessary bottleneck.
    val bounded: RDD[OSMFeature] = rdd.filter(f => f.geom.intersects(extent))

    /* Associate each Feature with a SpatialKey */
    val grid: RDD[(SpatialKey, OSMFeature)] = bounded.flatMap(f => byIntersect(mt, f))

    grid.groupByKey().map({ case (k, iter) =>
      val kExt: Extent = mt(k)

      /* Clip each geometry in some way */
      (k, iter.map(g => clip(kExt, g)))
    })
  }

  /* Takes advantage of the fact that most Geoms are small, and only occur in one Tile */
  private def byIntersect(mt: MapKeyTransform, f: OSMFeature): Iterator[(SpatialKey, OSMFeature)] = {
    val b: GridBounds = mt(f.data._2)

    val keys: Iterator[SpatialKey] = for {
      x <- Iterator.range(b.colMin, b.colMax+1)
      y <- Iterator.range(b.rowMin, b.rowMax+1)
    } yield SpatialKey(x, y)

    keys.filter(k => mt(k).toPolygon.intersects(f.geom))
      .map(k => (k, f))
  }

  /** Given a collection of GeoTrellis `Feature`s which have been associated
    * with some `SpatialKey` and a "collation" function, form those `Feature`s
    * into a `VectorTile`.
    *
    * @see [[vectorpipe.vectortile.Collate]]
    */
  def toVectorTile(
    collate: (Extent, Iterable[OSMFeature]) => VectorTile,
    ld: LayoutDefinition,
    rdd: RDD[(SpatialKey, Iterable[OSMFeature])]
  ): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    rdd.map({ case (k, iter) => (k, collate(mt(k), iter))})
  }
}
