package vectorpipe

import scala.collection.mutable.{Set => MSet}

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import org.apache.spark._
import org.apache.spark.rdd._
import vectorpipe.osm._
import vectorpipe.osm.internal.{ElementToFeature => E2F}

// --- //

object Vectorpipe {

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
  def toFeatures(rdd: RDD[Element]): RDD[OSMFeature] = {

    /* All Geometric OSM Relations.
     * A (likely false) assumption made in the `flatTree` function is that
     * Geometric Relations never appear in Relation Graphs. Therefore we can
     * naively grab them all here.
     */
    val geomRelations: RDD[Relation] = E2F.rawRelations(rdd).filter({ r =>
      r.data.tagMap.get("type") == Some("multipolygon")
    })

    // TODO Use the results on this!
    //val toDisseminate: ParSeq[(Long, Seq[ElementData])] = E2F.flatForest(E2F.relForest(rawRelations))

    val (points, rawLines, rawPolys) = E2F.geometries(E2F.rawNodes(rdd), E2F.rawWays(rdd))

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
  )(implicit sc: SparkContext): RDD[(SpatialKey, Iterable[OSMFeature])] = {

    val mt: MapKeyTransform = ld.mapTransform

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon()

    /* Filter once to reduce later workload */
    val bounded: RDD[OSMFeature] = rdd.filter(f => f.geom.intersects(extent))

    /* Associate each Feature with a SpatialKey */
    bounded.flatMap({ f =>
      val envelope: Extent = f.data._2
      val bounds: GridBounds = mt(envelope) /* Keys overlapping the Geom envelope */
      val gridEx: Extent = mt(bounds) /* Extent fitted to the key grid */
      val set: MSet[SpatialKey] = MSet.empty

      /* Undefined behaviour if used concurrently */
      val g: (Int, Int) => Unit = { (x, y) =>
        set += SpatialKey(bounds.colMin + x, bounds.rowMin + y)
      }

      /* Extend envelope to snap to the tile grid */
      val re = RasterExtent(gridEx, bounds.width, bounds.height)

      Rasterizer.foreachCellByGeometry(f.geom, re)(g)

      set.map(k => (k, f))
    }).groupByKey()
      .map({ case (k, iter) =>
        val kExt: Extent = mt(k)

        /* Clip each geometry in some way */
        (k, iter.map(g => clip(kExt, g)))
      })
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
  )(implicit sc: SparkContext): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    rdd.map({ case (k, iter) => (k, collate(mt(k), iter))})
  }
}
