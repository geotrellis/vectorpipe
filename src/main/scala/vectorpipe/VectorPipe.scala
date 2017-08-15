package vectorpipe

import geotrellis.raster.GridBounds
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import org.apache.spark.rdd._

// --- //

/** VectorPipe is a library for mass conversion of OSM data into Mapbox
  * VectorTiles. It is powered by [[https://github.com/locationtech/geotrellis
  * GeoTrellis]] and [[https://spark.apache.org Apache Spark]].
  *
  * ==Outline==
  * GeoTrellis and Spark do most of our work for us. Writing a `main`
  * function that uses VectorPipe need not contain much more than:
  * {{{
  * import vectorpipe._
  *
  * val layout: LayoutDefinition =
  *   ZoomedLayoutScheme.layoutForZoom(15, WebMercator.worldExtent, 512)
  *
  * ... // TODO dealing with ORC
  *
  * val (nodes, ways, relations): (RDD[osm.Node], RDD[osm.Way], RDD[osm.Relation]) = ...
  *
  * val features: RDD[OSMFeature] =
  *   osm.toFeatures(nodes, ways, relations)
  *
  * val featGrid: RDD[(SpatialKey, Iterable[OSMFeature])] =
  *   VectorPipe.toGrid(Clip.byHybrid, layout, features)
  *
  * val tiles: RDD[(SpatialKey, VectorTile)] =
  *   VectorPipe.toVectorTile(Collate.byAnalytics, layout, featGrid)
  * }}}
  * The `tiles` RDD could then be used as a GeoTrellis tile layer as
  * needed.
  *
  * ==Writing Portable Tiles==
  * This method outputs VectorTiles to a directory structure appropriate for
  * serving by a Tile Map Server. The VTs themselves are saved in the usual
  * `.mvt` format, and so can be read by any other tool. The example that
  * follows writes `tiles` from above to an S3 bucket:
  * {{{
  * import geotrellis.spark.io.s3._  // requires the `geotrellis-s3` library
  *
  * /* How should a `SpatialKey` map to a filepath on S3? */
  * val s3PathFromKey: SpatialKey => String = SaveToS3.spatialKeyToPath(
  *   LayerId("sample", 1),  // Whatever zoom level it is
  *   "s3://some-bucket/catalog/{name}/{z}/{x}/{y}.mvt"
  * )
  *
  * tiles.saveToS3(s3PathFromKey)
  * }}}
  *
  * ==Writing a GeoTrellis Layer of VectorTiles==
  * The disadvantage of the "Portable Tiles" approach is that there is no
  * way to read the tiles back into a `RDD[(SpatialKey, VectorTile)]` and do
  * Spark-based manipulation operations. To do that, the tiles have to be
  * written as a "GeoTrellis Layer" from the get-go. The output of such a write
  * are split and compressed files that aren't readable by other tools. This
  * method compresses VectorTiles to about half the size of a normal `.mvt`.
  * {{{
  * import geotrellis.spark._
  * import geotrellis.spark.io._
  * import geotrellis.spark.io.file._    /* When writing to your local computer */
  *
  * /* IO classes */
  * val catalog: String = "/home/you/tiles/"  /* This must exist ahead of time! */
  * val store = FileAttributeStore(catalog)
  * val writer = FileLayerWriter(store)
  *
  * /* Dynamically determine the KeyBounds */
  * val bounds: KeyBounds[SpatialKey] =
  *   tiles.map({ case (key, _) => KeyBounds(key, key) }).reduce(_ combine _)
  *
  * /* Construct metadata for the Layer */
  * val meta = LayerMetadata(layout, bounds)
  *
  * /* Write the Tile Layer */
  * writer.write(LayerId("north-van", 15), ContextRDD(tiles, meta), ZCurveKeyIndexMethod)
  * }}}
  */
object VectorPipe {

  /** Given a particular Layout (tile grid), split a collection of [[Feature]]s
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
  def toGrid[G <: Geometry, D](
    clip: (Extent, Feature[G, D]) => Feature[G, D],
    ld: LayoutDefinition,
    rdd: RDD[Feature[G, D]]
  ): RDD[(SpatialKey, Iterable[Feature[G, D]])] = {

    val mt: MapKeyTransform = ld.mapTransform

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon

    /* Filter once to reduce later workload */
    // TODO: This may be an unnecessary bottleneck.
    // val bounded: RDD[Feature[G, D]] = rdd.filter(f => f.geom.intersects(extent))

    /* Associate each Feature with a SpatialKey */
    val grid: RDD[(SpatialKey, Feature[G, D])] = rdd.flatMap(f => byIntersect(mt, f))

    grid.groupByKey().map({ case (k, iter) =>
      val kExt: Extent = mt(k)

      /* Clip each geometry in some way */
      (k, iter.map(g => clip(kExt, g)))
    })
  }

  /* Takes advantage of the fact that most Geoms are small, and only occur in one Tile */
  private def byIntersect[G <: Geometry, D](
    mt: MapKeyTransform,
    f: Feature[G, D]
  ): Iterator[(SpatialKey, Feature[G, D])] = {

    val b: GridBounds = mt(f.geom.envelope)

    /* For most Geoms, this will only produce one [[SpatialKey]]. */
    val keys: Iterator[SpatialKey] = for {
      x <- Iterator.range(b.colMin, b.colMax+1)
      y <- Iterator.range(b.rowMin, b.rowMax+1)
    } yield SpatialKey(x, y)

    /* Yes, this filter is necessary, since the Geom's _envelope_ could intersect
     * grid cells that the Geom itself does not. Not excluding these false positives
     * could lead to undefined behaviour later in the clipping step.
     */
    keys.filter(k => mt(k).toPolygon.intersects(f.geom)).map(k => (k, f))
  }

  /** Given a collection of GeoTrellis `Feature`s which have been associated
    * with some `SpatialKey` and a "collation" function, form those `Feature`s
    * into a `VectorTile`.
    *
    * @see [[vectorpipe.vectortile.Collate]]
    */
  def toVectorTile[G <: Geometry, D](
    collate: (Extent, Iterable[Feature[G, D]]) => VectorTile,
    ld: LayoutDefinition,
    rdd: RDD[(SpatialKey, Iterable[Feature[G, D]])]
  ): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    rdd.map({ case (k, iter) => (k, collate(mt(k), iter))})
  }
}
