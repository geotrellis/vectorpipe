package vectorpipe

import geotrellis.proj4._
import geotrellis.raster.GridBounds
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile.VectorTile
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
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
    * @param logError An IO function that will log any clipping failures.
    */
  def toGrid[G <: Geometry, D](
    clip: (Extent, Feature[G, D], (Extent, G) => Boolean) => Option[Feature[G, D]],
    logError: (Extent, Feature[G, D]) => Unit,
    ld: LayoutDefinition,
    rdd: RDD[Feature[G, D]],
    partitioner: Partitioner
  ): RDD[(SpatialKey, Iterable[Feature[G, D]])] = {

    val mt: MapKeyTransform = ld.mapTransform

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon

    /* Filter once to reduce later workload */
    // TODO: This may be an unnecessary bottleneck.
    // val bounded: RDD[Feature[G, D]] = rdd.filter(f => f.geom.intersects(extent))

    // TO NOTE: I moved the clip to before the shuffle, which cuts down on shuffle for larger
    // geometries.

    /* Associate each Feature with a SpatialKey */
    val grid: RDD[(SpatialKey, Feature[G, D])] = rdd.flatMap(f => byIntersect(mt, f, clip, logError))

    grid.groupByKey(partitioner = partitioner)
  }

  // TODO: Clean up, would be nice to have in GeoTrellis proper
  import geotrellis.raster._
  import geotrellis.raster.rasterize.{Rasterizer, Callback}
  import geotrellis.raster.{GridBounds, RasterExtent, PixelIsArea}
  import java.util.concurrent.ConcurrentHashMap
  import collection.JavaConverters._
  private def spatialKeysForMultiLine(multiLine: MultiLine, mt: MapKeyTransform): Iterator[SpatialKey] = {
    val extent = multiLine.envelope
    val bounds: GridBounds = mt(extent)
    val options = Rasterizer.Options(includePartial=true, sampleType=PixelIsArea)

    val boundsExtent: Extent = mt(bounds)
    val rasterExtent = RasterExtent(boundsExtent, bounds.width, bounds.height)

    /*
     * Use the Rasterizer to construct  a list of tiles which meet
     * the  query polygon.   That list  of tiles  is stored  as an
     * array of  tuples which  is then  mapped-over to  produce an
     * array of KeyBounds.
     */
    val tiles = new ConcurrentHashMap[(Int,Int), Unit]
    val fn = new Callback {
      def apply(col : Int, row : Int): Unit = {
        val tile : (Int, Int) = (bounds.colMin + col, bounds.rowMin + row)
        tiles.put(tile, Unit)
      }
    }

    multiLine.foreach(rasterExtent, options)(fn)
    tiles.keys.asScala.map { case (col, row) => SpatialKey(col, row) }
  }

  private def spatialKeysForMultiPolygon(multiPolygon: MultiPolygon, mt: MapKeyTransform): Iterator[SpatialKey] = {
    val extent = multiPolygon.envelope
    val bounds: GridBounds = mt(extent)
    val options = Rasterizer.Options(includePartial=true, sampleType=PixelIsArea)
    val boundsExtent: Extent = mt(bounds)
    val rasterExtent = RasterExtent(boundsExtent, bounds.width, bounds.height)

    /*
     * Use the Rasterizer to construct  a list of tiles which meet
     * the  query polygon.   That list  of tiles  is stored  as an
     * array of  tuples which  is then  mapped-over to  produce an
     * array of KeyBounds.
     */
    val tiles = new ConcurrentHashMap[(Int,Int), Unit]
    val fn = new Callback {
      def apply(col : Int, row : Int): Unit = {
        val tile : (Int, Int) = (bounds.colMin + col, bounds.rowMin + row)
        tiles.put(tile, Unit)
      }
    }

    multiPolygon.foreach(rasterExtent, options)(fn)
    tiles.keys.asScala.map { case (col, row) => SpatialKey(col, row) }
  }


  /* Takes advantage of the fact that most Geoms are small, and only occur in one Tile */
  def byIntersect[G <: Geometry, D](
    mt: MapKeyTransform,
    f: Feature[G, D],
    clip: (Extent, Feature[G, D], (Extent, G) => Boolean) => Option[Feature[G, D]],
    logError: (Extent, Feature[G, D]) => Unit
  ): Iterator[(SpatialKey, Feature[G, D])] = {

    def clipFeature(k: SpatialKey, f: Feature[G, D], contains: (Extent, G) => Boolean): Option[(SpatialKey, Feature[G, D])] = {
      val kExt = mt(k)
      clip(kExt, f, contains) match {
        case Some(h) => Some(k -> h)
        case None => logError(kExt, f); None /* Sneaky IO to log the error */
      }
    }

    // TODO: DRY out
    // TODO: Better way to work around GeometryCollection results of clipping
    val iterator =
      f.geom match {
        case p: Point =>
          val k = mt(p)
          Iterator((k, f))
        case mp: MultiPoint =>
          mp.points
            .map(mt(_))
            .distinct
            .map(clipFeature(_, f, { (x, y) => true }))
            .flatten
            .iterator
        case l: Line =>
          val keys = spatialKeysForMultiLine(MultiLine(l), mt).toArray

          val len = keys.length
          var continue = true
          var i = 0
          val elems = scala.collection.mutable.ListBuffer[(SpatialKey, Feature[G, D])]()
          val startTime = System.currentTimeMillis
          while(i < len && continue) {
            // How many will 5 seconds cut out? let's see!
            if(System.currentTimeMillis - startTime > 5000) {
              continue = false
            } else {
              val k = keys(i)
              val kExt = mt(k)
              clip(kExt, f, { (x,y) => false }) match {
                case Some(f) => elems += k -> f
                case None => continue = false
              }
            }
            i += 1
          }
          if(!continue) { logError(Extent(0, 0, 1, 1), f) }

          elems.iterator

//          spatialKeysForMultiLine(MultiLine(l), mt).map(clipFeature(_, f, { (x,y) => false })).flatten
        case ml: MultiLine =>
          val keys = spatialKeysForMultiLine(ml, mt).toArray

          val len = keys.length
          var continue = true
          var i = 0
          val elems = scala.collection.mutable.ListBuffer[(SpatialKey, Feature[G, D])]()
          val startTime = System.currentTimeMillis
          while(i < len && continue) {
            // How many will 5 seconds cut out? let's see!
            if(System.currentTimeMillis - startTime > 5000) {
              continue = false
            } else {
              val k = keys(i)
              val kExt = mt(k)
              clip(kExt, f, { (x,y) => false }) match {
                case Some(f) => elems += k -> f
                case None => continue = false
              }
            }
            i += 1
          }
          if(!continue) { logError(Extent(0, 0, 1, 1), f) }

          elems.iterator

          //.map(clipFeature(_, f, { (x,y) => false })).flatten
        case p: Polygon =>
          val pg = PreparedGeometryFactory.prepare(p.jtsGeom)
          val contains = { (extent: Extent, geom: G) =>
            pg.contains(extent.toPolygon.jtsGeom)
          }

          val keys = spatialKeysForMultiPolygon(MultiPolygon(p), mt).toArray

          // TODO: What to do about this
          val len = keys.length
          var continue = true
          var i = 0
          val elems = scala.collection.mutable.ListBuffer[(SpatialKey, Feature[G, D])]()
          val startTime = System.currentTimeMillis
          while(i < len && continue) {
            // How many will 5 seconds cut out? let's see!
            if(System.currentTimeMillis - startTime > 5000) {
              continue = false
            } else {
              val k = keys(i)
              val kExt = mt(k)
              clip(kExt, f, contains) match {
                case Some(f) => elems += k -> f
                case None => continue = false
              }
            }
            i += 1
          }
          if(!continue) { logError(Extent(0, 0, 1, 1), f) }

          elems.iterator
  //        spatialKeysForMultiPolygon(MultiPolygon(p), mt).map(clipFeature(_, f, contains)).flatten
        case mp: MultiPolygon =>
          val pg = PreparedGeometryFactory.prepare(mp.jtsGeom)
          val contains = { (extent: Extent, geom: G) =>
            pg.contains(extent.toPolygon.jtsGeom)
          }

          val keys = spatialKeysForMultiPolygon(mp, mt).toArray

          // TODO: What to do about this
          val len = keys.length
          var continue = true
          var i = 0
          val elems = scala.collection.mutable.ListBuffer[(SpatialKey, Feature[G, D])]()
          val startTime = System.currentTimeMillis
          while(i < len && continue) {
            // How many will 5 seconds cut out? let's see!
            if(System.currentTimeMillis - startTime > 5000) {
              continue = false
            } else {
              val k = keys(i)
              val kExt = mt(k)
              clip(kExt, f, contains) match {
                case Some(f) => elems += k -> f
                case None => continue = false
              }
            }
            i += 1
          }
          if(!continue) { logError(Extent(0, 0, 1, 1), f) }

          elems.iterator

          //spatialKeysForMultiPolygon(mp, mt).map(clipFeature(_, f, contains)).flatten
        case gc: GeometryCollection =>
          // How did we get one of these? In case it does happen, use generic case.
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
          keys.filter(k => mt(k).toPolygon.intersects(f.geom))
              .map(k => clipFeature(k, f, { (x,y) => false })).flatten
      }

    iterator.flatMap { case (k, f) =>
      f.geom match {
        case gc: GeometryCollection =>
          val validGeoms: Seq[Geometry] =
            gc.points ++ gc.lines ++ gc.polygons ++
            gc.multiPoints ++ gc.multiLines ++ gc.multiPolygons
          validGeoms.map { g => k -> f.mapGeom { _ => g.asInstanceOf[G] } } // TODO: BOOOOOOO
        case _ =>
          Seq(k -> f)
      }
    }
  }

  /** Given a collection of GeoTrellis `Feature`s which have been associated
    * with some `SpatialKey` and a "collation" function, form those `Feature`s
    * into a `VectorTile`.
    *
    * @see [[vectorpipe.Collate]]
    */
  def toVectorTile[G <: Geometry, D](
    collate: (Extent, Iterable[Feature[G, D]]) => VectorTile,
    ld: LayoutDefinition,
    rdd: RDD[(SpatialKey, Iterable[Feature[G, D]])]
  ): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    rdd.mapPartitions({ partition =>
      partition.map { case (k, iter) => (k, collate(mt(k), iter)) }
    }, preservesPartitioning = true)
  }

  private def logString[G <: Geometry, D](e: Extent, f: Feature[G, D]): String =
    s"CLIP FAILURE W/ EXTENT: ${e}\nELEMENT METADATA: ${f.data}\nGEOM: ${f.geom.reproject(WebMercator, LatLng).toGeoJson}"

  /** Print any clipping errors to STDOUT - to be passed to [[toGrid]]. */
  def stdout[G <: Geometry, D](e: Extent, f: Feature[G, D]): Unit = println(logString(e, f))

  /** Log a clipping error as an ERROR through Spark's default log4j - to be passed to [[toGrid]]. */
  def log4j[G <: Geometry, D](e: Extent, f: Feature[G, D]): Unit =
    Logger.getRootLogger().error(logString(e, f))

  /** Don't log clipping failures - to be passed to [[toGrid]]. */
  def ignore[G <: Geometry, D](e: Extent, f: Feature[G, D]): Unit = Unit
}
