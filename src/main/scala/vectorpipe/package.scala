import java.nio.ByteBuffer

import scala.util.{ Try, Success, Failure }

import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.clip.ClipToGrid.Predicates
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile.VectorTile
import org.apache.avro._
import org.apache.avro.generic._
import org.apache.log4j.Logger
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
  *
  * @groupname actions Actions
  * @groupdesc actions Functions to transform `RDD`s of Features along the pipeline.
  * @groupprio actions 0
  *
  * @groupname logging Error Logging
  * @groupdesc logging Useful defaults for functions like [[vectorpipe.grid]], where
  *                    we wish to log small failures and skip them, instead of crashing
  *                    the entire Spark job.
  * @groupprio logging 1
  *
  * @groupname instances Typeclass Instances
  * @groupprio instances 2
  */
package object vectorpipe {

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
    * @group actions
    */
  def grid[D](
    clip: (Extent, Feature[Geometry, D], Predicates) => Option[Feature[Geometry, D]],
    logError: (((Extent, Feature[Geometry, D])) => String) => ((Extent, Feature[Geometry, D])) => Unit,
    ld: LayoutDefinition,
    rdd: RDD[Feature[Geometry, D]]
  ): RDD[(SpatialKey, Iterable[Feature[Geometry, D]])] = {

    /** A way to render some Geometry that failed to clip. */
    val errorClipping: ((Extent, Feature[Geometry, D])) => String = { case (e, f) =>
      s"CLIP FAILURE W/ EXTENT: ${e}\nELEMENT METADATA: ${f.data}\nGEOM: ${f.geom.reproject(WebMercator, LatLng).toGeoJson}"
    }

    def work(e: Extent, f: Feature[Geometry, D], p: Predicates): Option[Feature[Geometry, D]] = {
      Try(clip(e, f, p)) match {
        case Failure(_) => logError(errorClipping)((e, f)); None
        case Success(g) => g
      }
    }

    rdd.clipToGrid(ld, work _).groupByKey
  }

  /** Given a collection of GeoTrellis `Feature`s which have been associated
    * with some `SpatialKey` and a "collation" function, form those `Feature`s
    * into a `VectorTile`.
    *
    * @see [[vectorpipe.Collate]]
    * @group actions
    */
  def vectortiles[G <: Geometry, D](
    collate: (Extent, Iterable[Feature[G, D]]) => VectorTile,
    ld: LayoutDefinition,
    rdd: RDD[(SpatialKey, Iterable[Feature[G, D]])]
  ): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    rdd.map({ case (k, iter) => (k, collate(mt(k), iter))})
  }

  /** Log an error to STDOUT.
    *
    * @group logging
    */
  def logToStdout[A](f: A => String): A => Unit = { a => println(f(a)) }

  /** Log an error as an ERROR through Spark's default log4j.
    *
    * @group logging
    */
  def logToLog4j[A](f: A => String): A => Unit = { a => Logger.getRootLogger().error(f(a)) }

  /** Skip over some failure.
    *
    * @group logging
    */
  def logNothing[A](f: A => String): A => Unit = { _ => () }

  /** Encode a [[VectorTile]] via Avro. This is the glue for Layer IO.
    *
    * @group instances
    */
  implicit val vectorTileCodec = new AvroRecordCodec[VectorTile] {
    def schema: Schema = SchemaBuilder
      .record("VectorTile").namespace("geotrellis.vectortile")
      .fields()
      .name("bytes").`type`().bytesType().noDefault()
      .name("extent").`type`(extentCodec.schema).noDefault()
      .endRecord()

    def encode(tile: VectorTile, rec: GenericRecord): Unit = {
      rec.put("bytes", ByteBuffer.wrap(tile.toBytes))
      rec.put("extent", extentCodec.encode(tile.tileExtent))
    }

    def decode(rec: GenericRecord): VectorTile = {
      val bytes: Array[Byte] = rec[ByteBuffer]("bytes").array
      val extent: Extent = extentCodec.decode(rec[GenericRecord]("extent"))

      VectorTile.fromBytes(bytes, extent)
    }
  }
}
