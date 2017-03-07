package vectorpipe.osm

import scala.collection.mutable.{Set => MSet}

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

// --- //

class OSMFeatureRDD(val self: RDD[OSMFeature]) extends MethodExtensions[RDD[OSMFeature]] {

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
    * @return The pair `(OSMFeature, Extent)` reprents the clipped Feature
    *         along with its __original__ bounding envelope. This envelope
    *         is to be encoded into the Feature's metadata within a VT,
    *         and could be used later to aid the reconstruction of the original,
    *         unclipped Feature.
    */
  def toGrid(ld: LayoutDefinition)
            (clip: (Extent, OSMFeature) => OSMFeature)
            (implicit sc: SparkContext): RDD[(SpatialKey, Iterable[(OSMFeature, Extent)])] = {

    val mt: MapKeyTransform = ld.mapTransform

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon()

    /* Filter once to reduce later workload */
    val bounded: RDD[OSMFeature] = self.filter(f => f.geom.intersects(extent))

    /* Associate each Feature with a SpatialKey */
    bounded.flatMap({ f =>
      val env: Extent = f.geom.envelope
      val bounds: GridBounds = mt(env) /* Keys overlapping the Geom envelope */
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
        (k, iter.map(g => (clip(kExt, g), g.envelope)))
      })
  }

}
