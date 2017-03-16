package vectorpipe.osm

import geotrellis.spark._
import geotrellis.spark.tiling.{ LayoutDefinition, MapKeyTransform }
import geotrellis.util._
import geotrellis.vector.Extent
import geotrellis.vectortile.VectorTile
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

// --- //

class TiledOSMRDD(
  val self: RDD[(SpatialKey, Iterable[OSMFeature])]
) extends MethodExtensions[RDD[(SpatialKey, Iterable[OSMFeature])]] {

  /** Given a collection of GeoTrellis `Feature`s which have been associated
    * with some `SpatialKey` and a "collation" function, form those `Feature`s
    * into a `VectorTile`.
    *
    * @see [[vectorpipe.vectortile.Collate]]
    */
  def toVectorTile(ld: LayoutDefinition)
                  (collate: (Extent, Iterable[OSMFeature]) => VectorTile)
                  (implicit sc: SparkContext): RDD[(SpatialKey, VectorTile)] = {
    val mt: MapKeyTransform = ld.mapTransform

    self.map({ case (k, iter) => (k, collate(mt(k), iter))})
  }

}
