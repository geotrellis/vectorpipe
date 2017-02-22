package vectorpipe.osm

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._
import vectorpipe.osm.internal.{ Gridding => G, Pow2Layout }

// --- //

class FeatureRDDMethods(val self: RDD[OSMFeature]) extends MethodExtensions[RDD[OSMFeature]] {

  /** Given a particular zoom level, split a collection of [[OSMFeature]]s
    * into a grid of them indexed by [[SpatialKey]].
    *
    * @param ld The LayoutDefinition defining the area to gridify.
    */
  def toGrid(ld: LayoutDefinition)(implicit sc: SparkContext): RDD[(SpatialKey, Array[OSMFeature])] = {

    /* Initial bounding box for capturing Features */
    val extent: Polygon = ld.extent.toPolygon()

    /* Filter once to avoid later false positives on Extent overlapping after
     * performing `Gridding.inflate`.
     */
    val bounded: RDD[OSMFeature] = self.filter(f => f.geom.intersects(extent))

    work(G.inflate(ld), self)
  }

  /** Divide-and-conquer over progressively smaller extents, capturing
    * Features as it goes in `O(nlogn)`.
    */
  private def work(
    p2l: Pow2Layout,
    rdd: RDD[OSMFeature]
  )(implicit sc: SparkContext): RDD[(SpatialKey, Array[OSMFeature])] = p2l match {
    case p if p.isUnit => sc.parallelize(
      seq = Seq((p.kb.minKey, rdd.collect())),
      numSlices = 1  /* One partition per SpatialKey */ // TODO Bad idea?
    )
    case _ => p2l.reduction.map({ p =>
      /* An extent for this subsection of the given Layout */
      val extent: Polygon = p.extent.toPolygon()

      work(p, rdd.filter(f => f.geom.intersects(extent)))
    }).reduce(_ ++ _)
  }
}
