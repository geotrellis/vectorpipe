package vectorpipe.osm

import geotrellis.vector._
import org.apache.spark.rdd.RDD

// --- //

/** A meaningful wrapping around the various geometry types that can be produced
  * from the OSM-to-Features conversion. By keeping each Geometry subtype separate,
  * the user can discard types they don't need.
  */
case class Features(
  points:     RDD[Feature[Point, ElementMeta]],
  lines:      RDD[Feature[Line, ElementMeta]],
  polygons:   RDD[Feature[Polygon, ElementMeta]],
  multiPolys: RDD[Feature[MultiPolygon, ElementMeta]]
) {
  /** Flatten the separated features into a single `RDD` of their supertype, `Geometry`. */
  def geometries: RDD[OSMFeature] = points.sparkContext.union(
    points.map(identity), lines.map(identity), polygons.map(identity), multiPolys.map(identity)
  )
}
