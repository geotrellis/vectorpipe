package vectorpipe

import geotrellis.spark.SpatialKey
import geotrellis.vector._
import org.apache.spark.rdd._
import vectorpipe.util.Tree

// --- //

package object osm {

  type TagMap = Map[String, String]
  type OSMFeature = Feature[Geometry, (Tree[ElementData], Extent)]
  type OSMPoint = Feature[Point, Tree[ElementData]]
  type OSMLine = Feature[Line, Tree[ElementData]]
  type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  implicit class elementRDDMethods(rdd: RDD[Element]) extends ElementRDD(rdd)
  implicit class featureRDDMethods(rdd: RDD[OSMFeature]) extends OSMFeatureRDD(rdd)
  implicit class tiledOSMRDDMethods(rdd: RDD[(SpatialKey, Iterable[OSMFeature])]) extends TiledOSMRDD(rdd)

}
