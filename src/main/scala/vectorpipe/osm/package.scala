package vectorpipe

import vectorpipe.util.Tree

import geotrellis.vector._
import org.apache.spark.rdd._

// --- //

package object osm {
  type TagMap = Map[String, String]
  type OSMFeature = Feature[Geometry, Tree[ElementData]]
  type OSMPoint = Feature[Point, Tree[ElementData]]
  type OSMLine = Feature[Line, Tree[ElementData]]
  type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  implicit class injectElementRDDMethods(rdd: RDD[Element]) extends ElementRDDMethods(rdd)
  implicit class injectFeatureRDDMethods(rdd: RDD[OSMFeature]) extends FeatureRDDMethods(rdd)

}
