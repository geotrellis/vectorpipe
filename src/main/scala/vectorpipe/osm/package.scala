package vectorpipe

import vectorpipe.util.Tree

import geotrellis.vector._
import org.apache.spark.rdd._

// --- //

package object osm {
  type TagMap = Map[String, String]
  type OSMFeature = Feature[Geometry, Tree[ElementData]]

  implicit class injectElementRDDMethods(rdd: RDD[Element]) extends ElementRDDMethods(rdd)

}
