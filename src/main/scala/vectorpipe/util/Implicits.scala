package vectorpipe.util

import geotrellis.vector._
import spray.json._

import scala.reflect.ClassTag

object Implicits extends Implicits

trait Implicits extends RobustFeatureFormats {
  implicit class RobustFeaturesToGeoJson[G <: Geometry: ClassTag, D: JsonWriter](features: Traversable[RobustFeature[G, D]]) {
    def toGeoJson(): String = {
      val fc = new JsonRobustFeatureCollection

      features.foreach(fc.add(_))

      fc.toJson.compactPrint
    }
  }
}
