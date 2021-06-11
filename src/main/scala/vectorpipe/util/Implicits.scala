package vectorpipe.util

import geotrellis.vector._
import _root_.io.circe._

import scala.reflect.ClassTag

object Implicits extends Implicits

trait Implicits extends RobustFeatureFormats {
  implicit class RobustFeaturesToGeoJson[G <: Geometry: ClassTag, D: Encoder](features: Traversable[RobustFeature[G, D]]) {
    def toGeoJson(): String = {
      val fc = new JsonRobustFeatureCollection

      features.foreach(fc.add(_))

      fc.asJson.noSpaces
    }
  }
}
