package vectorpipe.util

import geotrellis.vector._
import geotrellis.vector.io._
import spray.json._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}

class JsonRobustFeatureCollectionMap(features: List[JsValue] = Nil) {
  private val buffer = mutable.ListBuffer(features:_*)

  def add[G <: Geometry: ClassTag, D: JsonWriter](featureMap: (String, RobustFeature[G, D])) =
    buffer += RobustFeatureFormats.writeRobustFeatureJsonWithID(featureMap)

  def toJson: JsValue = {
    val bboxOption = getAll[Geometry].map(_._2.envelope).reduceOption(_ combine _)
    bboxOption match {
      case Some(bbox) =>
        JsObject(
          "type" -> JsString("FeatureCollection"),
          "bbox" -> ExtentListWriter.write(bbox),
          "features" -> JsArray(buffer.toVector)
        )
      case _ =>
        JsObject(
          "type" -> JsString("FeatureCollection"),
          "features" -> JsArray(buffer.toVector)
        )
    }
  }

  private def getFeatureID(js: JsValue): String = {
    js.asJsObject.getFields("id") match {
      case Seq(JsString(id)) => id
      case Seq(JsNumber(id)) => id.toString
      case _ => throw new DeserializationException("Feature expected to have \"ID\" field")
    }
  }

  def getAll[F :JsonReader]: Map[String, F] = {
    var ret = Map[String, F]()
    features.foreach{ f =>
      Try(f.convertTo[F]) match {
        case Success(feature) =>
          ret += getFeatureID(f) -> feature
        case Failure(_: spray.json.DeserializationException) =>  //didn't match, live to fight another match
        case Failure(e) => throw e // but bubble up other exceptions
      }
    }
    ret.toMap
  }
}
