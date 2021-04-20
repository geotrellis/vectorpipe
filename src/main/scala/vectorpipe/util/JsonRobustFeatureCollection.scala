package vectorpipe.util

import geotrellis.vector._
import geotrellis.vector.io._
import spray.json._

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}

class JsonRobustFeatureCollection(features: List[JsValue] = Nil) {
  private val buffer = mutable.ListBuffer(features: _*)

  def add[G <: Geometry: ClassTag, D: JsonWriter](feature: RobustFeature[G, D]) =
    buffer += RobustFeatureFormats.writeRobustFeatureJson(feature)

  def getAll[F: JsonReader]: Vector[F] = {
    val ret = new VectorBuilder[F]()
    features.foreach{ f =>
      Try(f.convertTo[F]) match {
        case Success(feature) =>
          ret += feature
        case Failure(_: spray.json.DeserializationException) =>  //didn't match, live to fight another match
        case Failure(e) => throw e // but bubble up other exceptions
      }
    }
    ret.result()
  }

  def getAllGeometries(): Vector[Geometry] =
    getAll[Point] ++ getAll[Line] ++ getAll[Polygon] ++
      getAll[MultiPoint] ++ getAll[MultiLine] ++ getAll[MultiPolygon]

  def toJson: JsValue = {
    val bboxOption = getAllGeometries.map(_.envelope).reduceOption(_ combine _)
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
}

// object JsonRobustFeatureCollection {

//   def apply() = new JsonRobustFeatureCollection

//   def apply[G <: Geometry, D: JsonWriter]
