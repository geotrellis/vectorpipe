
package vectorpipe.util

import cats.syntax.either._
import geotrellis.vector._
import _root_.io.circe._
import _root_.io.circe.syntax._

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.reflect.ClassTag

class JsonRobustFeatureCollection(features: List[Json] = Nil) {
  private val buffer = mutable.ListBuffer(features: _*)

  def add[G <: Geometry: ClassTag, D: Encoder](feature: RobustFeature[G, D]) =
    buffer += RobustFeatureFormats.writeRobustFeatureJson(feature)

  def getAll[F: Decoder]: Vector[F] = {
    val ret = new VectorBuilder[F]()
    features.foreach{ _.as[F].foreach(ret += _) }
    ret.result()
  }

  def getAllRobustFeatures[F <: RobustFeature[_, _] :Decoder]: Vector[F] =
    getAll[F]

  def getAllPointFeatures[D: Decoder]()           = getAll[RobustFeature[Point, D]]
  def getAllLineStringFeatures[D: Decoder]()      = getAll[RobustFeature[LineString, D]]
  def getAllPolygonFeatures[D: Decoder]()         = getAll[RobustFeature[Polygon, D]]
  def getAllMultiPointFeatures[D: Decoder]()      = getAll[RobustFeature[MultiPoint, D]]
  def getAllMultiLineStringFeatures[D: Decoder]() = getAll[RobustFeature[MultiLineString, D]]
  def getAllMultiPolygonFeatures[D: Decoder]()    = getAll[RobustFeature[MultiPolygon, D]]

  def getAllGeometries(): Vector[Geometry] =
    getAll[Point] ++ getAll[LineString] ++ getAll[Polygon] ++
      getAll[MultiPoint] ++ getAll[MultiLineString] ++ getAll[MultiPolygon]

  def asJson: Json = {
    val bboxOption = getAllGeometries.map(_.extent).reduceOption(_ combine _)
    bboxOption match {
      case Some(bbox) =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "bbox" -> Extent.listEncoder(bbox),
          "features" -> buffer.toVector.asJson
        )
      case _ =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "features" -> buffer.toVector.asJson
        )
    }
  }
}

object JsonRobustFeatureCollection {
  def apply() = new JsonRobustFeatureCollection()

  def apply[G <: Geometry: ClassTag, D: Encoder](features: Traversable[RobustFeature[G, D]]) = {
    val fc = new JsonRobustFeatureCollection()
    features.foreach(fc.add(_))
    fc
  }

  def apply(features: Traversable[Json])(implicit d: DummyImplicit): JsonRobustFeatureCollection =
    new JsonRobustFeatureCollection(features.toList)
}
