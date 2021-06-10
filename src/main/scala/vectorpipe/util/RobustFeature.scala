package vectorpipe.util

import geotrellis.vector.io._
import geotrellis.vector._
import spray.json._

import scala.reflect.{ClassTag, classTag}
import scala.util.Try

case class RobustFeature[+G <: Geometry: ClassTag, D](geom: Option[G], data: D) {
  def toFeature(): Feature[G, D] = {
    val g = geom match {
      case Some(gg) => gg
      // case x if classTag[Option[Line]].runtimeClass.isInstance(x) => Line(GeomFactory.factory.createLineString)
      // case x if classTag[Option[Polygon]].runtimeClass.isInstance(x) => Polygon(GeomFactory.factory.createPolygon)
      // case x if classTag[Option[MultiPoint]].runtimeClass.isInstance(x) => MultiPoint(GeomFactory.factory.createMultiPoint)
      // case x if classTag[Option[MultiLine]].runtimeClass.isInstance(x) => MultiLine(GeomFactory.factory.createMultiLineString)
      // case x if classTag[Option[MultiPolygon]].runtimeClass.isInstance(x) => MultiPolygon(GeomFactory.factory.createMultiPolygon)
      case _ => MultiPoint.EMPTY
    }
    Feature(g.asInstanceOf[G], data)
  }
}

trait RobustFeatureFormats {
  def writeRobustFeatureJson[G <: Geometry: ClassTag, D: JsonWriter](obj: RobustFeature[G, D]): JsValue = {
    val feature = obj.toFeature
    JsObject(
      "type" -> JsString("Feature"),
      "geometry" -> GeometryFormat.write(feature.geom),
      "bbox" -> ExtentListWriter.write(feature.envelope),
      "properties" -> obj.data.toJson
    )
  }

  def readRobustFeatureJson[D: JsonReader, G <: Geometry: JsonReader: ClassTag](value: JsValue): RobustFeature[G, D] = {
    value.asJsObject.getFields("type", "geometry", "properties") match {
      case Seq(JsString("Feature"), geom, data) =>
        val g = Try(geom.convertTo[G]).toOption
        val d = data.convertTo[D]
        g match {
          case Some(gg) if gg isEmpty => RobustFeature(None, d)
          case _ => RobustFeature(g, d)
        }
      case _ => throw new DeserializationException("Feature expected")
    }
  }

  def writeRobustFeatureJsonWithID[G <: Geometry: ClassTag, D: JsonWriter](idFeature: (String, RobustFeature[G, D])): JsValue = {
    val feature = idFeature._2.toFeature
    JsObject(
      "type" -> JsString("Feature"),
      "geometry" -> GeometryFormat.write(feature.geom),
      "bbox" -> ExtentListWriter.write(feature.envelope),
      "properties" -> idFeature._2.data.toJson,
      "id" -> JsString(idFeature._1)
    )
  }

  def readFeatureJsonWithID[D: JsonReader, G <: Geometry: JsonReader: ClassTag](value: JsValue): (String, RobustFeature[G, D]) = {
    value.asJsObject.getFields("type", "geometry", "properties", "id") match {
      case Seq(JsString("Feature"), geom, data, id) =>
        val g = Try(geom.convertTo[G]).toOption
        val d = data.convertTo[D]
        val i = id.toString
        g match {
          case Some(gg) if gg isEmpty => (i, RobustFeature(None, d))
          case _ => (i, RobustFeature(g, d))
        }
      case _ => throw new DeserializationException("Feature expected")
    }
  }

  implicit def robustFeatureReader[G <: Geometry: JsonReader: ClassTag, D: JsonReader] = new RootJsonReader[RobustFeature[G, D]] {
    override def read(json: JsValue): RobustFeature[G, D] =
      readRobustFeatureJson[D, G](json)
  }

  implicit def robustFeatureWriter[G <: Geometry: JsonWriter: ClassTag, D: JsonWriter] = new RootJsonWriter[RobustFeature[G, D]] {
    override def write(obj: RobustFeature[G, D]): JsValue = writeRobustFeatureJson(obj)
  }

  implicit def robustFeatureFormat[G <: Geometry: JsonFormat: ClassTag, D: JsonFormat] = new RootJsonFormat[RobustFeature[G, D]] {
    override def read(json: JsValue): RobustFeature[G, D] =
      readRobustFeatureJson[D, G](json)
    override def write(obj: RobustFeature[G, D]): JsValue =
      writeRobustFeatureJson(obj)
  }

  implicit object robustFeatureCollectionFormat extends RootJsonFormat[JsonRobustFeatureCollection] {
    override def read(json: JsValue): JsonRobustFeatureCollection = json.asJsObject.getFields("type", "features") match {
      case Seq(JsString("FeatureCollection"), JsArray(features)) => new JsonRobustFeatureCollection(features.toList)
      case _ => throw new DeserializationException("FeatureCollection expected")
    }
    override def write(obj: JsonRobustFeatureCollection): JsValue = obj.toJson
  }

  implicit object robustFeatureCollectionMapFormat extends RootJsonFormat[JsonRobustFeatureCollectionMap] {
    override def read(json: JsValue): JsonRobustFeatureCollectionMap = json.asJsObject.getFields("type", "features") match {
      case Seq(JsString("FeatureCollection"), JsArray(features)) => new JsonRobustFeatureCollectionMap(features.toList)
      case _ => throw new DeserializationException("FeatureCollection expected")
    }
    override def write(obj: JsonRobustFeatureCollectionMap): JsValue = obj.toJson
  }
}

object RobustFeatureFormats extends RobustFeatureFormats with DefaultJsonProtocol
