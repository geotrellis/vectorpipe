package vectorpipe.util

import cats.syntax.either._
import geotrellis.vector._
import geotrellis.vector.io.json._
import _root_.io.circe._
import _root_.io.circe.syntax._

import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}

case class RobustFeature[+G <: Geometry: ClassTag, D](geom: Option[G], data: D) {
  def toFeature(): Feature[G, D] = {
    val g = geom match {
      case Some(gg) => gg
      case _ => MultiPoint.EMPTY
    }
    Feature(g.asInstanceOf[G], data)
  }
}

trait RobustFeatureFormats {
  def writeRobustFeatureJson[G <: Geometry: ClassTag, D: Encoder](obj: RobustFeature[G, D]): Json = {
    val feature = obj.toFeature
    Json.obj(
      "type" -> "Feature".asJson,
      "geometry" -> GeometryFormats.geometryEncoder(feature.geom),
      "bbox" -> Extent.listEncoder(feature.geom.extent),
      "properties" -> obj.data.asJson
    )
  }

  def writeRobustFeatureJsonWithID[G <: Geometry: ClassTag, D: Encoder](idFeature: (String, RobustFeature[G, D])): Json = {
    val feature = idFeature._2.toFeature
    Json.obj(
      "type" -> "Feature".asJson,
      "geometry" -> GeometryFormats.geometryEncoder(feature.geom),
      "bbox" -> Extent.listEncoder(feature.geom.extent),
      "properties" -> idFeature._2.data.asJson,
      "id" -> idFeature._1.asJson
    )
  }

  def readRobustFeatureJson[D: Decoder, G <: Geometry: Decoder: ClassTag](value: Json): RobustFeature[G, D] = {
    val c = value.hcursor
    (c.downField("type").as[String], c.downField("geometry").focus, c.downField("properties").focus) match {
      case (Right("Feature"), Some(geom), Some(data)) =>
        //val g = Try(geom.convertTo[G]).toOption
        //val d = data.convertTo[D]
        (Try(geom.as[G].toOption).toOption.getOrElse(None), data.as[D].toOption) match {
          case (Some(g), Some(d)) if g isEmpty => RobustFeature(None, d)
          case (Some(g), Some(d)) => RobustFeature(Some(g), d)
          case (None, Some(d)) => RobustFeature(None, d)
          case (_, None) => throw new Exception(s"Feature expected well-formed data; got $data")
        }
      case _ => throw new Exception("Feature expected")
    }
  }

  def readRobustFeatureJsonWithID[D: Decoder, G <: Geometry: Decoder: ClassTag](value: Json): (String, RobustFeature[G, D]) = {
    val c = value.hcursor
    (c.downField("type").as[String], c.downField("geometry").focus, c.downField("properties").focus, c.downField("id").focus) match {
      case (Right("Feature"), Some(geom), Some(data), Some(id)) =>
        //val g = Try(geom.convertTo[G]).toOption
        //val d = data.convertTo[D]
        (Try(geom.as[G].toOption).toOption.getOrElse(None), data.as[D].toOption, id.as[String].toOption) match {
          case (Some(g), Some(d), Some(i)) if g isEmpty => (i, RobustFeature(None, d))
          case (Some(g), Some(d), Some(i)) => (i, RobustFeature(Some(g), d))
          case (None, Some(d), Some(i)) => (i, RobustFeature(None, d))
          case _ => throw new Exception(s"Feature expected well-formed id and data; got (${id}, ${data})")
        }
      case _ => throw new Exception("Feature expected")
    }
  }

  implicit def robustFeatureDecoder[G <: Geometry: Decoder: ClassTag, D: Decoder]: Decoder[RobustFeature[G, D]] =
    Decoder.decodeJson.emap { json: Json =>
      Try(readRobustFeatureJson[D, G](json)) match {
        case Success(f) => Right(f)
        case Failure(e) => Left(e.getMessage)
      }
    }

  implicit def robustFeatureEncoder[G <: Geometry: Encoder: ClassTag, D: Encoder]: Encoder[RobustFeature[G, D]] =
    Encoder.encodeJson.contramap[RobustFeature[G, D]] { writeRobustFeatureJson }

  implicit val robustFeatureCollectionEncoder: Encoder[JsonRobustFeatureCollection] =
    Encoder.encodeJson.contramap[JsonRobustFeatureCollection] { _.asJson }

  implicit val robustFeatureCollectionDecoder: Decoder[JsonRobustFeatureCollection] =
    Decoder.decodeHCursor.emap { c: HCursor =>
      (c.downField("type").as[String], c.downField("features").focus) match {
        case (Right("FeatureCollection"), Some(features)) => Right(JsonRobustFeatureCollection(features.asArray.toVector.flatten))
        case _ => Left("FeatureCollection expected")
      }
    }

  implicit val robustFeatureCollectionMapEncoder: Encoder[JsonRobustFeatureCollectionMap] =
    Encoder.encodeJson.contramap[JsonRobustFeatureCollectionMap] { _.asJson }

  implicit val robustFeatureCollectionMapDecoder: Decoder[JsonRobustFeatureCollectionMap] =
    Decoder.decodeHCursor.emap { c: HCursor =>
      (c.downField("type").as[String], c.downField("features").focus) match {
        case (Right("FeatureCollection"), Some(features)) => Right(JsonRobustFeatureCollectionMap(features.asArray.toVector.flatten))
        case _ => Left("FeatureCollection expected")
      }
    }
}

object RobustFeatureFormats extends RobustFeatureFormats
