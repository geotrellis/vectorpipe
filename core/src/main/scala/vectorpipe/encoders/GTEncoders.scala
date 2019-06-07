package vectorpipe.encoders

import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object GTEncoders {
  implicit def gtGeometryEncoder: Encoder[Geometry] = Encoders.kryo[Geometry]
  implicit def gtPointEncoder: Encoder[Point] = ExpressionEncoder()
  implicit def gtMultiPointEncoder: Encoder[MultiPoint] = ExpressionEncoder()
  implicit def gtLineEncoder: Encoder[Line] = ExpressionEncoder()
  implicit def gtMultiLineEncoder: Encoder[MultiLine] = ExpressionEncoder()
  implicit def gtPolygonEncoder: Encoder[Polygon] = ExpressionEncoder()
  implicit def gtMultiPolygonEncoder: Encoder[MultiPolygon] = ExpressionEncoder()

  implicit def gtFeatureEncoder[G <: Geometry, D](implicit ev1: Encoder[G], ev2: Encoder[D]): Encoder[Feature[G, D]] = Encoders.kryo[Feature[G, D]]

  implicit def gtVectorTileEncoder: Encoder[VectorTile] = Encoders.kryo[VectorTile]
  //implicit def gtLayerEncoder: Encoder[Layer] = Encoders.javaSerialization[Layer]
  //implicit def gtStrictLayerEncoder: Encoder[StrictLayer] = Encoders.kryo[StrictLayer]
}
