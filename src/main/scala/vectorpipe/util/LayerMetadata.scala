package vectorpipe.util

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.util.GetComponent
import spray.json._
import spray.json.DefaultJsonProtocol._

// --- //

/** Minimalist Layer-level metadata. Necessary for writing layers of VectorTiles. */
case class LayerMetadata[K: JsonFormat](layout: LayoutDefinition, bounds: KeyBounds[K])

object LayerMetadata {

  /** A Lens into the key bounds. */
  implicit def metaGet[K]: GetComponent[LayerMetadata[K], Bounds[K]] =
    GetComponent(_.bounds)

  /** Json Conversion. */
  implicit def metaFormat[K: JsonFormat]: RootJsonFormat[LayerMetadata[K]] =
    jsonFormat2(LayerMetadata[K])
}
