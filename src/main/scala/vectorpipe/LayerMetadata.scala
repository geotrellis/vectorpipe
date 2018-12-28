package vectorpipe

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.util.Component

import spray.json._
import spray.json.DefaultJsonProtocol._

// --- //

/** Minimalist Layer-level metadata. Necessary for writing layers of VectorTiles. */
case class LayerMetadata[K: JsonFormat](layout: LayoutDefinition, bounds: KeyBounds[K])

object LayerMetadata {

  /** A Lens into and from the key bounds. */
  implicit def metaComponent[K: JsonFormat]: Component[LayerMetadata[K], Bounds[K]] =
    Component[LayerMetadata[K], Bounds[K]](
      _.bounds,
      (md, bounds) => bounds match {
        case kb: KeyBounds[K] => md.copy(bounds = kb)
        case _ => md
      }
    )

  /** Json Conversion. */
  implicit def metaFormat[K: JsonFormat]: RootJsonFormat[LayerMetadata[K]] =
    jsonFormat2(LayerMetadata[K])
}
