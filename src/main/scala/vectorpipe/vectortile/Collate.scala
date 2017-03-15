package vectorpipe.vectortile

import geotrellis.vector._
import geotrellis.vectortile.{ StrictLayer, VectorTile, Value }
import scala.collection.mutable.ListBuffer

// --- //

/** "Schema" functions which form [[geotrellis.vectortile.VectorTile]]s from
  * collections of [[OSMFeature]]s.
  */
object Collate {
  /** Collate some normal collection of Geometries without metadata into a
    * [[VectorTile]]. The resulting Tile has three [[Layer]]s, labelled `points`,
    * `lines`, and `polygons.
    */
  def byNakedGeoms(tileExtent: Extent, geoms: Iterable[Geometry]): VectorTile = {
    val points  = new ListBuffer[Feature[Point, Map[String, Value]]]
    val mpoints = new ListBuffer[Feature[MultiPoint, Map[String, Value]]]
    val lines   = new ListBuffer[Feature[Line, Map[String, Value]]]
    val mlines  = new ListBuffer[Feature[MultiLine, Map[String, Value]]]
    val polys   = new ListBuffer[Feature[Polygon, Map[String, Value]]]
    val mpolys  = new ListBuffer[Feature[MultiPolygon, Map[String, Value]]]

    /* Partition the Geometries by subtype */
    geoms.foreach({
      case g: Point        => points.append(Feature(g, Map.empty))
      case g: MultiPoint   => mpoints.append(Feature(g, Map.empty))
      case g: Line         => lines.append(Feature(g, Map.empty))
      case g: MultiLine    => mlines.append(Feature(g, Map.empty))
      case g: Polygon      => polys.append(Feature(g, Map.empty))
      case g: MultiPolygon => mpolys.append(Feature(g, Map.empty))
    })

    // TODO: Ensure Polygon winding order

    val pointLayer = StrictLayer.empty("points", tileExtent).copy(
      points = points, multiPoints = mpoints
    )

    val lineLayer = StrictLayer.empty("lines", tileExtent).copy(
      lines = lines, multiLines = mlines
    )

    val polyLayer = StrictLayer.empty("polygons", tileExtent).copy(
      polygons = polys, multiPolygons = mpolys
    )

    val layers = Map(
      pointLayer.name -> pointLayer,
      lineLayer.name -> lineLayer,
      polyLayer.name -> polyLayer
    )

    VectorTile(layers, tileExtent)
  }
}
