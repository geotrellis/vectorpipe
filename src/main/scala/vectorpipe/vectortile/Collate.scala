package vectorpipe.vectortile

import scala.collection.mutable.{ListBuffer, Map => MMap}

import geotrellis.vector._
import geotrellis.vectortile.{StrictLayer, Value, VectorTile}
import vectorpipe.osm.OSMFeature

// --- //

/** "Schema" functions which form [[geotrellis.vectortile.VectorTile]]s from
  * collections of [[OSMFeature]]s.
  *
  * ===On Winding Order===
  * VectorTiles require that Polygon exteriors have clockwise winding order,
  * and that interior holes have counter-clockwise winding order. These assume
  * that the origin `(0,0)` is in the '''top-left''' corner.
  *
  * '''All collation functions here must correct for winding order
  * themselves.''' Why? OSM data makes no guarantee about what winding order its
  * derived Polygons will have. We could correct winding order when our first
  * `RDD[OSMFeature]` is created, except that its unlikely that the clipping
  * process afterward would maintain our winding for all Polygons.
  */
object Collate {
  /** Collate some collection of Features into a [[VectorTile]] while
    * dropping any metadata they might have had. The resulting Tile has three
    * [[Layer]]s, labelled `points`, `lines`, and `polygons`.
    *
    * @param tileExtent The CRS Extent of the Tile to be created.
    */
  def withoutMetadata[G <: Geometry, D](tileExtent: Extent, geoms: Iterable[Feature[G, D]]): VectorTile = {
    def partition(f: Feature[G, D]): String = f.geom match {
      case g: Point        => "points"
      case g: MultiPoint   => "points"
      case g: Line         => "lines"
      case g: MultiLine    => "lines"
      case g: Polygon      => "polygons"
      case g: MultiPolygon => "polygons"
    }

    generically(tileExtent, geoms, partition, { _: D => Map.empty })
  }

  def byAnalytics(tileExtent: Extent, geoms: Iterable[OSMFeature]): VectorTile = {
    ???
  }

  /** Given some Feature and a way to determine which [[Layer]] it should
    * belong to (by layer name), collate each Feature into the appropriate
    * [[Layer]] and form a [[VectorTile]].
    *
    * @param tileExtent The Extent of ''this'' Tile.
    * @param geoms The Features to collate into various [[Layer]]s.
    * @param partition The means by which to place a certain Feature into a
    *                  certain [[Layer]]. The `String` is returns is the name
    *                  of the Layer the collate the Feature into.
    * @param metadata The means by which to transform some Feature's metadata
    *                 into the type that [[VectorTile]]s expect.
    */
  def generically[G <: Geometry, D](
    tileExtent: Extent,
    geoms: Iterable[Feature[G, D]],
    partition: Feature[G, D] => String,
    metadata: D => Map[String, Value]
  ): VectorTile = {
    def work(name: String, lb: ListBuffer[Feature[G, D]]): StrictLayer = {
      val points  = new ListBuffer[Feature[Point, Map[String, Value]]]
      val mpoints = new ListBuffer[Feature[MultiPoint, Map[String, Value]]]
      val lines   = new ListBuffer[Feature[Line, Map[String, Value]]]
      val mlines  = new ListBuffer[Feature[MultiLine, Map[String, Value]]]
      val polys   = new ListBuffer[Feature[Polygon, Map[String, Value]]]
      val mpolys  = new ListBuffer[Feature[MultiPolygon, Map[String, Value]]]

      /* Partition the Geometries by subtype. Polygon winding order is corrected. */
      lb.foreach({ f => f.geom match {
        case g: Point        => points.append(Feature(g, metadata(f.data)))
        case g: MultiPoint   => mpoints.append(Feature(g, metadata(f.data)))
        case g: Line         => lines.append(Feature(g, metadata(f.data)))
        case g: MultiLine    => mlines.append(Feature(g, metadata(f.data)))
        case g: Polygon      => polys.append(Feature(winding(g), metadata(f.data)))
        case g: MultiPolygon => mpolys.append(Feature(MultiPolygon(g.polygons.map(winding)), metadata(f.data)))
      }})

      /* The values 4096 and 2 here are expected defaults */
      StrictLayer(name, 4096, 2, tileExtent, points, mpoints, lines, mlines, polys, mpolys)
    }

    /* Both `MMap` and `ListBuffer` are mutable */
    val collated: MMap[String, ListBuffer[Feature[G, D]]] = MMap.empty

    geoms.foreach({ f =>
      val layer: String = partition(f)

      collated.get(layer) match {
        case None => collated.update(layer, ListBuffer(f))
        case Some(lb) => lb.append(f)
      }
    })

    /* Collate each collection of Features into a [[Layer]] */
    val layers: Map[String, StrictLayer] =
      collated.map({ case (k,v) => (k, work(k,v)) }).toMap

    VectorTile(layers, tileExtent)
  }
}
