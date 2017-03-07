package vectorpipe.geom

import geotrellis.vector._
import vectorpipe.osm._

// --- //

/** Clipping Strategies.
  * Remember that `identity` is also technically a valid Clipping Strat. */
object Clip {
  def toNearestPoint(extent: Extent, line: Line): MultiLine = ???

  /** Naively clips Features to fit the given Extent. */
  def byExtent(extent: Extent, f: OSMFeature): OSMFeature = Feature(
    f.geom.intersection(extent.toPolygon).toGeometry().get,
    f.data
  )

  /** Clips Features to a 3x3 grid surrounding the current Tile.
    * This has been found to capture ''most'' Features which stretch
    * outside their original Tile, and helps avoid the pain of
    * restitching later.
    */
  def byBufferedExtent(extent: Extent, f: OSMFeature): OSMFeature =
    byExtent(extent.expandBy(extent.width, extent.height), f)

  /** Bias the clipping strategy based on the incoming [[Geometry]]. */
  def byHybrid(extent: Extent, f: OSMFeature): OSMFeature = f.geom match {
    case pnt: Point => f  /* A `Point` will always fall within the Extent */
    case line: Line => Feature(toNearestPoint(extent, line), f.data)
    case poly: Polygon => byBufferedExtent(extent, f)
    case mply: MultiPolygon => byBufferedExtent(extent, f)
  }
}
