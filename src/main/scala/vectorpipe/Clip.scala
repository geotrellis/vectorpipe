package vectorpipe

import scala.annotation.tailrec
import scala.util.Try

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector._

// --- //

/** Clipping Strategies. */
object Clip {

  /** For any segment of a [[Line]] that extends outside the Extent,
    * clip directly on its nearest Point to the outside edge of that Extent.
    *
    * @see [[https://github.com/geotrellis/vectorpipe/issues/11]]
    */
  def toNearestPoint(extent: Extent, line: Line): MultiLine = {

    /* Setting these here makes calls to `Clip.intersects` faster */
    val centre: Point = Point(extent.xmin + (extent.width / 2), extent.ymin + (extent.height / 2))
    val radius: Double = extent.northWest.distance(centre)

    @tailrec def work(lines: List[Line], acc: List[Point], last: Point, ps: List[Point]): List[Line] = ps match {
      case Nil if acc.nonEmpty => Line(last :: acc) :: lines
      case Nil => lines
      /* First check: Is the current Point within the Extent?
       * Regardless of where the previous Point was, we want to keep it:
       *   In  -> In : We're inside the Extent still.
       *   Out -> In : We were outside, now moving in.
       *
       * Second check: Have we moved outside the Extent?
       */
      case p :: rest if (extent.intersects(p) || extent.intersects(last)) => work(lines, last :: acc, p, rest)
      /* We've moved further away from the first Point outside the Extent */
      case p :: rest if acc.nonEmpty => work(Line(last :: acc) :: lines, Nil, p, rest)
      /* A line segment crosses the Extent, but has no Points within it */
      case p :: rest if intersects(centre, radius, last, p) => work(lines, last :: acc, p, rest)
      /* Otherwise, we're moving along a segment of external Points. */
      case p :: rest => work(lines, acc, p, rest)
    }

    val h :: t = line.points.toList

    MultiLine(work(Nil, Nil, h, t))
  }

  /** A faster way to test Line-Extent intersection, when it's known that:
    *   - The Line only has two points
    *   - The two points lie outside the Extent
    */
  private[this] def intersects(centre: Point, radius: Double, p1: Point, p2: Point): Boolean =
    centre.distanceToSegment(p1, p2) <= radius

  /** Naively clips Features to fit the given Extent. */
  def byExtent[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[Geometry, D]] = {
    val exPoly: Polygon = extent.toPolygon

    val clipped: Try[Geometry] = f.geom match {
      case mp: MultiPolygon => Try(MultiPolygon(mp.polygons.flatMap(_.intersection(exPoly).as[Polygon])))
      case _ => Try(f.geom.intersection(exPoly).toGeometry.get)
    }

    clipped.toOption.map(g => Feature(g, f.data))
  }

  /** Clips Features to a 3x3 grid surrounding the current Tile.
    * This has been found to capture ''most'' Features which stretch
    * outside their original Tile, and helps avoid the pain of
    * restitching later.
    */
  def byBufferedExtent[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[Geometry, D]] =
    byExtent(extent.expandBy(extent.width, extent.height), f)

  /** Bias the clipping strategy based on the incoming [[Geometry]]. */
  def byHybrid[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[Geometry, D]] = f.geom match {
    case pnt: Point => Some(f)  /* A `Point` will always fall within the Extent */
    case line: Line => Some(Feature(toNearestPoint(extent, line), f.data))
    case poly: Polygon => byBufferedExtent(extent, f)
    case mply: MultiPolygon => byBufferedExtent(extent, f)
    case _ => None
  }

  /** Yield an [[Feature]] as-is. */
  def asIs[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[G, D]] = Some(f)
}
