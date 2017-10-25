package vectorpipe

import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}

import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector._
import geotrellis.vector.io._

// --- //

/** Clipping Strategies. */
object Clip {

  /** For any segment of a [[Line]] that extends outside the Extent,
    * clip directly on its nearest Point to the outside edge of that Extent.
    *
    * @see [[https://github.com/geotrellis/vectorpipe/issues/11]]
    */
  def toNearestPoint(extent: Extent, line: Line): MultiLine = {
    val origPoints: Array[Point] = line.points
    val points: Array[Point] = origPoints.tail

    /* Setting these here makes calls to `Clip.intersects` faster */
    val centre: Point = Point(extent.xmin + (extent.width / 2), extent.ymin + (extent.height / 2))
    val radius: Double = extent.northWest.distance(centre)

    /* The mutability is real */
    var acc: ListBuffer[Point] = new ListBuffer[Point]
    var lines: ListBuffer[Line] = new ListBuffer[Line]
    var i: Int = 0
    var last: Point = origPoints.head

    while (i < points.length) {
      val p: Point = points(i)

      if (extent.intersects(p) || extent.intersects(last)) {
      /* First condition: The current Point is within the Extent.
       * Regardless of where the previous Point was, we want to keep it:
       *   In  -> In : We're inside the Extent still.
       *   Out -> In : We were outside, now moving in.
       *
       * Second condition: We've moved outside the Extent.
       */
        acc.append(last)
      } else if (acc.nonEmpty) {
        /* We've moved further away from the first Point outside the Extent */
        acc.append(last)
        lines.append(Line(acc))
        acc = new ListBuffer[Point]
      } else if (intersects(centre, radius, last, p)) {
        /* A line segment crosses the Extent, but has no Points within it */
        acc.append(last)
      }

      /* Otherwise, we're moving along a segment of external Points. */
      last = p
      i += 1
    }

    if (acc.nonEmpty) {
      acc.append(last)
      lines.append(Line(acc))
    }

    MultiLine(lines)
  }

  /** A faster way to test Line-Extent intersection, when it's known that:
    *   - The Line only has two points
    *   - The two points lie outside the Extent
    */
  private def intersects(centre: Point, radius: Double, p1: Point, p2: Point): Boolean =
    centre.distanceToSegment(p1, p2) <= radius

  /** Naively clips Features to fit the given Extent. */
  def byExtent[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[Geometry, D]] = f.geom match {
    case g: Point                              => Some(f)
    case g: MultiPoint if coveredBy(g, extent) => Some(f)
    case g: MultiPoint                         => clip(extent, g).map(Feature(_, f.data))
    case g: Line       if coveredBy(g, extent) => Some(f)
    case g: Line                               => clip(extent, g).map(Feature(_, f.data))
    case g: MultiLine  if coveredBy(g, extent) => Some(f)
    case g: MultiLine                          => clip(extent, g).map(Feature(_, f.data))
    case g =>  /* Polygon and MultiPolygon */
      val pg = PreparedGeometryFactory.prepare(g.jtsGeom)
      val ep = extent.toPolygon.jtsGeom

      if (pg.covers(ep)) Some(Feature(extent, f.data))
      else if (pg.coveredBy(ep)) Some(f)
      else clip(extent, g).map(Feature(_, f.data))
  }

  private def coveredBy[G <: Geometry](g: G, e: Extent): Boolean =
    g.jtsGeom.coveredBy(e.toPolygon.jtsGeom)

  private def clip[G <: Geometry](e: Extent, g: G): Option[Geometry] = {
    val exPoly: Polygon = e.toPolygon

    val clipped: Try[Geometry] = g match {
      case mp: MultiPolygon => Try(MultiPolygon(mp.polygons.flatMap(_.intersection(exPoly).as[Polygon])))
      case _ => Try(g.intersection(exPoly).toGeometry.get)
    }

    clipped.toOption
  }

  /** Clips Features to a 3x3 grid surrounding the current Tile.
    * This has been found to capture ''most'' Features which stretch
    * outside their original Tile, and helps avoid the pain of
    * restitching later.
    */
  def byBufferedExtent[G <: Geometry, D](
    extent: Extent,
    f: Feature[G, D]
  ): Option[Feature[Geometry, D]] = f.geom match {
    case g: Point => Some(f)  /* A `Point` will always fall within the Extent */
    case _ => byExtent(extent.expandBy(extent.width, extent.height), f)
  }

  /** Yield an [[Feature]] as-is. */
  def asIs[G <: Geometry, D](extent: Extent, f: Feature[G, D]): Option[Feature[G, D]] = Some(f)
}
