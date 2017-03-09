package vectorpipe.geom

import scala.annotation.tailrec

import geotrellis.vector._
import scalaz._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import vectorpipe.osm._

// --- //

/** Clipping Strategies.
  * Remember that `identity` is also technically a valid Clipping Strat. */
object Clip {

  private type Acc = (List[Point], List[Line])
  private type ClipState[T] = State[Acc, T]

  /** For any segment of a [[Line]] that extends outside the Extent,
    * clip directly on its nearest Point to the outside edge of that Extent.
    *
    * @see [[https://github.com/geotrellis/vectorpipe/issues/11]]
    */
  def toNearestPoint(extent: Extent, line: Line): MultiLine = ???

  /* Clipping with manual recursion */
  def toNearestPointR(extent: Extent, line: Line): MultiLine = {
    @tailrec def work(
      ps: Array[Point],
      last: Point,
      acc: List[Point],
      lines: List[Line]
    ): (List[Line], Point, List[Point]) = last match {

      /* We've reached the end of the Line */
      case _ if ps.isEmpty => (lines, last, acc)

      /* First condition: The current Point is within the Extent.
       * Regardless of where the previous Point was, we want to keep it:
       *   In  -> In : We're inside the Extent still.
       *   Out -> In : We were outside, now moving in.
       *
       * Second condition: We've moved outside the Extent.
       */
      case l if extent.intersects(ps.head) || extent.intersects(l) =>
        work(ps.tail, ps.head, l :: acc, lines)

      /* We've moved further away from the first Point outside the Extent */
      case l if acc.nonEmpty => work(ps.tail, ps.head, Nil, Line(l :: acc) :: lines)

      /* We're moving along a segment of external Points. */
      case _ => work(ps.tail, ps.head, acc, lines)

    }

    /* ASSUMPTION: This has length > 0 */
    val points: Array[Point] = line.points

    val allLines = work(points.tail, points.head, Nil, Nil) match {
      /* No need to check if `last` is actually in the Extent.
       * If `acc` is non-empty, we know them all to be `In` Points,
       * in which case `last` would be added regardless.
       */
      case (lines, last, acc) if acc.nonEmpty => Line(last :: acc) :: lines
      case (lines, _, _) => lines
    }

    MultiLine(allLines)
  }

  /* Clipping with foldLeftM */
  def toNearestPointF(extent: Extent, line: Line): MultiLine = {
    val points: Array[Point] = line.points

    val allLines = ImmutableArray.fromArray(points.tail).foldLeftM[ClipState, Point](points.head)({

      /* First condition: The current Point is within the Extent.
       * Regardless of where the previous Point was, we want to keep it:
       *   In  -> In : We're inside the Extent still.
       *   Out -> In : We were outside, now moving in.
       *
       * Second condition: We've moved outside the Extent.
       */
      case (l,p) if extent.intersects(p) || extent.intersects(l) =>
        State.modify[Acc]({ case (acc, lines) => (l :: acc, lines) }) >> p.pure[ClipState]

      case (l,p) => State.get[Acc].flatMap({
        /* We've moved further away from the first Point outside the Extent */
        case (acc, lines) if acc.nonEmpty =>
          State.put[Acc]((Nil, Line(l :: acc) :: lines)) >> p.pure[ClipState]

        /* We're moving along a segment of external Points. The very first
         * Point being outside the Extent will also trigger this.
         */
        case _ => p.pure[ClipState]
      })
    }).run((Nil, Nil)) match {
      case ((acc, lines), last) if acc.nonEmpty => Line(last :: acc) :: lines
      case ((_, lines), _) => lines
    }

    MultiLine(allLines)
  }

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

  /** Yield an [[OSMFeature]] as-is. */
  def asIs(extent: Extent, f: OSMFeature): OSMFeature = f
}
