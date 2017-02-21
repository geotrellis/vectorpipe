package vectorpipe.osm.internal

import geotrellis.raster.TileLayout
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent

import scalaz.std.stream._
import scalaz.syntax.applicative._

// --- //

/** A [[LayoutDefinition]] whose grid dimensions are known to be powers of 2.
  *
  * Do not instantiate this class yourself. Use [[Gridding.inflate]] instead.
  */
class Pow2Layout(val ld: LayoutDefinition) extends AnyVal {
  /** If possible, reduce this `Pow2Layout` to children who are also Pow2Layout. */
  def reduction: Seq[Pow2Layout] = (ld.layoutCols, ld.layoutRows) match {
    case (1, 1) => Seq.empty[Pow2Layout] /* Cannot reduce further */
    case (c, r) if c == r => squareReduction
    case (c ,r) if c > r => horizontalReduction
    case _ => verticalReduction
  }

  /** Split this Pow2Layout into four equal parts. */
  private def squareReduction: Seq[Pow2Layout] = {
    val tl = TileLayout(ld.layoutCols / 2, ld.layoutRows / 2, ld.tileCols, ld.tileRows)

    /* The size of a new child Extent */
    val xDelta: Double = (ld.extent.xmax - ld.extent.xmin) / 2
    val yDelta: Double = (ld.extent.ymax - ld.extent.ymin) / 2

    val topLeft = Extent(
      ld.extent.xmin, ld.extent.ymin + yDelta,
      ld.extent.xmax - xDelta, ld.extent.ymax
    )

    val topRight = Extent(
      ld.extent.xmin + xDelta, ld.extent.ymin + yDelta,
      ld.extent.xmax, ld.extent.ymax
    )

    val bottomLeft = Extent(
      ld.extent.xmin, ld.extent.ymin,
      ld.extent.xmax - xDelta, ld.extent.ymax - yDelta
    )

    val bottomRight = Extent(
      ld.extent.xmin + xDelta, ld.extent.ymin,
      ld.extent.xmax, ld.extent.ymax - yDelta
    )

    Seq(
      new Pow2Layout(LayoutDefinition(topLeft, tl)),
      new Pow2Layout(LayoutDefinition(topRight, tl)),
      new Pow2Layout(LayoutDefinition(bottomLeft, tl)),
      new Pow2Layout(LayoutDefinition(bottomRight, tl))
    )
  }

  /** Cut this Layout into Left and Right child pieces.
    * Child dimensions are still powers of two.
    */
  private def horizontalReduction: Seq[Pow2Layout] = {
    val tl = TileLayout(ld.layoutCols / 2, ld.layoutRows, ld.tileCols, ld.tileRows)
    val xDelta: Double = (ld.extent.xmax - ld.extent.xmin) / 2
    val left = Extent(ld.extent.xmin, ld.extent.ymin, ld.extent.xmax - xDelta, ld.extent.ymax)
    val right = Extent(ld.extent.xmin + xDelta, ld.extent.ymin, ld.extent.xmax, ld.extent.ymax)

    Seq(
      new Pow2Layout(LayoutDefinition(left, tl)),
      new Pow2Layout(LayoutDefinition(right, tl))
    )
  }

  /** Cut this Layout into Top and Bottom child pieces.
    * Child dimensions are still powers of two.
    */
  private def verticalReduction: Seq[Pow2Layout] = {
    val tl = TileLayout(ld.layoutCols, ld.layoutRows / 2, ld.tileCols, ld.tileRows)
    val yDelta: Double = (ld.extent.ymax - ld.extent.ymin) / 2
    val top = Extent(ld.extent.xmin, ld.extent.ymin + yDelta, ld.extent.xmax, ld.extent.ymax)
    val bot = Extent(ld.extent.xmin, ld.extent.ymin, ld.extent.xmax, ld.extent.ymax - yDelta)

    Seq(
      new Pow2Layout(LayoutDefinition(top, tl)),
      new Pow2Layout(LayoutDefinition(bot, tl))
    )
  }
}

/**
  * Internal mechanics for gridding a collection of [[OSMFeature]].
  */
object Gridding {

  /** Create the physical grid of [[SpatialKey]]s. */
  def grid(ld: LayoutDefinition): Stream[SpatialKey] = {
    val maxCols = ld.tileLayout.layoutCols
    val maxRows = ld.tileLayout.layoutRows

    (Stream.range(0, maxCols) |@| Stream.range(0, maxRows)) { SpatialKey(_, _) }
  }

  /**
    * Expands the dimensions of a [[LayoutDefinition]] such that its
    * [[TileLayout]] dimensions are powers of 2. The [[Extent]] is expanded to
    * match this as well.
    */
  def inflate(ld: LayoutDefinition): Pow2Layout = {

    /* --- Inflate the TileLayout --- */

    /* Note that this doesn't necessarily expand the Tile grid into a square,
     * only to some rectangle whose side lengths are both perfect squares.
     */
    val tileLayout: TileLayout = TileLayout(
      pow2Ceil(ld.layoutCols), pow2Ceil(ld.layoutRows),
      ld.tileCols, ld.tileRows
    )

    /* --- Inflate the Extent --- */

    /* How many cols and rows did the Layout increase by? */
    val colDiff: Int = tileLayout.layoutCols - ld.layoutCols
    val rowDiff: Int = tileLayout.layoutRows - ld.layoutRows

    /* How many Extent-units should the Extent increase by? */
    val dim: Extent = ld.mapTransform(SpatialKey(0, 0)) /* The dimensions of one grid tile */
    val xDelta: Double = Math.abs(dim.xmax - dim.xmin)
    val yDelta: Double = Math.abs(dim.ymax - dim.ymin)

    val extent: Extent = Extent(
      ld.extent.xmin, ld.extent.ymin - (rowDiff * yDelta),
      ld.extent.xmax + (colDiff * xDelta), ld.extent.ymax
    )

    new Pow2Layout(LayoutDefinition(extent, tileLayout))
  }

  /** The next power of two after the given value. Yields the value itself
    * if it is a power of two.
    */
  /* Adapted from: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2 */
  def pow2Ceil(n: Int): Int = n match {
    case n if isPow2(n) => n
    case n => 1 << (1 + Math.floor(Math.log(n.toDouble) / Math.log(2)).toInt)
  }

  /** Is the given Int a power of two? */
  def isPow2(n: Int): Boolean = (n & (n - 1)) == 0

}
