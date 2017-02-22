package vectorpipe.osm.internal

import geotrellis.spark.tiling._
import geotrellis.spark._
import geotrellis.vector.Extent

// --- //

/** [[KeyBounds]] whose grid dimensions are known to be powers of 2, with
  * an associated [[Extent]].
  *
  * Do not instantiate this class yourself. Use [[Gridding.inflate]] instead.
  * Assumes the input to be sane - i.e. no zero-sized TileLayouts.
  */
case class Pow2Layout(kb: KeyBounds[SpatialKey], extent: Extent) {
  /** The number of columns in this Layout. */
  lazy val cols: Int = kb.maxKey.col - kb.minKey.col + 1

  /** The number of rows in this Layout. */
  lazy val rows: Int = kb.maxKey.row - kb.minKey.row + 1

  /** Is this [[Pow2Layout]] a 1-by-1 square? */
  def isUnit: Boolean = cols == 1 && rows == 1

  /** If possible, reduce this `Pow2Layout` to children who are also Pow2Layout. */
  def reduction: Seq[Pow2Layout] = (cols, rows) match {
    case (1, 1) => Seq.empty[Pow2Layout] /* Cannot reduce further */
    case (c, r) if c == r => squareReduction
    case (c ,r) if c > r => horizontalReduction
    case _ => verticalReduction
  }

  /** Split this Pow2Layout into four equal parts. */
  private def squareReduction: Seq[Pow2Layout] = {
    /* The size of a new child Extent */
    val Δx: Double = (extent.xmax - extent.xmin) / 2
    val Δy: Double = (extent.ymax - extent.ymin) / 2

    /* The size of a new child KeyBounds. Rows and cols assumed to be equal. */
    val Δb: Int = cols / 2

    val topLeft = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.maxKey.col - Δb, kb.maxKey.row - Δb)),
      Extent(extent.xmin, extent.ymin + Δy, extent.xmax - Δx, extent.ymax)
    )

    val topRight = Pow2Layout(
      KeyBounds(
        SpatialKey(kb.minKey.col + Δb, kb.minKey.row),
        SpatialKey(kb.maxKey.col, kb.maxKey.row - Δb)
      ),
      Extent(extent.xmin + Δx, extent.ymin + Δy, extent.xmax, extent.ymax)
    )

    val bottomLeft = Pow2Layout(
      KeyBounds(
        SpatialKey(kb.minKey.col, kb.minKey.row + Δb),
        SpatialKey(kb.maxKey.col - Δb, kb.maxKey.row)
      ),
      Extent(extent.xmin, extent.ymin, extent.xmax - Δx, extent.ymax - Δy)
    )

    val bottomRight = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col + Δb, kb.minKey.row + Δb), kb.maxKey),
      Extent(extent.xmin + Δx, extent.ymin, extent.xmax, extent.ymax - Δy)
    )

    Seq(topLeft, topRight, bottomLeft, bottomRight)
  }

  /** Cut this Layout into Left and Right child pieces.
    * Child dimensions are still powers of two.
    */
  private def horizontalReduction: Seq[Pow2Layout] = {
    /* Measures of horizontal change */
    val Δx: Double = (extent.xmax - extent.xmin) / 2
    val Δb: Int = cols / 2

    val left = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.maxKey.col - Δb, kb.maxKey.row)),
      Extent(extent.xmin, extent.ymin, extent.xmax - Δx, extent.ymax)
    )

    val right = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col + Δb, kb.minKey.row), kb.maxKey),
      Extent(extent.xmin + Δx, extent.ymin, extent.xmax, extent.ymax)
    )

    Seq(left, right)
  }

  /** Cut this Layout into Top and Bottom child pieces.
    * Child dimensions are still powers of two.
    */
  private def verticalReduction: Seq[Pow2Layout] = {
    /* Measures of vertical change */
    val Δy: Double = (extent.ymax - extent.ymin) / 2
    val Δb: Int = ((kb.maxKey.row - kb.minKey.row) / 2) + 1

    val top = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.maxKey.col, kb.maxKey.row - Δb)),
      Extent(extent.xmin, extent.ymin + Δy, extent.xmax, extent.ymax)
    )

    val bot = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col, kb.minKey.row + Δb), kb.maxKey),
      Extent(extent.xmin, extent.ymin, extent.xmax, extent.ymax - Δy)
    )

    Seq(top, bot)
  }
}

object Pow2Layout {

    /**
    * Expands the dimensions of a [[LayoutDefinition]] such that its
    * [[TileLayout]] dimensions are powers of 2. The [[Extent]] is expanded to
    * match this as well.
    */
  def inflate(ld: LayoutDefinition): Pow2Layout = {

    /* --- Inflate the Tile Grid --- */

    /* Note that this doesn't necessarily expand the Tile grid into a square,
     * only to some rectangle whose side lengths are both perfect squares.
     */

    val inflatedCols: Int = pow2Ceil(ld.layoutCols)
    val inflatedRows: Int = pow2Ceil(ld.layoutRows)

    /* Example: If the inflated Layout is to be 16x16, its KeyBounds
     * should be from (0,0) to (15,15).
     */
    val bounds: KeyBounds[SpatialKey] = KeyBounds(
      SpatialKey(0, 0), SpatialKey(inflatedCols - 1, inflatedRows - 1)
    )

    /* --- Inflate the Extent --- */

    /* How many cols and rows did the Layout increase by? */
    val colDiff: Int = inflatedCols - ld.layoutCols
    val rowDiff: Int = inflatedRows - ld.layoutRows

    /* How many Extent-units should the Extent increase by? */
    val dim: Extent = ld.mapTransform(SpatialKey(0, 0)) /* The dimensions of one grid tile */
    val xDelta: Double = Math.abs(dim.xmax - dim.xmin)
    val yDelta: Double = Math.abs(dim.ymax - dim.ymin)

    val extent: Extent = Extent(
      ld.extent.xmin, ld.extent.ymin - (rowDiff * yDelta),
      ld.extent.xmax + (colDiff * xDelta), ld.extent.ymax
    )

    Pow2Layout(bounds, extent)
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
