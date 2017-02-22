package vectorpipe.osm.internal

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
    val ex_Δ: Double = (extent.xmax - extent.xmin) / 2
    val ey_Δ: Double = (extent.ymax - extent.ymin) / 2

    /* The size of a new child KeyBounds */
    // TODO Only use one, because they should be equal?
    val bx_Δ: Int = cols / 2
    val by_Δ: Int = rows / 2

    val topLeft = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.maxKey.col - bx_Δ, kb.maxKey.row - by_Δ)),
      Extent(extent.xmin, extent.ymin + ey_Δ, extent.xmax - ex_Δ, extent.ymax)
    )

    val topRight = Pow2Layout(
      KeyBounds(
        SpatialKey(kb.minKey.col + bx_Δ, kb.minKey.row),
        SpatialKey(kb.maxKey.col, kb.maxKey.row - by_Δ)
      ),
      Extent(extent.xmin + ex_Δ, extent.ymin + ey_Δ, extent.xmax, extent.ymax)
    )

    val bottomLeft = Pow2Layout(
      KeyBounds(
        SpatialKey(kb.minKey.col, kb.minKey.row + by_Δ),
        SpatialKey(kb.maxKey.col - bx_Δ, kb.maxKey.row)
      ),
      Extent(extent.xmin, extent.ymin, extent.xmax - ex_Δ, extent.ymax - ey_Δ)
    )

    val bottomRight = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col + bx_Δ, kb.minKey.row + by_Δ), kb.maxKey),
      Extent(extent.xmin + ex_Δ, extent.ymin, extent.xmax, extent.ymax - ey_Δ)
    )

    Seq(topLeft, topRight, bottomLeft, bottomRight)
  }

  /** Cut this Layout into Left and Right child pieces.
    * Child dimensions are still powers of two.
    */
  private def horizontalReduction: Seq[Pow2Layout] = {
    /* Measures of horizontal change */
    val ex_Δ: Double = (extent.xmax - extent.xmin) / 2
    val bx_Δ: Int = ((kb.maxKey.col - kb.minKey.col) / 2) + 1

    val left = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.minKey.col + bx_Δ, kb.maxKey.row)),
      Extent(extent.xmin, extent.ymin, extent.xmax - ex_Δ, extent.ymax)
    )

    val right = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col + bx_Δ, kb.minKey.row), kb.maxKey),
      Extent(extent.xmin + ex_Δ, extent.ymin, extent.xmax, extent.ymax)
    )

    Seq(left, right)
  }

  /** Cut this Layout into Top and Bottom child pieces.
    * Child dimensions are still powers of two.
    */
  private def verticalReduction: Seq[Pow2Layout] = {
    /* Measures of vertical change */
    val ey_Δ: Double = (extent.ymax - extent.ymin) / 2
    val by_Δ: Int = ((kb.maxKey.row - kb.minKey.row) / 2) + 1

    val top = Pow2Layout(
      KeyBounds(kb.minKey, SpatialKey(kb.maxKey.col, kb.minKey.row + by_Δ)),
      Extent(extent.xmin, extent.ymin + ey_Δ, extent.xmax, extent.ymax)
    )

    val bot = Pow2Layout(
      KeyBounds(SpatialKey(kb.minKey.col, kb.minKey.row + by_Δ), kb.maxKey),
      Extent(extent.xmin, extent.ymin, extent.xmax, extent.ymax - ey_Δ)
    )

    Seq(top, bot)
  }
}
