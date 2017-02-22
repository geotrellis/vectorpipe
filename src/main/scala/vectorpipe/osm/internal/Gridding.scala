package vectorpipe.osm.internal

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent

// --- //

/**
  * Internal mechanics for gridding a collection of [[OSMFeature]].
  */
object Gridding {

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
