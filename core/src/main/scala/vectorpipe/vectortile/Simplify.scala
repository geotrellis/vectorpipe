package vectorpipe.vectortile

import geotrellis.spark.tiling.LayoutDefinition
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.simplify.TopologyPreservingSimplifier

object Simplify {

  /**
   * Simplifies geometry using JTS's topology-preserving simplifier.
   *
   * Note that there are known bugs with this simplifier.  Please refer to the
   * JTS documentation.  Faster simplifiers with fewer guarantees are available
   * there as well.
   */
  def withJTS(g: jts.Geometry, ld: LayoutDefinition): jts.Geometry = {
    TopologyPreservingSimplifier.simplify(g, ld.cellSize.resolution)
  }

}
