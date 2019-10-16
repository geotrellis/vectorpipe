package vectorpipe.vectortile

import geotrellis.vector._
import geotrellis.layer._
import org.locationtech.jts.simplify.TopologyPreservingSimplifier

object Simplify {

  /**
   * Simplifies geometry using JTS's topology-preserving simplifier.
   *
   * Note that there are known bugs with this simplifier.  Please refer to the
   * JTS documentation.  Faster simplifiers with fewer guarantees are available
   * there as well.
   */
  def withJTS(g: Geometry, ld: LayoutDefinition): Geometry = {
    TopologyPreservingSimplifier.simplify(g, ld.cellSize.resolution)
  }

}
