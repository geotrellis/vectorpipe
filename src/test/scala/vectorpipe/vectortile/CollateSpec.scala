package vectorpipe.vectortile

import geotrellis.vector._
import org.scalatest._

// --- //

class CollateSpec extends FunSpec with Matchers {
  val extent: Extent = Extent(0, 0, 1000, 1000)

  val geoms = Seq(Feature(Polygon(
    exterior = Line((1,1), (8,1), (8,4), (1,4), (1,1)),
    holes =
      Line((2,2), (2,3), (3,3), (3,2), (2,2)),
    Line((4,2), (4,3), (5,3), (5,2), (4,2)),
    Line((6,2), (6,3), (7,3), (7,2), (6,2))
  ),
    1  // Toy metadata.
  ))

  describe("Collation Functions") {
    it("withoutMetadata") {
      Collate.withoutMetadata(extent, geoms)
    }
  }
}
