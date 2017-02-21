package vectorpipe.osm.internal

import geotrellis.raster.TileLayout
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import org.scalatest._

// --- //

class Pow2LayoutSpec extends FunSpec with Matchers {
  val p2l = new Pow2Layout(LayoutDefinition(
    Extent(0, 0, 160, 160),
    TileLayout(16, 16, 10, 10)
  ))

  describe("Pow2Layout") {
    it("reduction - once") {
      p2l.reduction.length shouldBe 4
    }

    it("reduction - twice") {
      p2l.reduction.flatMap(p => p.reduction).length shouldBe 16
    }

    it("reduction - thrice") {
      p2l.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction).length shouldBe 64
    }

    it("reduction - four times") {
      p2l.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction).flatMap(p => p.reduction).length shouldBe 256
    }

    it("reduction - 1x1 LayoutDefinition") {
      p2l.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction).flatMap(p => p.reduction).flatMap(p => p.reduction).length shouldBe 0
    }
  }
}
