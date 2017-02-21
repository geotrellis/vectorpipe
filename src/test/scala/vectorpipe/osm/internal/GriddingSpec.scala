package vectorpipe.osm.internal

import vectorpipe.osm.internal.Gridding._

import org.scalatest._

// --- //

class GriddingSpec extends FunSpec with Matchers {
  describe("Powers of 2") {
    it("isPow2") {
      (1 to 10).map(n => isPow2(n)) shouldBe Seq(true, true, false, true, false, false, false, true, false, false)
    }

    it("pow2Ceil") {
      (1 to 10).map(n => pow2Ceil(n)) shouldBe Seq(1, 2, 4, 4, 8, 8, 8, 8, 16, 16)
    }
  }
}
