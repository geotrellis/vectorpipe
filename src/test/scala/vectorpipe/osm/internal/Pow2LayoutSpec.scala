package vectorpipe.osm.internal

import geotrellis.spark._
import geotrellis.vector.Extent
import org.scalatest._

// --- //

class Pow2LayoutSpec extends FunSpec with Matchers {
  val p2l = Pow2Layout(
    KeyBounds(SpatialKey(0,0), SpatialKey(15, 15)),
    Extent(0, 0, 160, 160)
  )

  val hor = Pow2Layout(
    KeyBounds(SpatialKey(0, 0), SpatialKey(15, 7)),
    Extent(0, 0, 160, 80)
  )

  val vert = Pow2Layout(
    KeyBounds(SpatialKey(0, 0), SpatialKey(7, 15)),
    Extent(0, 0, 80, 160)
  )

  def isSquare(r: Seq[Pow2Layout], expectedDim: Int): Unit = {
    /* Is each reduced area a square? */
    r.foreach(p => p.cols shouldBe p.rows)

    /* Is each area the proper size? */
    r.foreach({ p => p.cols shouldBe expectedDim; p.rows shouldBe expectedDim })
  }

  describe("Pow2Layout") {
    it("square reduction - once") {
      val r: Seq[Pow2Layout] = p2l.reduction

      r.length shouldBe 4
      isSquare(r, 8)
    }

    it("square reduction - twice") {
      val r = p2l.reduction.flatMap(p => p.reduction)

      r.length shouldBe 16
      isSquare(r, 4)
    }

    it("square reduction - thrice") {
      val r = p2l.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction)

      r.length shouldBe 64
      isSquare(r, 2)
    }

    it("square reduction - four times") {
      val r = p2l.reduction
        .flatMap(p => p.reduction)
        .flatMap(p => p.reduction)
        .flatMap(p => p.reduction)

      r.length shouldBe 256
      isSquare(r, 1)
    }

    it("square reduction - 1x1 Layout") {
      p2l.reduction
        .flatMap(p => p.reduction)
        .flatMap(p => p.reduction)
        .flatMap(p => p.reduction)
        .flatMap(p => p.reduction).length shouldBe 0
    }

    it("horizonal reduction - once") {
      val r = hor.reduction

      r.length shouldBe 2
      isSquare(r, 8)
    }

    it("horizonal reduction - twice") {
      val r = hor.reduction.flatMap(p => p.reduction)

      r.length shouldBe 8
      isSquare(r, 4)
    }

    it("horizonal reduction - thrice") {
      val r = hor.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction)

      r.length shouldBe 32
      isSquare(r, 2)
    }

    it("vertical reduction - once") {
      val r = vert.reduction

      r.length shouldBe 2
      isSquare(r, 8)
    }

    it("vertical reduction - twice") {
      val r = vert.reduction.flatMap(p => p.reduction)

      r.length shouldBe 8
      isSquare(r, 4)
    }

    it("vertical reduction - thrice") {
      val r = vert.reduction.flatMap(p => p.reduction).flatMap(p => p.reduction)

      r.length shouldBe 32
      isSquare(r, 2)
    }

  }
}
