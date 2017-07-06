package vectorpipe

import geotrellis.vector._
import org.scalatest._

// --- //

class ClipSpec extends FunSpec with Matchers {
  val extent = Extent(0, 0, 5, 5)

  describe("toNearestPoint - Java Style") {
    it("all in") {
      val line = Line(Point(1,1), Point(2,2), Point(3,3))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(line)
    }

    it("one side out") {
      val line = Line(Point(1,1), Point(2,2), Point(3,3), Point(7,7), Point(8,8), Point(9,9))
      val expected = Line(Point(1,1), Point(2,2), Point(3,3), Point(7,7))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(expected)
    }

    it("both sides out") {
      val line = Line(Point(-5,-5), Point(-1,-1), Point(1,1), Point(2,2), Point(3,3), Point(7,7), Point(8,8), Point(9,9))
      val expected = Line(Point(-1,-1), Point(1,1), Point(2,2), Point(3,3), Point(7,7))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(expected)
    }

    /* Brief exits of the Extent shouldn't result in a split */
    it("in - out - in") {
      val line = Line(Point(2,3), Point(-1,3), Point(2,2))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(line)
    }

    it("loop out and back in") {
      val line = Line(
        Point(4,1), Point(2,1), Point(-1,1), Point(-3,1),
        Point(-3,4), Point(-1,4), Point(2,4), Point(4,4)
      )
      val expected = MultiLine(
        Line(Point(4,1), Point(2,1), Point(-1,1)),
        Line(Point(-1,4), Point(2,4), Point(4,4))
      )

      Clip.toNearestPoint(extent, line) shouldBe expected

    }

    /* Multiple brief exits of the Extent shouldn't result in a split */
    it("star") {
      val line = Line(Point(3,2), Point(4,7), Point(5,3), Point(9,3), Point(4,1))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(line)
    }

    /* The Line has no Points in this Extent, but passes through it */
    it("pass through") {
      val line = Line(Point(-2,-2), Point(-1,-1), Point(6,6), Point(7,7))
      val expected = Line(Point(-1,-1), Point(6,6))

      Clip.toNearestPoint(extent, line) shouldBe MultiLine(expected)
    }
  }
}
