package vectorpipe.osm

import geotrellis.vector._
import org.scalatest._

// --- //

class ClippingSanity extends FunSpec with Matchers {
  /* A projectless extent for varifying clipping behaviour in JTS */
  val extent: Polygon = Extent(0, 0, 5, 5).toPolygon()

  describe("Points") {
    it("inside") {
      val p = Point(2.5, 2.5)
      val r: Option[Point] = p.intersection(extent).as[Point]

      r shouldBe Some(p)
    }

    it("border") {
      val p = Point(0, 2)

      p.intersection(extent).as[Point] shouldBe Some(p)
    }

    it("corner") {
      val p = Point(0, 0)

      p.intersection(extent).as[Point] shouldBe Some(p)
    }

    it("outside") {
      Point(10, 10).intersection(extent).as[Point] shouldBe None
    }
  }

  describe("Lines") {
    it("completely inside") {
      val l = Line(Point(1,1), Point(2,2))
      val r: Option[Line] = l.intersection(extent).as[Line]

      r shouldBe Some(l)
    }

    it("1 in, 1 border") {
      val l = Line(Point(1,1), Point(5,5))

      l.intersection(extent).as[Line] shouldBe Some(l)
    }

    it("1 in, 1 out") {
      val l = Line(Point(1,1), Point(1,10))
      val r = Line(Point(1,1), Point(1,5))  /* Expected result */

      l.intersection(extent).as[Line] shouldBe Some(r)
    }

    it("1 in, 2 out, passing through") {
      val l = Line(Point(1,-1), Point(1,2), Point(1,10))
      val r = Line(Point(1,0), Point(1,2), Point(1,5))

      l.intersection(extent).as[Line] shouldBe Some(r)
    }

    it("2 out, passing through") {
      val l = Line(Point(1,-1), Point(1,10))
      val r = Line(Point(1,0), Point(1,5))

      l.intersection(extent).as[Line] shouldBe Some(r)
    }

    it("2 out, non-intersecting") {
      val l = Line(Point(-1,-1), Point(-1,-10))

      l.intersection(extent).as[Line] shouldBe None
    }

    /* Zig-zagging in and out of the Extent creates a MultiLine */
    it("zigzag: in-out-in") {
      val l = Line(Point(1,1), Point(1,6), Point(4,6), Point(4,1))
      val ml = MultiLine(
        Line(Point(1,1), Point(1,5)),
        Line(Point(4,5), Point(4,1))
      )

      l.intersection(extent).asMultiLine shouldBe Some(ml)
    }
  }

  describe("Solid Polygons") {
    it("completely inside") {
      val p = Polygon(Line(
        Point(1,1), Point(1,2), Point(2,2), Point(2,1), Point(1,1)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(p)
    }

    it("one side out") {
      val p = Polygon(Line(
        Point(2,2), Point(2,3), Point(7,3), Point(7,2), Point(2,2)
      ))

      val r = Polygon(Line(
        Point(2,2), Point(2,3), Point(5,3), Point(5,2), Point(2,2)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(r)
    }

    it("two sides out") {
      val p = Polygon(Line(
        Point(-2,2), Point(-2,3), Point(7,3), Point(7,2), Point(-2,2)
      ))

      val r = Polygon(Line(
        Point(0,2), Point(0,3), Point(5,3), Point(5,2), Point(0,2)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(r)
    }

    it("rectangle over bottom corners") {
      val p = Polygon(Line(
        Point(-1,1), Point(6,1), Point(6,-1), Point(-1,-1), Point(-1,1)
      ))

      val r = Polygon(Line(
        Point(0,1), Point(5,1), Point(5,0), Point(0,0), Point(0,1)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(r)
    }

    it("rectangle over NW/SE corners") {
      val p = Polygon(Line(
        Point(-1,5), Point(0,6), Point(6,0), Point(5,-1), Point(-1,5)
      ))

      val r = Polygon(Line(
        Point(0,4), Point(0,5), Point(1,5), Point(5,1), Point(5,0), Point(4,0), Point(0,4)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(r)
    }
  }

  describe("Holed Polygons") {
    it("overlapping corner") {
      val p = Polygon(
        exterior = Line(Point(3,3), Point(3,7), Point(7,7), Point(7,3), Point(3,3)),
        holes = Line(Point(4,4), Point(6,4), Point(6,6), Point(4,6), Point(4,4))
      )

      val r = Polygon(Line(
        Point(3,3), Point(3,5), Point(4,5), Point(4,4), Point(5,4), Point(5,3), Point(3,3)
      ))

      p.intersection(extent).as[Polygon] shouldBe Some(r)
    }
  }

}
