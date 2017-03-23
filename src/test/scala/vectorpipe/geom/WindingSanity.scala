package vectorpipe.geom

import scala.collection.mutable.ListBuffer

import geotrellis.vector._
import geotrellis.vectortile.internal._
import org.scalatest._
import vectorpipe.vectortile._

// --- //

class WindingSanity extends FunSpec with Matchers {
  /** Get the area of a Polygon via the Surveyor Formula. */
  def area(p: Polygon): Double = {
    val buff = new ListBuffer[(Int,Int)]
    buff.appendAll(p.exterior.points.map(p => (p.x.toInt, p.y.toInt)))

    surveyor(buff)
  }

  describe("Surveyor Formula Behaviour") {
    it("Clockwise Poly has positive area") {
      val p = Polygon((1,1), (2,1), (2,2), (1,2), (1,1))

      assert(area(p) > 0)
    }

    it("Counterclockwise Poly has negative area") {
      val p = Polygon((1,1), (1,2), (2,2), (2,1), (1,1))

      assert(area(p) < 0)
    }
  }

  describe("Winding Order via Polygon.normalized") {
    it("Already clockwise should still be clockwise") {
      val p = Polygon((1,1), (2,1), (2,2), (1,2), (1,1))

      assert(area(winding(p)) > 0)
    }

    it("Counterclockwise should be clockwise") {
      val p = Polygon((1,1), (1,2), (2,2), (2,1), (1,1))

      assert(area(winding(p)) > 0)
    }

    it("Holed poly should have positive exterior, negative interior") {
      val p = Polygon(
        exterior = Line((1,1), (8,1), (8,4), (1,4), (1,1)),
        holes =
          Line((2,2), (2,3), (3,3), (3,2), (2,2)),
          Line((4,2), (4,3), (5,3), (5,2), (4,2)),
          Line((6,2), (6,3), (7,3), (7,2), (6,2))
      )
      val n = winding(p)

      assert(area(n) > 0)
      assert(area(Polygon(n.holes.head)) < 0)
    }
  }
}
