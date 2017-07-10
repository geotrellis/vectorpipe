package vectorpipe

import java.time.ZonedDateTime

import geotrellis.vector._
import org.scalatest._
import vectorpipe.osm._
import vectorpipe.util.Tree

// --- //

class CollateSpec extends FunSpec with Matchers {
  val extent: Extent = Extent(0, 0, 1000, 1000)

  val geom: Polygon = Polygon(
    exterior = Line((1,1), (8,1), (8,4), (1,4), (1,1)),
    holes = Seq(
      Line((2,2), (2,3), (3,3), (3,2), (2,2)),
      Line((4,2), (4,3), (5,3), (5,2), (4,2)),
      Line((6,2), (6,3), (7,3), (7,2), (6,2))
    )
  )

  val data0 = ElementData(
    ElementMeta(1037, "colin", "8765", 5, 1, ZonedDateTime.now, true),
    Map("object" -> "flagpole"),
    None
  )

  val data1 = ElementData(
    ElementMeta(10000, "colin", "8765", 5, 1, ZonedDateTime.now, true),
    Map("route" -> "footpath"),
    None
  )

  val data2 = ElementData(
    ElementMeta(100123, "colin", "8765", 5, 1, ZonedDateTime.now, true),
    Map("place" -> "park"),
    None
  )

  describe("Collation Functions") {
    it("withoutMetadata") {
      Collate.withoutMetadata(extent, Seq(Feature(geom, 1)))
    }

    it("byAnalytics") {
      val tree = Tree(data0, Seq(Tree.singleton(data1), Tree.singleton(data2)))

      //      println(tree.map(_.meta.id).pretty)

      Collate.byAnalytics(extent, Seq(Feature(geom, tree)))
    }
  }
}
