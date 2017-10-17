package vectorpipe.osm

import java.time.Instant

import org.scalatest._
import vectorpipe.osm.internal.PlanetHistory

// --- //

class FeatureConstruction extends FunSpec with Matchers {

  val wayMeta  = ElementMeta(1, "colin", "123abc", 1, 1, Instant.ofEpochMilli(10), true)
  val wayData  = ElementData(wayMeta, Map.empty)
  val nodeMeta = ElementMeta(2, "colin", "123abc", 1, 1, Instant.ofEpochMilli(5), true)
  val nodeData = ElementData(nodeMeta, Map.empty)
  val node1 = Node(90, 90, nodeData)
  val node2 = Node(91, 91, nodeData.copy(meta = nodeData.meta.copy(id = 3)))
  val node3 = Node(93, 94, nodeData.copy(meta = nodeData.meta.copy(id = 4)))

  /* Unordered on purpose, since the Spark step before `linesAndPolys` can't guarantee an order */
  val ways: List[Way] = List(
    Way(Vector(2,3,4), wayData.copy(meta = wayMeta.copy(version = 2, timestamp = Instant.ofEpochMilli(30)))),
    Way(Vector(2,3,4), wayData)
  )

  val locations: Map[Long, (Double, Double)] = Map((2L, (90d, 90d)), (3L, (91d, 91d)), (4L, (93d, 94d)))

  /* ID, version, time */
  val nodes: List[Node] = List(
    (2, 1, 5),  (3, 1, 5),  (4, 1, 5),
    (2, 2, 15),
    (3, 2, 20), (4, 2, 20),
    (2, 3, 25), (3, 3, 25), (4, 3, 25),
    (3, 4, 30),
    (2, 4, 35), (4, 4, 35)
  ).map { case (id, v, time) =>
      val (lat, lon) = locations(id.toLong)

      Node(lat, lon, ElementData(ElementMeta(id, "colin", "123abc", 1, v, Instant.ofEpochMilli(time), true), Map.empty))
  }

  describe("linesAndPolys") {
    it("should create all expected versions of a Line") {
      val (lines, polys) = PlanetHistory.linesAndPolys(ways, nodes)

      lines.length shouldBe 6
      polys.length shouldBe 0
    }
  }
}
