package vectorpipe.util

import org.scalatest._
import spire.std.any._

// --- //

class GraphSpec extends FunSpec with Matchers {
  describe("Graph Construction") {
    it("Empty graph") {
      val g = Graph.fromEdges(Seq())

      g.size shouldBe 0
    }

    it("Singleton Graph") {
      val g = Graph.fromEdges(Seq((0, "hi", Seq())))

      g.size shouldBe 1
      g.get(0) shouldBe Some("hi")
    }

    it("Connected Graph") {
      val g = Graph.fromEdges(Seq(
        (1, 'a', Seq(2,4)),
        (2, 'b', Seq(3)),
        (3, 'c', Seq(6, 7)),
        (4, 'd', Seq(5)),
        (5, 'e', Seq(7)),
        (6, 'f', Seq()),
        (7, 'g', Seq())
      ))

      g.size shouldBe 7
      g.get(7) shouldBe Some('g')
    }

    it("Disconnected Graph") {
      val g = Graph.fromEdges(Seq(
        (1, 'a', Seq(2,4)),
        (2, 'b', Seq(3)),
        (3, 'c', Seq(6, 7)),
        (4, 'd', Seq(5)),
        (5, 'e', Seq(7)),
        (6, 'f', Seq()),
        (7, 'g', Seq()),
        (8, 'h', Seq(9, 10)),
        (9, 'i', Seq(10)),
        (10, 'j', Seq())
      ))

      g.size shouldBe(10)
      g.get(1) shouldBe Some('a')
      g.get(7) shouldBe Some('g')
      g.get(10) shouldBe Some('j')
    }
  }

  describe("Graph Algorithms") {
    val g = Graph.fromEdges(Seq(
      (1, 1, Seq(2,3)),
      (2, 2, Seq(4)),
      (3, 3, Seq(4)),
      (4, 4, Seq()),
      (5, 5, Seq(6, 7)),
      (6, 6, Seq(8)),
      (7, 7, Seq(8)),
      (8, 8, Seq())
    ))

    it("Topological Sort") {
      g.topSort.map(v => g.node(v)._1) shouldBe Seq(5,7,6,8,1,3,2,4)
    }

    /*
    it("Topological Forest") {
      g.topologicalForest.foreach(t => println(t.pretty))
    }
     */

    it("Baarle-Nassau Topological Forest") {
      val bng = Graph.fromEdges(Seq(
        (52411, 52411, Seq(53136, 53134)),
        (53136, 53136, Seq(53134)),
        (53134, 53134, Seq(53114)),
        (53114, 53114, Seq(2524404)),
        (2524404, 2524404, Seq(53137)),
        (53137, 53137, Seq())
      ))

      bng.topologicalForest.head.preorder shouldBe Seq(52411, 53136, 53134, 53114, 2524404, 53137)
    }
  }
}
