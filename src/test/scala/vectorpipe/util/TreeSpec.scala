package vectorpipe.util

import org.scalatest._
import scalaz.syntax.functor._

// --- //

class TreeSpec extends FunSpec with Matchers {
  val tree: Tree[Char] = Tree('F', Seq(
    Tree('B', Seq(
      Tree('A', Seq()),
      Tree('D', Seq(
        Tree('C', Seq()),
        Tree('E', Seq())
      ))
    )),
    Tree('G', Seq(
      Tree('I', Seq(
        Tree('H', Seq())
      ))
    ))
  ))

  describe("Tree Traversal") {
    it("Pre-order") { tree.preorder shouldBe "FBADCEGIH".toSeq }
    it("Post-order") { tree.postorder shouldBe "ACEDBHIGF".toSeq }
  }

  describe("Tree Typeclasses") {
    it("Functor") {
      val t = Tree(1, Seq(
        Tree(2, Seq()),
        Tree(3, Seq())
      ))

      val t2 = Tree(2, Seq(
        Tree(3, Seq()),
        Tree(4, Seq())
      ))

      t.map(_ + 1) shouldBe t2
    }
  }
}
