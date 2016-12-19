package vectorpipe.util

import org.scalatest._

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
}
