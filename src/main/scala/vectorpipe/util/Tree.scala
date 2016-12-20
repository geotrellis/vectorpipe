package vectorpipe.util

import scalaz.Functor
import scalaz.syntax.functor._

// --- //

/** A typical immutable Tree implementation, mysteriously absent from
  *  Scala's standard library.
  */
case class Tree[T](root: T, children: Seq[Tree[T]]) {
  /** The elements of the tree in pre-order. */
  def preorder: Seq[T] = root +: children.flatMap(_.preorder)

  def postorder: Seq[T] = children.flatMap(_.postorder) :+ root

    /* Adapted from: http://stackoverflow.com/a/8948691/643684 */
  def pretty: String = {
    def work(tree: Tree[T], prefix: String, isTail: Boolean): String = {
      val (line, bar) = if (isTail) ("└── ", " ") else ("├── ", "│")

      val curr = s"${prefix}${line}${tree.root}"

      val kids: String = if (tree.children.isEmpty) {
        s"${prefix}${bar}   └── ∅"
      } else {
        val firsts = tree.children.init.map(t => work(t, s"${prefix}${bar}   ", false))
        val last = work(tree.children.last, s"${prefix}${bar}   ", true)

        (firsts :+ last).mkString("\n")
      }

      s"${curr}\n${kids}"
    }

    work(this, "", true)
  }
}

object Tree {
  /** Construct a Tree with a single node and no children. */
  def singleton[T](t: T): Tree[T] = Tree(t, Seq.empty[Tree[T]])

  /** Trees are mappable. */
  implicit val treeFunctor: Functor[Tree] = new Functor[Tree] {
    def map[A, B](fa: Tree[A])(f: A => B): Tree[B] = Tree(
      f(fa.root),
      fa.children.map(t => t.map(f))
    )
  }
}
