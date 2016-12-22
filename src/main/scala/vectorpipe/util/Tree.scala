package vectorpipe.util

import scalaz.{Functor, Applicative, Monad}
import scalaz.syntax.monad._

// --- //

/** A typical immutable Tree implementation, mysteriously absent from
  *  Scala's standard library.
  */
case class Tree[T](root: T, children: Seq[Tree[T]]) extends Serializable {
  /** The flattened elements of the tree in pre-order. */
  def preorder: Seq[T] = root +: children.flatMap(_.preorder)

  /** The flattened elements of the tree in post-order. */
  def postorder: Seq[T] = children.flatMap(_.postorder) :+ root

  /** The values of all leaves (nodes with no children) in the Tree. */
  def leaves: Seq[T] = {
    if (children.isEmpty) Seq(root) else children.flatMap(_.leaves)
  }

  /** A nicer `toString`. Adapted from: http://stackoverflow.com/a/8948691/643684 */
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

  /** Trees are Applicative Functors. */
  implicit val treeApplicative: Applicative[Tree] = new Applicative[Tree] {
    def point[A](a: => A): Tree[A] = singleton(a)

    def ap[A, B](fa: => Tree[A])(tf: => Tree[A => B]): Tree[B] = {
      val Tree(f, tfs) = tf

      Tree(f(fa.root), fa.children.map(t => t.map(f)) ++ tfs.map(t => fa <*> t))
    }
  }

  /** Trees are also Monads. */
  implicit val treeMonad: Monad[Tree] = new Monad[Tree] {
    def point[A](a: => A): Tree[A] = treeApplicative.point(a)

    def bind[A, B](fa: Tree[A])(f: A => Tree[B]): Tree[B] = {
      val Tree(r2, k2) = f(fa.root)

      Tree(r2, k2 ++ fa.children.map(t => t >>= f))
    }
  }
}
