package vectorpipe

package object util {
  /** An abstract representation of a vertex. */
  type Vertex = Int

  /** A common alias for a collection of [[Tree]]s. */
  type Forest[T] = Seq[Tree[T]]
}
