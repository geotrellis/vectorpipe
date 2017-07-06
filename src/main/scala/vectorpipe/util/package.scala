package vectorpipe

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector.Polygon

package object util {

  /** An abstract representation of a vertex. */
  type Vertex = Int

  /** A common alias for a collection of [[Tree]]s. */
  type Forest[T] = Seq[Tree[T]]

  /** Ensure a [[geotrellis.vector.Polygon]] has the correct winding order
    * to be used in a [[VectorTile]].
    */
  def winding(p: Polygon): Polygon = {
    /* `normalize` works in-place, so we clone first to avoid clobbering the
     * GT Polygon.
     */
    val geom = p.jtsGeom.clone.asInstanceOf[jts.Polygon]
    geom.normalize

    /* `normalize` makes exteriors run clockwise and holes run
     * counter-clockwise, but assuming that (0,0) is in the bottom left. VTs assume
     * (0,0) is in the top-left, so we need to reverse the results of the
     * normalization.
     */
    Polygon(geom.reverse().asInstanceOf[jts.Polygon])
  }

  /** Mysteriously missing from the Scala Standard Library. */
  def zip3[A,B,C](a: Stream[A], b: Stream[B], c: Stream[C]): Stream[(A,B,C)] = {
    if (a.isEmpty || b.isEmpty || c.isEmpty) Stream.empty else {
      (a.head, b.head, c.head) #:: zip3(a.tail, b.tail, c.tail)
    }
  }

  /** Mysteriously missing from the Scala Standard Library. */
  def uncurry3[A,B,C,D](f: (A,B,C) => D): ((A,B,C)) => D = { case (a,b,c) => f(a,b,c) }
}
