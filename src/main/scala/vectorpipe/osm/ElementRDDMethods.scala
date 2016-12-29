package vectorpipe.osm

import vectorpipe.util._

import geotrellis.util._
import geotrellis.vector._

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.operation.linemerge.LineMerger
import org.apache.spark.rdd._
import scalaz.syntax.applicative._
import spire.std.any._

import scala.collection.parallel.ParSeq

// --- //

/* TODOS
 *
 * Hell, but what about relations that have mixed member types? (ways + other relations)
 * See Relation #47796. It has ways, single nodes, and a relation labelled a "subarea".
 *
 */

class ElementRDDMethods(val self: RDD[Element]) extends MethodExtensions[RDD[Element]] {
  /** All [[Relation]] [[Element]]s, not doctored in any way. */
  def rawRelations: RDD[Relation] = self.flatMap({
    case e: Relation => Some(e)
    case _ => None
  })

  /** Crush Relation Trees into groups of concrete, geometric Elements
    * with all their parent metadata disseminated.
    *
    * Return type justification:
    *   - Relations form Trees, so any given one will be pointed to by at most one other.
    *   - Nodes and Ways can be pointed to by any number of Relations, and those
    *     Relations can be part of different Trees. So the `Long` keys for
    *     the Nodes and Ways may not be unique. Such Lists should be fused into
    *     a Tree later by Spark.
    */
  private def flatForest(forest: Forest[Relation]): ParSeq[(Long, Seq[ElementData])] = {
    forest.par.flatMap(t => flatTree(Seq.empty[ElementData], t))
  }

  /* Assumptions:
   *   - Multipolygons do not point to other relations.
   *   - No geometric relations exist in relation trees (CAUTION: likely false).
   */
  private def flatTree(
    parentData: Seq[ElementData],
    tree: Tree[Relation]
  ): Seq[(Long, Seq[ElementData])] = {
    val data: Seq[ElementData] = tree.root.data +: parentData

    // TODO: This String comparison is aweful! Use a sumtype!
    val refs = tree.root.members.foldRight(Seq.empty[(Long, Seq[ElementData])])({
      case (m, acc) if Set("node", "way").contains(m.memType) => (m.ref, data) +: acc
      case (_, acc) => acc
    })

    /* Recurse across children, with this Relation's metadata passed down */
    refs ++ tree.children.flatMap(t => flatTree(data, t))
  }

  /** All Relation [[Tree]]s, broken from their original Graph structure
    * via a topological sort.
    */
  private def relForest(rs: RDD[Relation]): Forest[Relation] = {
    /* Relations which link out to other ones.
     * ASSUMPTION: This Seq will have only a few thousand elements.
     */
    val hasOutgoing: Seq[(Long, Relation, Seq[Long])] =
      rs.filter(r => r.subrelations.length > 0)
        .map(r => (r.data.meta.id, r, r.subrelations))
        .collect
        .toSeq

    /* A giantish Forest of all OSM Relations which link to other relations.
     * Note: The "leaves" are missing, which the next step finds.
     */
    val forest: Forest[Relation] = Graph.fromEdges(hasOutgoing).topologicalForest

    /* Relations which could be leaves in a Relation Tree */
    val singletons: RDD[(Long, Relation)] =
      rs.filter(r => r.subrelations.isEmpty).keyBy(r => r.data.meta.id)

    /* Find all leaves, and reconstruct the Trees */
    forest.flatMap({ t =>
      /* Leaf Relations that weren't already in this Tree */
      val leaves: Seq[Relation] = t.leaves
        .flatMap(_.subrelations)
        .flatMap(rid => singletons.lookup(rid))

      /* The real way to do this would be to use lenses to "set" the leaves.
       * Haskell: `tree & deep (filtered (null . subForest) . subForest) %~ foo`
       * where `foo` (in Scala) would do the RDD lookups.
       */
      Graph.fromEdges((t.preorder ++ leaves).map(r => (r.data.meta.id, r, r.subrelations)))
        .topologicalForest
        .map(cull)
    })
  }

  /** Sanitize a Relation Tree, such that it doesn't contain any "refs"
    * to a Relation which isn't a child of it in the Tree.
    */
  private def cull(t: Tree[Relation]): Tree[Relation] = {
    val childIds: Set[Long] = t.children.map(_.root.data.meta.id).toSet

    Tree(
      t.root.copy(members = t.root.members.filter({ m: Member =>
        m.memType != "relation" || childIds.contains(m.ref)
      })),
      t.children.map(cull)
    )
  }

  /** The average Relation-to-Relation outdegree in this RDD. */
  def averageOutdegree: Double = {
    val r: RDD[Relation] = rawRelations

    r.map(r => r.subrelations.length).fold(0)(_ + _).toDouble / r.count().toDouble
  }

  /*
   * - Form all Relation Graphs
   * - Break into a Relation Forest, then RDD[Tree[Relation]]
   * - Reduce this to RDD[(Relation, Tree[ElementData])]
   * - ???
   * - Cull any members who are relations, but aren't children in the tree
   * - For every Relation, copy its metadata to any member which isn't a relation.
   */

  // TODO: These two functions below need to be given any parent Relation metadata.

  /** All [[Node]] [[Element]]s, not doctored in any way. */
  def rawNodes: RDD[Node] = self.flatMap({
    case e: Node => Some(e)
    case _ => None
  })

  /** All [[Way]] [[Element]]s, not doctored in any way. */
  def rawWays: RDD[Way] = self.flatMap({
    case e: Way => Some(e)
    case _ => None
  })

  /** Convert an RDD of raw OSM [[Element]]s into interpreted GeoTrellis
    * [[Feature]]s. In order to mix the various subtypes together, they've
    * been upcasted internally to [[Geometry]]. Note:
    * {{{
    * type OSMFeature = Feature[Geometry, Tree[ElementData]]
    * }}}
    *
    * ===Behaviour===
    * This algorithm aims to losslessly "sanitize" its input data,
    * in that it will break down malformed Relation structures, as
    * well as cull member references to Elements which no longer
    * exist (or exist outside the subset of data you're working
    * on). Mathematically speaking, there should exist a function
    * to reverse this conversion. This theoretical function and
    * `toFeatures` form an isomorphism if the source data is
    * correct. In other words, given:
    * {{{
    * parse: XML => RDD[Element]
    * toFeatures: RDD[Element] => RDD[OSMFeature]
    * restore: RDD[OSMFeature] => RDD[Element]
    * unparse: RDD[Element] => XML
    * }}}
    * then:
    * {{{
    * unparse(restore(toFeatures(parse(xml: XML))))
    * }}}
    * will yield a body of semantically correct OSM data.
    *
    * To achieve this sanity, the algorithm has the following behaviour:
    *   - Graphs of [[Relation]]s will be broken into spanning [[Tree]]s.
    *   - It doesn't make sense to represent non-multipolygon Relations as
    *     GeoTrellis `Geometry`s, so Relation metadata is disseminated
    *     across its child members. Otherwise, Relations are "dropped"
    *     from the output.
    */
  def toFeatures: RDD[OSMFeature] = {

    /* All Geometric OSM Relations.
     * A (likely false) assumption made in the `flatTree` function is that
     * Geometric Relations never appear in Relation Graphs. Therefore we can
     * naively grab them all here.
     */
    val geomRelations: RDD[Relation] = rawRelations.filter({ r =>
      r.data.tagMap.get("type") == Some("multipolygon")
    })

    // TODO Use the results on this!
    val toDisseminate: ParSeq[(Long, Seq[ElementData])] = flatForest(relForest(rawRelations))

    val (points, rawLines, rawPolys) = geometries(rawNodes, rawWays)

    val (multiPolys, lines, polys) = multipolygons(rawLines, rawPolys, geomRelations)

    points.asInstanceOf[RDD[OSMFeature]] ++
      lines.asInstanceOf[RDD[OSMFeature]] ++
      polys.asInstanceOf[RDD[OSMFeature]] ++
      multiPolys.asInstanceOf[RDD[OSMFeature]]
  }

  /**
   * Every OSM Node and Way converted to GeoTrellis Geometries.
   *  This includes Points, Lines, and Polygons which have no holes.
   *  Holed polygons are handled by [[multipolygons]], as they are represented
   *  by OSM Relations.
   */
  private def geometries(
    nodes: RDD[Node],
    ways: RDD[Way]
  ): (RDD[OSMPoint], RDD[OSMLine], RDD[OSMPolygon]) = {
    /* You're a long way from finishing this operation. */
    val links: RDD[(Long, Way)] = ways.flatMap(w => w.nodes.map(n => (n, w)))

    /* Nodes and Ways bound by the Node ID. Independent Nodes appear here
     * as well, but with an empty Iterable of Ways.
     */
    val grouped: RDD[(Iterable[Node], Iterable[Way])] =
      nodes.map(n => (n.data.meta.id, n)).cogroup(links).map(_._2)

    val linesPolys: RDD[Either[OSMLine, OSMPolygon]] =
      grouped
        .flatMap({ case (ns, ws) =>
          /* ASSUMPTION: `ns` is always length 1 because of the `cogroup` */
          val n = ns.head

          ws.map(w => (w, n))
        })
        .groupByKey
        .map({ case (w, ns) =>
          /* De facto maximum of 2000 Nodes */
          val sorted: Vector[Node] = ns.toVector.sortBy(n => n.data.meta.id)

          /* `get` is safe, the BTree is guaranteed to be populated,
           * since `ns` is guaranteed to be non-empty.
           */
          val tree: BTree[Node] = BTree.fromSortedSeq(sorted).get

          /* A binary search branch predicate */
          val pred: (Long, BTree[Node]) => Either[Option[BTree[Node]], Node] = { (n, tree) =>
            if (n == tree.value.data.meta.id) {
              Right(tree.value)
            } else if (n < tree.value.data.meta.id) {
              Left(tree.left)
            } else {
              Left(tree.right)
            }
          }

          /* The actual node coordinates in the correct order */
          val (points, data) =
            w.nodes
              .flatMap(n => tree.searchWith(n, pred))
              .map(n => ((n.lat, n.lon), n.data))
              .unzip

          /* Segregate by which are purely Lines, and which form Polygons */
          if (w.isLine) {
            Left(Feature(Line(points), Tree(w.data, data.map(_.pure[Tree]))))
          } else {
            Right(Feature(Polygon(points), Tree(w.data, data.map(_.pure[Tree]))))
          }
        })

    // TODO: Improve this inefficient RDD splitting.
    val lines: RDD[OSMLine] = linesPolys.flatMap({
      case Left(l) => Some(l)
      case _ => None
    })

    val polys: RDD[OSMPolygon] = linesPolys.flatMap({
      case Right(p) => Some(p)
      case _ => None
    })

    /* Single Nodes unused in any Way */
    val points: RDD[OSMPoint] = grouped.flatMap({ case (ns, ws) =>
      if (ws.isEmpty) {
        val n = ns.head

        Some(Feature(Point(n.lat, n.lon), Tree.singleton(n.data)))
      } else {
        None
      }
    })

    (points, lines, polys)
  }

  private def multipolygons(
    lines: RDD[OSMLine],
    polys: RDD[OSMPolygon],
    relations: RDD[Relation]
  ): (RDD[OSMMultiPoly], RDD[OSMLine], RDD[OSMPolygon]) = {
    // filter out polys that are used in relations
    // merge RDDs back together
    val relLinks: RDD[(Long, Relation)] =
      relations.flatMap(r => r.members.map(m => (m.ref, r)))

    val lineLinks: RDD[(Long, OSMLine)] =
      lines.map(f => (f.data.root.meta.id, f))

    /* All Polygons, Lines and Relations bound by their IDs */
    val grouped =
      polys.map(f => (f.data.root.meta.id, f)).cogroup(lineLinks, relLinks).map(_._2)

    val multipolys = grouped
      /* Assumption: Polygons and Lines exist in at most one "multipolygon" Relation */
      .flatMap({
        case (ps, _, rs) if !rs.isEmpty && !ps.isEmpty => Some((rs.head, Left(ps.head)))
        case (_, ls, rs) if !rs.isEmpty && !ls.isEmpty => Some((rs.head, Right(ls.head)))
        case _ => None
      })
      .groupByKey
      .map({ case (r, gs) =>
        /* Fuse Lines into Polygons */
        val ls: Vector[OSMLine] = gs.flatMap({
          case Right(l) => Some(l)
          case _ => None
        }).toVector

        val ps: Vector[OSMPolygon] = gs.flatMap({
          case Left(p) => Some(p)
          case _ => None
        }).toVector ++ fuseLines(spatialSort(ls.map(f => (f.geom.centroid.as[Point].get, f))).map(_._2))

        val outerIds: Set[Long] = r.members.partition(_.role == "outer")._1.map(_.ref).toSet

        val (outers, inners) = ps.partition(f => outerIds.contains(f.data.root.meta.id))

        /* Match outer and inner Polygons - O(n^2) */
        val fused = outers.map({ o =>
          Polygon(
            o.geom.exterior,
            inners.filter(i => o.geom.contains(i.geom)).map(_.geom.exterior)
          )
        })

        /* It is suggested by OSM that multipoly tag data should be stored in
         *  the Relation, not its constituent parts. Hence we take `r.data`
         *  as the root `ElementData` here.
         *
         *  However, "inner" Ways can have meaningful tags, such as a lake in
         *  the middle of a forest.
         *
         *  Furthermore, winding order doesn't matter in OSM, but it does
         *  in VectorTiles.
         *  TODO: Make sure winding order is handled correctly.
         */
        Feature(MultiPolygon(fused), Tree(r.data, outers.map(_.data) ++ inners.map(_.data)))
      })

    /* Lines which were part of no Relation */
    val openLines = grouped.flatMap({
      case (ps, ls, rs) if ps.isEmpty && rs.isEmpty => Some(ls.head)
      case _ => None
    })

    /* Polygons which were part of no Relation */
    val plainPolys = grouped.flatMap({
      case (ps, ls, rs) if ls.isEmpty && rs.isEmpty => Some(ps.head)
      case _ => None
    })

    (multipolys, openLines, plainPolys)
  }

  /**
   * Order a given Vector of Features such that each Geometry is as
   *  spatially close as possible to its neighbours in the result Vector.
   *  This is done by comparing the location of a centroid Point given for
   *  each Geometry.
   *
   *  Time complexity: O(nlogn)
   */
  private def spatialSort[T](v: Vector[(Point, T)]): Vector[(Point, T)] = v match {
    case Vector() => v
    case v if v.length < 6 => v.sortBy(_._1.x) // TODO Naive?
    case v => {
      /* Kernels - Two points around which to group all others */
      val mid: Point = v(v.length / 2)._1
      val avg: (Point, Point) => Point = (p1, p2) => Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)
      val (kl, kr) = (avg(v.head._1, mid), avg(v.last._1, mid))

      /* Group all points spatially around the two kernels */
      val (l, r) = v.partition({ f => f._1.distance(kl) < f._1.distance(kr) })

      /* Recombine the sorted lines by whichever endpoints are closest */
      (spatialSort(l), spatialSort(r)) match {
        case (Vector(), v2) => v2
        case (v1, Vector()) => v1
        case (v1, v2) => {
          val pairs = Seq(
            0 -> v1.last._1.distance(v2.head._1),
            1 -> v1.last._1.distance(v2.last._1),
            2 -> v1.head._1.distance(v2.head._1),
            3 -> v1.head._1.distance(v2.last._1)
          )

          pairs.sortBy(_._2).head._1 match {
            case 0 => v1 ++ v2
            case 1 => v1 ++ v2.reverse
            case 2 => v1.reverse ++ v2
            case 3 => v2 ++ v1
          }
        }
      }
    }
  }

  /**
   * ASSUMPTIONS:
   *    - Every Line in the given Vector can be fused
   *    - The final result of all fusions will be a set of Polygons
   *
   *  Time complexity (raw): O(n^2)
   *
   *  Time complexity (sorted): O(n)
   */
  private def fuseLines(
    v: Vector[Feature[Line, Tree[ElementData]]]
  ): Vector[Feature[Polygon, Tree[ElementData]]] = v match {
    case Vector() => Vector.empty
    case v if v.length == 1 => throw new IllegalArgumentException("Single unfusable Line remaining.")
    case v => {
      val (f, d, rest) = fuseOne(v)

      if (f.isClosed)
        Feature(Polygon(f), Tree(d.head.root, d)) +: fuseLines(rest)
      else
        fuseLines(Feature(f, Tree(d.head.root, d)) +: rest)
    }
  }

  /**
   * Fuse the head Line in the Vector with the first other Line possible.
   *  This borrows [[fuseLines]]'s assumptions.
   */
  private def fuseOne(
    v: Vector[OSMLine]
  ): (Line, Seq[Tree[ElementData]], Vector[OSMLine]) = {
    val h = v.head
    val t = v.tail

    // TODO: Use a `while` instead?
    for ((f, i) <- t.zipWithIndex) {
      if (h.geom.touches(f.geom)) { /* Found two lines that should fuse */
        val lm = new LineMerger /* from JTS */

        lm.add(h.geom.jtsGeom)
        lm.add(f.geom.jtsGeom)

        val line: Line = Line(lm.getMergedLineStrings.toArray(Array.empty[LineString]).head)

        val (a, b) = t.splitAt(i)

        /* Return early */
        return (line, Seq(h.data, f.data), a ++ b.tail)
      }
    }

    /* As every Line _must_ fuse, this should never be reached */
    ???
  }
}
