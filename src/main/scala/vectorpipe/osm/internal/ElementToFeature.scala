package vectorpipe.osm.internal

import scala.collection.parallel.ParSeq

import cats.data.State
import cats.implicits._
import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.operation.linemerge.LineMerger
import geotrellis.util._
import geotrellis.vector._
import org.apache.spark.rdd._
import spire.std.any._
import vectorpipe.osm._
import vectorpipe.util._

// --- //

/* TODOS
 *
 * Hell, but what about relations that have mixed member types? (ways + other relations)
 * See Relation #47796. It has ways, single nodes, and a relation labelled a "subarea".
 *
 */

private[vectorpipe] object ElementToFeature {

  // /** Crush Relation Trees into groups of concrete, geometric Elements
  //   * with all their parent metadata disseminated.
  //   *
  //   * Return type justification:
  //   *   - Relations form Trees, so any given one will be pointed to by at most one other.
  //   *   - Nodes and Ways can be pointed to by any number of Relations, and those
  //   *     Relations can be part of different Trees. So the `Long` keys for
  //   *     the Nodes and Ways may not be unique. Such Lists should be fused into
  //   *     a Tree later by Spark.
  //   */
  // private def flatForest(forest: Forest[Relation]): ParSeq[(Long, Seq[ElementData])] = {
  //   forest.par.flatMap(t => flatTree(Seq.empty[ElementData], t))
  // }

  // /** Assumptions:
  //  *   - Multipolygons do not point to other relations.
  //  *   - No geometric relations exist in relation trees (CAUTION: likely false).
  //  */
  // private def flatTree(
  //   parentData: Seq[ElementData],
  //   tree: Tree[Relation]
  // ): Seq[(Long, Seq[ElementData])] = {
  //   val data: Seq[ElementData] = tree.root.data +: parentData

  //   // TODO: This String comparison is aweful! Use a sumtype!
  //   val refs = tree.root.members.foldRight(Seq.empty[(Long, Seq[ElementData])])({
  //     case (m, acc) if Set("node", "way").contains(m.`type`) => (m.ref, data) +: acc
  //     case (_, acc) => acc
  //   })

  //   /* Recurse across children, with this Relation's metadata passed down */
  //   refs ++ tree.children.flatMap(t => flatTree(data, t))
  // }

  // /** All Relation [[Tree]]s, broken from their original Graph structure
  //   * via a topological sort.
  //   */
  // private def relForest(rs: RDD[Relation]): Forest[Relation] = {
  //   /* Relations which link out to other ones.
  //    * ASSUMPTION: This Seq will have only a few thousand elements.
  //    */
  //   val hasOutgoing: Seq[(Long, Relation, Seq[Long])] =
  //     rs.filter(r => r.subrelations.length > 0)
  //       .map(r => (r.data.meta.id, r, r.subrelations))
  //       .collect
  //       .toSeq

  //   /* A giantish Forest of all OSM Relations which link to other relations.
  //    * Note: The "leaves" are missing, which the next step finds.
  //    */
  //   val forest: Forest[Relation] = Graph.fromEdges(hasOutgoing).topologicalForest

  //   /* Relations which could be leaves in a Relation Tree */
  //   val singletons: RDD[(Long, Relation)] =
  //     rs.filter(r => r.subrelations.isEmpty).keyBy(r => r.data.meta.id)

  //   /* Find all leaves, and reconstruct the Trees */
  //   forest.flatMap({ t =>
  //     /* Leaf Relations that weren't already in this Tree */
  //     val leaves: Seq[Relation] = t.leaves
  //       .flatMap(_.subrelations)
  //       .flatMap(rid => singletons.lookup(rid))

  //     /* The real way to do this would be to use lenses to "set" the leaves.
  //      * Haskell: `tree & deep (filtered (null . subForest) . subForest) %~ foo`
  //      * where `foo` (in Scala) would do the RDD lookups.
  //      */
  //     Graph.fromEdges((t.preorder ++ leaves).map(r => (r.data.meta.id, r, r.subrelations)))
  //       .topologicalForest
  //       .map(cull)
  //   })
  // }

  // /** Sanitize a Relation Tree, such that it doesn't contain any "refs"
  //   * to a Relation which isn't a child of it in the Tree.
  //   */
  // private def cull(t: Tree[Relation]): Tree[Relation] = {
  //   val childIds: Set[Long] = t.children.map(_.root.data.meta.id).toSet

  //   Tree(
  //     t.root.copy(members = t.root.members.filter({ m: Member =>
  //       m.`type` != "relation" || childIds.contains(m.ref)
  //     })),
  //     t.children.map(cull)
  //   )
  // }

  /*
   * - Form all Relation Graphs
   * - Break into a Relation Forest, then RDD[Tree[Relation]]
   * - Reduce this to RDD[(Relation, Tree[ElementData])]
   * - ???
   * - Cull any members who are relations, but aren't children in the tree
   * - For every Relation, copy its metadata to any member which isn't a relation.
   */

  /**
   * Every OSM Node and Way converted to GeoTrellis Geometries.
   *  This includes Points, Lines, and Polygons which have no holes.
   *  Holed polygons are handled by [[multipolygons]], as they are represented
   *  by OSM Relations.
   *
   *  @note: The performance of this method is serviced by having a partitioner set
   *         on the the nodes and ways.
   */
  def geometries(
    nodes: RDD[(Long, Node)],
    ways: RDD[(Long, Way)]
  ): (RDD[OSMPoint], RDD[OSMLine], RDD[OSMPolygon]) = {
    /* You're a long way from finishing this operation. */
    // TODO: Don't map whole way, or else you shuffle whole way.
    // Only map to way Id.
    // Ways -> (WayId, Way) (partitioned)
    // Ways -> (NodeId, WayId), Join to (NodeId, Node), map to (Node, It[WayId]), flatmap (WayId, NodeId),/
    // Group by key with same partitioner, join to ways.
    val nodesToWayIds: RDD[(Node, Iterable[Long])] =
      nodes
        .cogroup(
          ways
            .flatMap { case (wayId, way) =>
              way.nodes.map { nodeId => (nodeId, wayId) }
            }
        )
        .flatMap { case (_, (nodes, wayIds)) =>
          if(nodes.isEmpty) { None }
          else {
            /* ASSUMPTION: `nodes` contains distinct elements */
            val node = nodes.head
            Some((node, wayIds))
          }
        }


    val wayIdsToNodes: RDD[(Long, Iterable[Node])] =
      nodesToWayIds
        .flatMap { case (node, wayIds) =>
          wayIds.map(wayId => (wayId, node))
        }
        .groupByKey

    val linesPolys: RDD[Either[OSMLine, OSMPolygon]] =
      ways
        .join(wayIdsToNodes)
        .flatMap { case (_, (way, nodes)) =>

          /* De facto maximum of 2000 Nodes */
          val sorted: Vector[Node] = nodes.toVector.sortBy(n => n.data.meta.id)

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
          val (points, data): (Vector[(Double, Double)], Vector[ElementData]) =
            way
              .nodes
              .flatMap(n => tree.searchWith(n, pred))
              .map(n => ((n.lon, n.lat), n.data))
              .unzip

          // TODO: 2017 May  9 @ 08:31 - Why are we `try`ing here?
          // 2017 Aug 14 - Maybe because Line/Poly construction can fail if `points` is empty?
          // Confirm why it would ever be empty.
          try {
            /* Segregate by which are purely Lines, and which form Polygons */
            if (way.isLine) {
              // TODO: Report somehow that the Line was dropped for being degenerate.
              if (isDegenerateLine(points)) None else {
                Some(Left(Feature(Line(points), Tree(way.data, data.map(_.pure[Tree])))))
              }
            } else {
              Some(Right(Feature(Polygon(points), Tree(way.data, data.map(_.pure[Tree])))))
            }
          } catch {
            case e: Throwable => None // TODO Be more elegant about this?
          }

        }

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
    val points: RDD[OSMPoint] = nodesToWayIds.flatMap({ case (n, ws) =>
      if (ws.isEmpty) {
        Some(Feature(Point(n.lon, n.lat), Tree.singleton(n.data)))
      } else {
        None
      }
    })

    (points, lines, polys)
  }

  // TODO - optimize. Also I suspect this is the cause of the hang in the Finland step.
  def multipolygons(
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
      .flatMap({ case (r, gs) =>
        /* Fuse Lines into Polygons */
        val ls: Vector[OSMLine] = gs.flatMap({
          case Right(l) => Some(l)
          case _ => None
        }).toVector

        // TODO: This code is suspect
        //      I don't know what this is doing, commenting out for now.
        //      I suspect this is the code that makes the 8th stage crazy slow.

        // /* All line segments, roughly in order of connecting to others. */
        // val sorted = spatialSort(ls.map(f => (f.geom.centroid.as[Point].get, f))).map(_._2)

        // /* All line segments which could fuse into Polygons */
        // val (dumpedLineCount, fusedLines) = fuseLines(sorted).run(0).value

        // if (dumpedLineCount > 0) println(s"LINES DUMPED BY fuseLines: ${dumpedLineCount}")

        val ps: Vector[OSMPolygon] = gs.flatMap({
          case Left(p) => Some(p)
          case _ => None
        }).toVector //++ fusedLines

        val outerIds: Set[Long] = r.members.partition(_.role == "outer")._1.map(_.ref).toSet

        val (outers, inners) = ps.partition(f => outerIds.contains(f.data.root.meta.id))

        /* Match outer and inner Polygons - O(n^2) */
        val fused = outers.map({ o =>
          Polygon(
            o.geom.exterior,
            inners.filter(i => o.geom.contains(i.geom)).map(_.geom.exterior)
          )
        })

        if (fused.isEmpty) None else {
          /* It is suggested by OSM that multipoly tag data should be stored in
           * the Relation, not its constituent parts. Hence we take `r.data`
           * as the root `ElementData` here.
           *
           * However, "inner" Ways can have meaningful tags, such as a lake in
           * the middle of a forest.
           *
           * Furthermore, winding order doesn't matter in OSM, but it does
           * in VectorTiles.
           */
          Some(Feature(MultiPolygon(fused), Tree(r.data, outers.map(_.data) ++ inners.map(_.data))))
        }
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

  /* Code smell to massage the Applicative use below */
  private type IntState[T] = State[Int, T]

  /**
   * ASSUMPTIONS:
   *    - In the full dataset, every Line in the given Vector can be fused
   *    - In a partial dataset, not all Lines are guaranteed to fuse. UNFUSED LINES ARE DUMPED.
   *    - The final result of all fusions will be a set of Polygons
   *
   *  Time complexity (raw): O(n^2)
   *
   *  Time complexity (sorted): O(n)
   *
   * Note: The [[State]] monad usage here is for counting Lines which couldn't
   * be fused.
   */
  private def fuseLines(
    v: Vector[Feature[Line, Tree[ElementData]]]
  ): State[Int, Vector[Feature[Polygon, Tree[ElementData]]]] =
    v match {
      case Vector() => Vector.empty.pure[IntState]
      case v if v.length == 1 => State.modify[Int](_ + 1).map(_ => Vector.empty)
      case v => {
        fuseOne(v).flatMap({
          case None => fuseLines(v.tail)
          case Some((f, d, i, rest)) => {
            if (f.isClosed)
              fuseLines(rest).map(c => Feature(Polygon(f), Tree(d.head.root, d)) +: c)
            else {
              /* The next sections of the currently accumulating Line may be far down
               * the Vector, so we punt it forward to avoid wasted traversals
               * and intersection checks.
               * This is a band-aid around the fact that `spatialSort` doesn't
               * place all sections of every fusable line in order; clumps of them
               * can appear far apart in the Vector.
               *
               * If `i == 0`, the splitAt and `(++)` below are basically no-ops,
               * so there's no point in any `if (i == 0) skipTheSplitAt`.
               */
              val (a, b) = rest.splitAt(i)

              fuseLines(a ++ (Feature(f, Tree(d.head.root, d)) +: b))
            }
          }
        })
      }
    }

  /**
   * Fuse the head Line in the Vector with the first other Line possible.
   * This borrows [[fuseLines]]'s assumptions.
   */
  private def fuseOne(
    v: Vector[OSMLine]
  ): State[Int, Option[(Line, Seq[Tree[ElementData]], Int, Vector[OSMLine])]] = {
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

        /* Hand-hold the typechecker */
        val res: Option[(Line, Seq[Tree[ElementData]], Int, Vector[OSMLine])] =
          Some((line, Seq(h.data, f.data), i, a ++ b.tail))

        /* Return early */
        return res.pure[IntState]
      }
    }

    State.modify[Int](_ + 1).map(_ => None)
  }

  /** Is a given line illegal? */
  private def isDegenerateLine(points: Vector[(Double, Double)]): Boolean = {
    /* Is the Line just two of the same Point?
     * This would otherwise cause problems for JTS later.
     */
    points.length == 2 && points(0) == points(1)
  }
}
