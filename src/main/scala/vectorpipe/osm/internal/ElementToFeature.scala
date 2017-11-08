package vectorpipe.osm.internal

import scala.collection.parallel.ParSeq

import cats.data.State
import cats.implicits._
import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.operation.linemerge.LineMerger
import geotrellis.proj4._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.rdd._
import spire.std.any._
import vectorpipe.osm._
import vectorpipe.util._

// --- //

private[vectorpipe] object ElementToFeature {

  /**
   * Every OSM Node and Way converted to GeoTrellis Geometries.
   *  This includes Points, Lines, and Polygons which have no holes.
   *  Holed polygons are handled by [[multipolygons]], as they are represented
   *  by OSM Relations.
   */
  def geometries(
    nodes: RDD[Node],
    ways: RDD[Way]
  ): (RDD[OSMPoint], RDD[OSMLine], RDD[OSMPolygon]) = {
    /* You're a long way from finishing this operation. */
    val links: RDD[(Long, Way)] = ways.flatMap(w => w.nodes.map(n => (n, w)))

    /* Nodes and Ways bound by the Node ID. Independent Nodes appear here
     * as well, but with an empty Iterable of Ways.
     */
    val grouped: RDD[(Iterable[Node], Iterable[Way])] =
      nodes.map(n => (n.meta.id, n)).cogroup(links).map(_._2)

    val linesPolys: RDD[Either[OSMLine, OSMPolygon]] =
      grouped
        .flatMap({ case (ns, ws) =>
          /* ASSUMPTION: `ns` is always length 1 because of the `cogroup` */
          val n = ns.head

          ws.map(w => (w, n))
        })
        .groupByKey
        .flatMap({ case (w, ns) =>
          /* De facto maximum of 2000 Nodes */
          val sorted: Vector[Node] = ns.toVector.sortBy(n => n.meta.id)

          /* `get` is safe, the BTree is guaranteed to be populated,
           * since `ns` is guaranteed to be non-empty.
           */
          val tree: BTree[Node] = BTree.fromSortedSeq(sorted).get

          /* A binary search branch predicate */
          val pred: (Long, BTree[Node]) => Either[Option[BTree[Node]], Node] = { (n, tree) =>
            if (n == tree.value.meta.id) {
              Right(tree.value)
            } else if (n < tree.value.meta.id) {
              Left(tree.left)
            } else {
              Left(tree.right)
            }
          }

          /* The actual node coordinates in the correct order */
          val (points, data): (Vector[(Double, Double)], Vector[ElementMeta]) =
            w.nodes
              .flatMap(n => tree.searchWith(n, pred))
              .map(n => ((n.lon, n.lat), n.meta))
              .unzip

          // TODO: 2017 May  9 @ 08:31 - Why are we `try`ing here?
          // 2017 Aug 14 - Maybe because Line/Poly construction can fail if `points` is empty?
          // Confirm why it would ever be empty.
          try {
            /* Segregate by which are purely Lines, and which form Polygons */
            if (w.isLine) {
              // TODO: Report somehow that the Line was dropped for being degenerate.
              if (isDegenerateLine(points)) None else {
                Some(Left(Feature(Line(points), w.meta)))
              }
            } else {
              Some(Right(Feature(Polygon(points), w.meta)))
            }
          } catch {
            case e: Throwable => None // TODO Be more elegant about this?
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

        Some(Feature(Point(n.lon, n.lat), n.meta))
      } else {
        None
      }
    })

    (points, lines, polys)
  }

  /** A way to render an OSM Line that failed to be reconstructed into a Polygon. */
  private def errorFusing(f: OSMLine): String =
    s"LINE FUSION FAILURE\nELEMENT METADATA: ${f.data}\nGEOM: ${f.geom.reproject(WebMercator, LatLng).toGeoJson}"

  def multipolygons(
    logError: (OSMLine => String) => OSMLine => Unit,
    lines: RDD[OSMLine],
    polys: RDD[OSMPolygon],
    relations: RDD[Relation]
  ): (RDD[OSMMultiPoly], RDD[OSMLine], RDD[OSMPolygon]) = {
    // filter out polys that are used in relations
    // merge RDDs back together
    val relLinks: RDD[(Long, Relation)] =
      relations.flatMap(r => r.members.map(m => (m.ref, r)))

    val lineLinks: RDD[(Long, OSMLine)] =
      lines.map(f => (f.data.id, f))

    /* All Polygons, Lines and Relations bound by their IDs */
    val grouped =
      polys.map(f => (f.data.id, f)).cogroup(lineLinks, relLinks).map(_._2)

    val multipolys = grouped
      /* Assumption: Polygons and Lines exist in at most one "multipolygon" Relation */
      .flatMap({
        case (ps, _, rs) if !rs.isEmpty && !ps.isEmpty => Some((rs.head, Left(ps.head)))
        case (_, ls, rs) if !rs.isEmpty && !ls.isEmpty => Some((rs.head, Right(ls.head)))
        case _ => None
      })
      .groupByKey
      .flatMap { case (r, gs) =>
        /* Fuse Lines into Polygons */
        val ls: Vector[OSMLine] = gs.flatMap({
          case Right(l) => Some(l)
          case _ => None
        }).toVector

        /* All line segments, rougly in order of connecting to others. */
        val sorted: Vector[Feature[Line, ElementMeta]] =
          spatialSort(ls.map(f => (f.geom.centroid.as[Point].get, f))).map(_._2)

        /* All line segments which could fuse into Polygons */
        val (unfusedLines, fusedLines) = fuseLines(sorted).run(List.empty).value

        /* Log each `Line` which failed to fuse */
        unfusedLines.foreach(logError(errorFusing))

        val ps: Vector[OSMPolygon] = gs.flatMap({
          case Left(p) => Some(p)
          case _ => None
        }).toVector ++ fusedLines

        val outerIds: Set[Long] = r.members.partition(_.role == "outer")._1.map(_.ref).toSet

        /* While `fuseLines` above does dump most metadata while it's fusing, it at least
         * keeps the metadata of one of the Lines that made up the final Polygon.
         * It's the original ID of the Line that is stored in its parent Relation
         * as a "member", so checking here for the presence of the Line's ID is all
         * that's necessary to confirm the whole Polygon.
         *
         * Geometries which were already Polygons (and thus didn't need extra fusing)
         * will naturally do the right thing here.
         */
        val (outers, inners) = ps.partition(f => outerIds.contains(f.data.id))

        /* Match outer and inner Polygons - O(n^2) */
        val fused: Vector[Polygon] = outers.map({ o =>
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
          Some(Feature(MultiPolygon(fused), r.meta))
        }
      }

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
  private type LineState[T] = State[List[OSMLine], T]

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
  private def fuseLines(v: Vector[OSMLine]): State[List[OSMLine], Vector[OSMPolygon]] = v match {
    case Vector()  => Vector.empty.pure[LineState]
    case Vector(h) => State.modify[List[OSMLine]](h :: _).map(_ => Vector.empty)
    case v => fuseOne(v) match {
      case None => State.modify[List[OSMLine]](v.head :: _) *> fuseLines(v.tail)
      case Some((f, d, i, rest)) => {
        if (f.isClosed)
          fuseLines(rest).map(c => Feature(Polygon(f), d) +: c)
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

          fuseLines(a ++ (Feature(f, d) +: b))
        }
      }
    }
  }

  /**
   * Fuse the head Line in the Vector with the first other Line possible.
   * This borrows [[fuseLines]]'s assumptions.
   */
  private def fuseOne(v: Vector[OSMLine]): Option[(Line, ElementMeta, Int, Vector[OSMLine])] = {
    val h = v.head
    val t = v.tail

    // TODO: Use a `while` instead? Scala `for` is slow.
    for ((f, i) <- t.zipWithIndex) {
      if (h.geom.touches(f.geom)) { /* Found two lines that should fuse */
        val lm = new LineMerger /* from JTS */

        lm.add(h.geom.jtsGeom)
        lm.add(f.geom.jtsGeom)

        val line: Line = Line(lm.getMergedLineStrings.toArray(Array.empty[LineString]).head)

        val (a, b) = t.splitAt(i)

        /* Return early */
        return Some((line, h.data, i, a ++ b.tail))
      }
    }

    /* The line `h` didn't fuse! */
    None
  }

  /** Is a given line illegal? */
  private def isDegenerateLine(points: Vector[(Double, Double)]): Boolean = {
    /* Is the Line just two of the same Point?
     * This would otherwise cause problems for JTS later.
     */
    points.length == 2 && points(0) == points(1)
  }
}
