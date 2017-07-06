package vectorpipe

import scala.collection.mutable.{ListBuffer, Map => MMap}

import geotrellis.vector._
import geotrellis.vectortile._
import vectorpipe.osm._
import vectorpipe.util._

// --- //

/** "Collator" or "Schema" functions which form
  * `VectorTile`s from collections of GeoTrellis
  * `Feature`s. Any function can be considered a valid "collator" if it
  * satisfies the type:
  * {{{
  * collate: (Extent, Iterable[Feature[G,D]]) => VectorTile
  * }}}
  *
  * ==Usage==
  * Create a VectorTile from some collection of GeoTrellis Geometries:
  * {{{
  * val tileExtent: Extent = ... // Extent of _this_ Tile
  * val geoms: Iterable[Feature[Geometry, Map[String, String]]] = ...  // Some collection of Geometries
  *
  * val tile: VectorTile = Collate.withStringMetadata(tileExtent, geoms)
  * }}}
  * Create a VectorTile via some custom collation scheme:
  * {{{
  * def partition(f: Feature[G,D]): String = ...
  * def metadata(d: D): Map[String, Value] = ...
  *
  * val tileExtent: Extent = ... // Extent of _this_ Tile
  * val geoms: Iterable[Feature[G, D]] = ...  // Some collection of Geometries
  *
  * val tile: VectorTile = Collate.generically(tileExtent, geoms, partition, metadata)
  * }}}
  *
  * ==Writing your own Collator Function==
  * We provide a few defaults here, but any collation scheme is possible.
  * Collation just refers to the process of organizing some `Iterable`
  * collection of Geometries into various VectorTile `Layer`s. Creating your own
  * collator is done easiest with [[generically]]. It expects a ''partition''
  * function to guide Geometries into separate Layers, and a ''metadata''
  * transformation function.
  *
  * ====Partition Functions====
  * A valid partition function must be of the type:
  * {{{
  * partition: Feature[G,D] => String
  * }}}
  * The output String is the name of the `Layer` you'd like a given
  * `Feature` to be relegated to. Notice that the entire `Feature` is available
  * (i.e. both its Geometry and metadata), so that your partitioner can make
  * fine-grained choices.
  *
  * ====Metadata Transformation Functions====
  * One of these takes your `D` type and transforms it into what `VectorTile`s expect:
  * {{{
  * metadata: D => Map[String, Value]
  * }}}
  * You're encouraged to review the `Value` sum-type in
  * [[https://geotrellis.github.io/scaladocs/latest/#geotrellis.vectortile.package
  * geotrellis.vectortile]]
  *
  * ==On Winding Order==
  * VectorTiles require that Polygon exteriors have clockwise winding order,
  * and that interior holes have counter-clockwise winding order. These assume
  * that the origin `(0,0)` is in the '''top-left''' corner.
  *
  * '''Any custom collator which does not call `generically` must correct
  * for Polygon winding order manually.''' This can be done via the [[winding]]
  * function.
  *
  * But why correct for winding order at all? Well, OSM data makes no guarantee
  * about what winding order its derived Polygons will have. We could correct
  * winding order when our first `RDD[OSMFeature]` is created, except that its
  * unlikely that the clipping process afterward would maintain our winding for
  * all Polygons.
  */
object Collate {
  /** Can be used as the `partition` argument to `generically`. Splits
    * Geometries by their subtype, naming three `Layer`s: `points`, `lines` and
    * `polygons`.
    */
  def byGeomType[G <: Geometry, D](f: Feature[G, D]): String = f.geom match {
    case g: Point        => "points"
    case g: MultiPoint   => "points"
    case g: Line         => "lines"
    case g: MultiLine    => "lines"
    case g: Polygon      => "polygons"
    case g: MultiPolygon => "polygons"
  }

  /** Partition all Features into a single Layer. */
  def intoOneLayer[G <: Geometry, D](f: Feature[G, D]): String = "features"

  /** Collate some collection of Features into a [[VectorTile]] while
    * dropping any metadata they might have had. The resulting Tile has three
    * [[Layer]]s, labelled `points`, `lines`, and `polygons`.
    *
    * @param tileExtent The CRS Extent of the Tile to be created.
    */
  def withoutMetadata[G <: Geometry, D](tileExtent: Extent, geoms: Iterable[Feature[G, D]]): VectorTile =
    generically(tileExtent, geoms, byGeomType, { _: D => Map.empty })

  /** Collate Features into "Analytic VectorTiles". These aim to hold their
    * data in such a way that it is as close to an isomorphism with the original
    * OSM data as possible. Their Geometries are organised into three Layers:
    * "points", "lines", and "polygons".
    */
  def byAnalytics(tileExtent: Extent, geoms: Iterable[OSMFeature]): VectorTile =
    generically(tileExtent, geoms, byGeomType, flatParents)

  private[vectorpipe] def flatParents(d: Tree[ElementData]): Map[String, Value] = {

    def work(parent: Long, node: Tree[ElementData]): Seq[(Extra, ElementData)] = node.root.extra match {
      /* It's a Way that got eaten */
      case None =>
        (WayExtra(Seq(parent)), node.root) +: node.children.flatMap(t => work(node.root.meta.id, t))

      /* It's a Node that got eaten */
      case Some(Right(p)) => Seq((NodeExtra(p, Seq(parent)), node.root))

      /* Should never occur */
      case _ => sys.error("BOOM!"); Seq.empty
    }

    d.root match {
      /* It was a top-level Node, not part of any Way */
      case ElementData(_, _, Some(Left(extent))) if d.children.isEmpty =>
        compressMeta(0, GeomExtra(extent), d.root)

      /* It was either a Way that became a Line/Poly, or a Relation that became a MultiPoly */
      case ElementData(_, _, Some(Left(extent))) => {

        /* Combine data for Nodes which were shared by multiple Ways */
        val kids = uniqueNodes(d.children.flatMap(t => work(d.root.meta.id, t)))

        ((GeomExtra(extent), d.root) +: kids)
          .zipWithIndex
          .map({ case ((extra, meta), i) => compressMeta(i.toShort, extra, meta) })
          .reduce(_ ++ _)
      }

      /* Should never happen, since all top-level Features should have had
       * their `extra` field sent to hold their bounding envelope in a
       * `Some(Left(...))`
       */
      case _ => Map.empty
    }
  }

  /** Combine Node metadata if those from different Ways are detected to be
    * from the same original Node. Non-node metadata is returned unaltered.
    */
  private def uniqueNodes(items: Seq[(Extra, ElementData)]): Seq[(Extra, ElementData)] = {

    val (nodes, others) = items.partition({
      case (NodeExtra(_, _), _) => true
      case _ => false
    })

    val uniques: Seq[(Extra, ElementData)] = {
      val um: MMap[Long, (Extra, ElementData)] = MMap.empty

      nodes.foreach({
        /* Handle the collision */
        case n if um.contains(n._2.meta.id) => {
          val m: NodeExtra = um(n._2.meta.id)._1.asInstanceOf[NodeExtra]

          val foo = (NodeExtra(m.point, n._1.asInstanceOf[NodeExtra].ways ++ m.ways), n._2)

          um.update(n._2.meta.id, foo)
        }

        /* No key collision */
        case n => um.update(n._2.meta.id, n)
      })

      um.values.toSeq
    }

    uniques ++ others
  }

  private def compressMeta(id: Short, extra: Extra, data: ElementData): Map[String, Value] = {

    /* In Scala, Chars are unsigned 16-bit values */
    val prefix: Char = id.toChar

    val usuals = Map(
      Seq(prefix, 0x00.toChar).mkString -> VInt64(data.meta.id),
      Seq(prefix, 0x01.toChar).mkString -> VString(data.meta.user),
      Seq(prefix, 0x02.toChar).mkString -> VString(data.meta.userId),
      Seq(prefix, 0x03.toChar).mkString -> VInt64(data.meta.changeSet.toLong),
      Seq(prefix, 0x04.toChar).mkString -> VInt64(data.meta.version.toLong),
      Seq(prefix, 0x05.toChar).mkString -> VString(data.meta.timestamp.toString),
      Seq(prefix, 0x06.toChar).mkString -> VBool(data.meta.visible)
    )

    val tags: Map[String, Value] = data.tagMap
      .toStream
      .map({ case (k,v) => k ++ ":" ++ v })
      .zipWithIndex
      .map({ case (v,i) => Seq(prefix, (i + 0x70).toChar).mkString -> VString(v) })
      .toMap

    val extras: Map[String, Value] = extra match {
      case NodeExtra(p, ws) => {

        val latLng = Map(
          Seq(prefix, 0x07.toChar).mkString -> VDouble(p.y),
          Seq(prefix, 0x08.toChar).mkString -> VDouble(p.x)
        )

        val ways = ws.zipWithIndex
          .map({ case (v,i) => Seq(prefix, (i + 0x20).toChar).mkString -> VInt64(v) })
          .toMap

        latLng ++ ways
      }

      case WayExtra(rs) =>
        rs.zipWithIndex
          .map({ case (v,i) => Seq(prefix, (i + 0x40).toChar).mkString -> VInt64(v) })
          .toMap

      case GeomExtra(env) => Map(
        Seq(prefix, 0x09.toChar).mkString -> VDouble(env.xmin),
        Seq(prefix, 0x0a.toChar).mkString -> VDouble(env.ymin),
        Seq(prefix, 0x0b.toChar).mkString -> VDouble(env.xmax),
        Seq(prefix, 0x0c.toChar).mkString -> VDouble(env.ymax)
      )
    }

    usuals ++ tags ++ extras
  }

  /** Similar behaviour to [[byAnalytics]], except that recursive metadata
    * is not stored.
    */
  def byAnalyticsLite(tileExtent: Extent, geoms: Iterable[OSMFeature]): VectorTile = {
    def metadata(d: Tree[ElementData]): Map[String, Value] = {

      Map(
        "id"            -> VInt64(d.root.meta.id),
        "user"          -> VString(d.root.meta.user),
        "userId"        -> VString(d.root.meta.userId),
        "changeSet"     -> VInt64(d.root.meta.changeSet.toLong),
        "version"       -> VInt64(d.root.meta.version.toLong),
        "timestamp"     -> VString(d.root.meta.timestamp.toString),
        "visible"       -> VBool(d.root.meta.visible)
      )
    }

    generically(tileExtent, geoms, byGeomType, metadata)
  }

  def withStringMetadata[G <: Geometry](tileExtent: Extent, geoms: Iterable[Feature[G, Map[String, String]]]): VectorTile =
    generically(tileExtent, geoms, byGeomType, { d: Map[String,String] => d.mapValues(VString) })

  /** Given some Feature and a way to determine which [[Layer]] it should
    * belong to (by layer name), collate each Feature into the appropriate
    * [[Layer]] and form a [[VectorTile]]. '''Polygon winding order is
    * corrected.'''
    *
    * @param tileExtent The Extent of ''this'' Tile.
    * @param geoms The Features to collate into various [[Layer]]s.
    * @param partition The means by which to place a certain Feature into a
    *                  certain [[Layer]]. The `String` is returns is the name
    *                  of the Layer the collate the Feature into.
    * @param metadata The means by which to transform some Feature's metadata
    *                 into the type that [[VectorTile]]s expect.
    */
  def generically[G <: Geometry, D](
    tileExtent: Extent,
    geoms: Iterable[Feature[G, D]],
    partition: Feature[G, D] => String,
    metadata: D => Map[String, Value]
  ): VectorTile = {
    def work(name: String, lb: ListBuffer[Feature[G, D]]): StrictLayer = {
      val points  = new ListBuffer[Feature[Point, Map[String, Value]]]
      val mpoints = new ListBuffer[Feature[MultiPoint, Map[String, Value]]]
      val lines   = new ListBuffer[Feature[Line, Map[String, Value]]]
      val mlines  = new ListBuffer[Feature[MultiLine, Map[String, Value]]]
      val polys   = new ListBuffer[Feature[Polygon, Map[String, Value]]]
      val mpolys  = new ListBuffer[Feature[MultiPolygon, Map[String, Value]]]

      /* Partition the Geometries by subtype. Polygon winding order is corrected. */
      lb.foreach({ f => f.geom match {
        case g: Point        => points.append(Feature(g, metadata(f.data)))
        case g: MultiPoint   => mpoints.append(Feature(g, metadata(f.data)))
        case g: Line         => lines.append(Feature(g, metadata(f.data)))
        case g: MultiLine    => mlines.append(Feature(g, metadata(f.data)))
        case g: Polygon      => polys.append(Feature(winding(g), metadata(f.data)))
        case g: MultiPolygon => mpolys.append(Feature(MultiPolygon(g.polygons.map(winding)), metadata(f.data)))
      }})

      /* The values 4096 and 2 here are expected defaults. The `ListBuffer`s
       * are converted to `Vector` to ensure that their contents are immutable.
       */
      StrictLayer(
        name, 4096, 2, tileExtent, points.toVector, mpoints.toVector,
        lines.toVector, mlines.toVector, polys.toVector, mpolys.toVector
      )
    }

    /* Both `MMap` and `ListBuffer` are mutable */
    val collated: MMap[String, ListBuffer[Feature[G, D]]] = MMap.empty

    geoms.foreach({ f =>
      val layer: String = partition(f)

      collated.get(layer) match {
        case None => collated.update(layer, ListBuffer(f))
        case Some(lb) => lb.append(f)
      }
    })

    /* Collate each collection of Features into a [[Layer]] */
    val layers: Map[String, StrictLayer] =
      collated.map({ case (k,v) => (k, work(k,v)) }).toMap

    VectorTile(layers, tileExtent)
  }
}
