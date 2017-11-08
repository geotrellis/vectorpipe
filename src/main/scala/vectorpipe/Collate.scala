package vectorpipe

import scala.annotation.tailrec

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
  * for Polygon winding order manually.''' This can be done via the [[vectorpipe.util.winding]]
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
    case _               => "polygons"
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

  /** Give each Geometry type its own VectorTile layer, and store the [[ElementData]] as-is. */
  def byOSM(tileExtent: Extent, geoms: Iterable[OSMFeature]): VectorTile = {

    def metadata(m: ElementMeta): Map[String, Value] = {
      Map(
        "id"            -> VInt64(m.id),
        "user"          -> VString(m.user),
        "userId"        -> VInt64(m.uid),
        "changeSet"     -> VInt64(m.changeset),
        "version"       -> VInt64(m.version),
        "timestamp"     -> VString(m.timestamp.toString),
        "visible"       -> VBool(m.visible)
      ) ++ m.tags.map { case (k, v) => (k, VString(v)) }
    }

    generically(tileExtent, geoms, byGeomType, metadata)
  }

  def withStringMetadata[G <: Geometry](tileExtent: Extent, geoms: Iterable[Feature[G, Map[String, String]]]): VectorTile =
    generically(tileExtent, geoms, byGeomType, { d: Map[String,String] => d.map { case (k,v) => (k, VString(v)) }})

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

    def work(name: String, fs: Iterable[Feature[G, D]]): StrictLayer = {

      /* The expected defaults for Vector Tiles. */
      val vtExtent: Int = 4096
      val vtVersion: Int = 2

      /* Polygon winding order is corrected for here. */
      @tailrec def collate(
        poi: List[Feature[Point, Map[String, Value]]],
        mpo: List[Feature[MultiPoint, Map[String, Value]]],
        lin: List[Feature[Line, Map[String, Value]]],
        mli: List[Feature[MultiLine, Map[String, Value]]],
        pol: List[Feature[Polygon, Map[String, Value]]],
        mpl: List[Feature[MultiPolygon, Map[String, Value]]],
        fts: List[Feature[G, D]]
      ): StrictLayer = fts match {
        case Nil =>
          StrictLayer(name, vtExtent, vtVersion, tileExtent, poi, mpo, lin, mli, pol, mpl)
        case Feature(g: Point, d) :: rest =>
          collate(Feature(g, metadata(d)) :: poi, mpo, lin, mli, pol, mpl, rest)
        case Feature(g: MultiPoint, d) :: rest =>
          collate(poi, Feature(g, metadata(d)) :: mpo, lin, mli, pol, mpl, rest)
        case Feature(g: Line, d) :: rest =>
          collate(poi, mpo, Feature(g, metadata(d)) :: lin, mli, pol, mpl, rest)
        case Feature(g: MultiLine, d) :: rest =>
          collate(poi, mpo, lin, Feature(g, metadata(d)) :: mli, pol, mpl, rest)
        case Feature(g: Polygon, d) :: rest =>
          collate(poi, mpo, lin, mli, Feature(winding(g), metadata(d)) :: pol, mpl, rest)
        case Feature(g: MultiPolygon, d) :: rest =>
          collate(poi, mpo, lin, mli, pol, Feature(MultiPolygon(g.polygons.map(winding)), metadata(d)) :: mpl, rest)
        case _ :: rest =>
          collate(poi, mpo, lin, mli, pol, mpl, rest)
      }

      collate(Nil, Nil, Nil, Nil, Nil, Nil, fs.toList)
    }

    val collated: Map[String, Iterable[Feature[G, D]]] = geoms.groupBy(partition)

    /* Collate each collection of Features into a [[Layer]] */
    val layers: Map[String, StrictLayer] =
      collated.map { case (k, fs) => (k, work(k, fs)) }

    VectorTile(layers, tileExtent)
  }

}
