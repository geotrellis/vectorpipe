package vectorpipe.vectortile

import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling._
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vectortile.Value
import org.apache.spark.sql.{DataFrame, Row}
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.simplify

/**
 * The interface governing the transformation from processed OSM dataframes to
 * vector tiles.
 *
 * After loading an OSM source and transforming via [[OSM.toGeometry]] and any
 * filtering/modification steps, one may want to output a set of vector tiles
 * from the remaining geometry and metadata.  The vector tile output process is
 * governed by a Pipeline definition that describes how to convert a data frame
 * into a set of vector tiles and how to pass information to subsequent zoom
 * levels.
 *
 * [It is worth noting that nothing in this interface restricts the input data
 * to having originated from OSM.  Any DataFrame containing a column of JTS
 * geometry is a valid target.]
 *
 * This API treats vector tile generation as an inherently hierarchical process
 * governed by a series of operations.  Every zoom level will be addressed in
 * the same way, and the sequence of operations can be depicted schematically as
 * below:
 *
 *     ┌──────────┐
 *     │ Zoom z-1 │
 *     └──────────┘
 *          ↑
 *       simplify
 *          ↑
 *        reduce
 *          ↑
 *     ╔══════════╗                          ┌─────────────┐
 *     ║  Zoom z  ║ → select → clip → pack → │ VECTOR TILE │
 *     ╚══════════╝                          └─────────────┘
 *
 * Each of these operations is documented below.  The only extra note is that [[
 * reduce]] and [[simplify]] will be called prior to processing the initial zoom
 * level.
 */
trait Pipeline {
  /**
   * The root URI for output.
   */
  val baseOutputURI: java.net.URI

  /**
   * Name of the column containing the JTS geometry.
   */
  val geometryColumn: String

  /**
   * Declaration of layer generation style.
   *
   * Some pipelines are simple and require a single layer in the output vector
   * tile, while others require multiple layers.  This value is used by
   * pipelines to declare how layers are generated.  In the simplest case,
   * derived pipelines should declare
   * `val layerMultiplicity = SingleLayer(layerName)` in their definition.  This
   * will cause all geometries to be written out to the layer `layerName`.  For
   * more complex vectortile sets, one may set
   * `val layerMultiplicity = LayerNameInColumn(colName)`; this will require
   * that the custom pipeline create a column of name `colName` containing
   * [[String]] layer names where the different classes of geometries will be
   * found.  Each named layer will be collected separately and aggregated per
   * spatial key into a vectortile.
   */
  val layerMultiplicity: LayerMultiplicity

  /**
   * Reduce the input data between zoom levels.
   *
   * Not all data is useful in all levels of the vector tile pyramid.  When a
   * geometry will no longer be used in any subsequent generated level, it may be
   * desirable to drop the relevant rows from the DataFrame to limit the amount
   * of working data.  Moreover, it may be desirable to collect multiple entities
   * into a single, aggregated entity.  These sorts of operations are made
   * possible by the [[reduce]] function.
   *
   * This function will be called once before tile generation begins at the
   * initial zoom level.  Before this initial call, the geometry in each row
   * will have been automatically keyed to the given layout—that is, the list of
   * spatial keys that the geometry intersects will have been constructed and
   * made available in the column labeled `keyColumn`.
   *
   * @param   input           A DataFrame minimally containing a "geom" field of
   *                          JTS [[Geometry]] and a field of Array[SpatialKey]
   *                          with the name given by the keyColumn param, below
   * @param   layoutLevel     The layout level for the target zoom
   * @param   keyColumn       The name of the column containing
   *                          Array[SpatialKey] giving the list of keys that the
   *                          geometry interacts with
   */
  def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = input

  /*
   * Lower complexity of geometry while moving to less resolute zoom levels.
   *
   * While moving from finer to coarser levels of the pyramid, it may not be
   * necessary to maintain the full level of detail of available geometries.
   * This function is used to reduce the complexity of geometries.  The
   * [[LayoutDefinition]] will be for the target zoom level.
   *
   * By default, we return the input geometry without simplification, but there
   * is a simplifier using JTS's topology-preserving simplifier available in
   * [[vectorpipe.vectortile.Simplify]].
   */
  def simplify(g: jts.Geometry, layout: LayoutDefinition): jts.Geometry = g

  /**
   * Select geometries for display at a given zoom level.
   *
   * VectorPipe allows geometries to be carried up the pyramid for display at a
   * later time.  This function is used to choose the elements that will be
   * displayed at the target zoom level.  This is useful for implementing a
   * display schema, or for example, selection based on prominence.  VectorPipe
   * will add a column of Array[SpatialKey] in the column with the name given by
   * `keyColumn`.  These keys identify the layout tiles which intersect each
   * geometry at the current zoom level.  This information may be useful when
   * deciding which geometries to display.
   *
   * If [[layerNameIsColumn]] is true, the resulting DataFrame must supply a
   * String-typed column of the name contained in [[layerName]] to indicate
   * which layer a geometry belongs in.
   */
  def select(input: DataFrame, targetZoom: Int, keyColumn: String): DataFrame = input

  /**
   * Clip geometries prior to writing to vector tiles.
   *
   * It may be desirable to carry only the portion of a geometry that intersects
   * a given vector tile to keep down memory usage.  This function can be used
   * to implement such schemes.
   *
   * Basic (non-no-op) clipping functions can be found in [[Clipping]].
   */
  def clip(geom: jts.Geometry, key: SpatialKey, layoutLevel: LayoutLevel): jts.Geometry = geom

  /**
   * Convert table rows to output features.
   *
   * A straightforward conversion from a table row to a geometric feature.  The
   * data carried by the feature are stored as entries in a Map[String, Value].
   * See [[geotrellis.vectortile.Value]] for details.
   */
  def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] =
    Feature(Geometry(row.getAs[jts.Geometry](geometryColumn)), Map.empty)
}
