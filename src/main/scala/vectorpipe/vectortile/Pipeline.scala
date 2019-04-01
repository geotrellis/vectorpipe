package vectorpipe.vectortile

import geotrellis.vector.Geometry
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling._
import org.apache.spark.sql.{DataFrame, Row}
import org.locationtech.jts.{geom => jts}

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
 * This API treats vector tile generation as an inherently heirarchical process.
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
 */
trait Pipeline {
  /**
   * The root URI for output.
   */
  val baseURI: java.net.URI

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
   * initial zoom level.  All participating geometries will already have been
   * keyed to the given layout, with the list of relevant keys available in the
   * "keys" field.
   *
   * @param   input           A DataFrame minimally containing a "geom" field of
   *                          JTS [[Geometry]], a "keys" field of
   *                          Array[SpatialKey], and a "tags" field
   * @param   layoutLevel     The layout level
   */
  def reduce(input: DataFrame, layoutLevel: LayoutLevel): DataFrame

  /*
   * Lower complexity of geometry while moving to less resolute zoom levels.
   *
   * While moving from finer to coarser levels of the pyramid, it may not be
   * necessary to maintain the full level of detail of available geometries.
   * This function is used to reduce the complexity of geometries.  The
   * [[LayoutDefinition]] will be for the target zoom level.
   */
  def simplify(g: jts.Geometry, layout: LayoutDefinition): jts.Geometry

  /**
   * Select geometries for display at a given zoom level.
   *
   * VectorPipe allows geometries to be carried up the pyramid for display at a
   * later time.  This function is used to choose the elements that will be
   * displayed at the target zoom level.  This is useful for implementing a
   * display schema, or for example, selection based on prominence (relying on
   * the array of spatial keys added to the data frame in the column with the
   * longest name fitting the regular expression '[_]+keys').
   */
  def select(input: DataFrame, targetZoom: Int): DataFrame

  /**
   * Clip geometries prior to writing to vector tiles.
   *
   * It may be desirable to carry only the portion of a geometry that intersects
   * a given vector tile to keep down memory usage.  This function can be used
   * to implement such schemes.
   */
  def clip(geom: jts.Geometry, key: SpatialKey, layoutLevel: LayoutLevel): jts.Geometry

  /**
   * Convert table rows to output features.
   *
   * A straightforward conversion from a table row to a geometric feature.  The
   * data carried by the feature are stored as entries in a Map[String, Value].
   * See [[geotrellis.vectortile.Value]] for details.
   */
  def pack[G <: Geometry](row: Row, zoom: Int): VectorTileFeature[G]
}
