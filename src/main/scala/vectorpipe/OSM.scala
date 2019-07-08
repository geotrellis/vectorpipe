package vectorpipe

import java.sql.Timestamp

import org.locationtech.jts.{geom => jts}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vectorpipe.functions.osm.removeUninterestingTags
import vectorpipe.internal._

object OSM {
  /**
    * Convert a raw OSM dataframe into a frame containing JTS geometries for each unique id/changeset.
    *
    * This currently produces Points for nodes containing "interesting" tags, LineStrings and Polygons for ways
    * (according to OSM rules for defining areas), MultiPolygons for multipolygon and boundary relations, and
    * LineStrings / MultiLineStrings for route relations.
    *
    * @param input DataFrame containing node, way, and relation elements
    * @return DataFrame containing geometries.
    */
  def toGeometry(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._
    val st_pointToGeom = org.apache.spark.sql.functions.udf { pt: jts.Point => pt.asInstanceOf[jts.Geometry] }

    val elements = input
      .withColumn("tags", removeUninterestingTags('tags))

    val nodes = preprocessNodes(elements)

    val nodeGeoms = constructPointGeometries(nodes)
      .withColumn("minorVersion", lit(0))
      .withColumn("geom", st_pointToGeom('geom))

    val wayGeoms = reconstructWayGeometries(elements, nodes)

    val relationGeoms = reconstructRelationGeometries(elements, wayGeoms)

    nodeGeoms
      .union(wayGeoms.where(size('tags) > 0).drop('geometryChanged))
      .union(relationGeoms)
  }

  /**
    * Snapshot pre-processed elements.
    *
    * A Time Pin is stuck through a set of elements that have been augmented with a 'validUntil column to identify all
    * that were valid at a specific point in time (i.e. updated before the target timestamp and valid after it).
    *
    * @param df        Elements (including 'validUntil column)
    * @param timestamp Optional timestamp to snapshot at
    * @return DataFrame containing valid elements at timestamp (or now)
    */
  def snapshot(df: DataFrame, timestamp: Timestamp = null): DataFrame = {
    import df.sparkSession.implicits._

    df
      .where(
        'updated <= coalesce(lit(timestamp), current_timestamp)
          and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil, date_add(current_timestamp, 1)))
  }

  /**
    * Augment geometries with user metadata.
    *
    * When 'changeset is included, user (name and 'uid) metadata is joined from a DataFrame containing changeset
    * metadata.
    *
    * @param geoms      Geometries to augment.
    * @param changesets Changesets DataFrame with user metadata.
    * @return Geometries augmented with user metadata.
    */
  def addUserMetadata(geoms: DataFrame, changesets: DataFrame): DataFrame = {
    import geoms.sparkSession.implicits._

    geoms
      .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))
  }

}
