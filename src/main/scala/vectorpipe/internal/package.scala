package vectorpipe

import java.sql.Timestamp

import geotrellis.vector._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.{geom => jts}
import vectorpipe.functions.asDouble
import vectorpipe.functions.osm._
import vectorpipe.relations.{MultiPolygons, Routes}

package object internal {
  val NodeType: Byte = 1
  val WayType: Byte = 2
  val RelationType: Byte = 3
  val MultiPolygonRoles: Seq[String] = Set("", "outer", "inner").toSeq

  @transient lazy val logger: Logger = Logger.getLogger(getClass)

  /**
    * Pre-process nodes.
    *
    * Copies coordinates + tags from versions prior to being deleted (!'visible), as they're cleared out otherwise, and
    * adds 'validUntil based on the creation timestamp of the next version. Nodes with null 'validUntil values are
    * currently valid.
    *
    * @param history DataFrame containing nodes.
    * @return processed nodes.
    */
  def preprocessNodes(history: DataFrame, extent: Option[Extent] = None): DataFrame = {
    import history.sparkSession.implicits._

    val filteredHistory =
      extent match {
        case Some(e) =>
          history
            .where('lat > e.ymin and 'lat < e.ymax)
            .where('lon > e.xmin and 'lon < e.xmax)
        case None =>
          history
      }

    if (filteredHistory.columns.contains("validUntil")) {
      filteredHistory
    } else {
      @transient val idByVersion = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags
      // present and use those
      filteredHistory
        .where('type === "node")
        .repartition('id)
        .withColumn("lat", asDouble('lat))
        .withColumn("lon", asDouble('lon))
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
            lag('tags, 1) over idByVersion)
            .otherwise('tags) as 'tags,
          when(!'visible, lag('lat, 1) over idByVersion).otherwise('lat) as 'lat,
          when(!'visible, lag('lon, 1) over idByVersion).otherwise('lon) as 'lon,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByVersion) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible,
          !((lag('lat, 1) over idByVersion) <=> 'lat and (lag('lon, 1) over idByVersion) <=> 'lon) as 'geometryChanged
        )
    }
  }

  /**
    * Pre-process ways.
    *
    * Copies tags from versions prior to being deleted (!'visible), as they're cleared out otherwise, dereferences
    * 'nds.ref, and adds 'validUntil based on the creation timestamp of the next version. Ways with null 'validUntil
    * values are currently valid.
    *
    * @param history DataFrame containing ways.
    * @return processed ways.
    */
  def preprocessWays(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByVersion = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags or nds; use a window function to retrieve the last
      // tags and nds present and use those
      history
        .where('type === "way")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
            lag('tags, 1) over idByVersion)
            .otherwise('tags) as 'tags,
          when(!'visible, lag($"nds.ref", 1) over idByVersion)
            .otherwise($"nds.ref") as 'nds,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByVersion) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible,
          !((lag('nds, 1) over idByVersion) <=> 'nds) as 'geometryChanged
        )
    }
  }

  /**
    * Pre-process relations.
    *
    * Copies tags from versions prior to being deleted (!'visible), as they're cleared out otherwise, and adds
    * 'validUntil based on the creation timestamp of the next version. Relations with null 'validUntil values are
    * currently valid.
    *
    * @param history DataFrame containing relations.
    * @return processed relations.
    */
  def preprocessRelations(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

      // when an element has been deleted, it doesn't include any tags; use a window function to retrieve the last tags
      // present and use those
      history
        .where('type === "relation")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull,
            lag('tags, 1) over idByUpdated)
            .otherwise('tags) as 'tags,
          when(!'visible, lag('members, 1) over idByUpdated)
            .otherwise('members) as 'members,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByUpdated) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible)
    }
  }

  /**
    * Construct point geometries.
    *
    * "Uninteresting" nodes are not included, so this is not suitable for producing vertices for way/relation assembly.
    *
    * @param nodes DataFrame containing nodes
    * @return Nodes as Point geometries
    */
  def constructPointGeometries(nodes: DataFrame): DataFrame = {
    import nodes.sparkSession.implicits._

    val ns = preprocessNodes(nodes)
      .where(size(removeSemiInterestingTags('tags)) > 0)

    ns
      // fetch the last version of a node within a single changeset
      .select('changeset, 'id, 'version, 'timestamp)
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('timestamp) as 'updated)
      .join(ns.drop('changeset), Seq("id", "version"))
      .select(
        lit(NodeType) as '_type,
        'id,
        when('lon.isNotNull and 'lat.isNotNull, st_makePoint('lon, 'lat)) as 'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version)
  }

  /**
    * Reconstruct way geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset). Output geometries will be LineStrings or Polygons
    * according to OSM conventions for defining areas.
    *
    * @param _ways        DataFrame containing ways to reconstruct.
    * @param _nodes       DataFrame containing nodes used to construct ways.
    * @param _nodesToWays Optional lookup table.
    * @return Way geometries.
    */
  def reconstructWayGeometries(_ways: DataFrame, _nodes: DataFrame, _nodesToWays: Option[DataFrame] = None): DataFrame = {
    implicit val ss: SparkSession = _ways.sparkSession
    import ss.implicits._
    ss.withJTS

    @transient val idByVersion = Window.partitionBy('id).orderBy('version)

    val nodes = preprocessNodes(_nodes)
      // no longer correct after filtering out unchanged geometries
      .drop('validUntil)
      .where('geometryChanged)
      .drop('geometryChanged)
      // re-calculate validUntil windows
      .withColumn("validUntil", lead('timestamp, 1) over idByVersion)

    val ways = preprocessWays(_ways)
      .withColumn("isArea", isArea('tags))

    // Create (or re-use) a lookup table for node → ways
    val nodesToWays = _nodesToWays.getOrElse[DataFrame] {
      ways
        .select(explode('nds) as 'id, 'id as 'wayId, 'version, 'timestamp, 'validUntil)
    }

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that
    // triggered the association. This will later be used to assemble ways at each of those points in time. If you need
    // authorship, join on changesets
    // TODO check on partitioning of nodes (assume that the thing requesting the join gets to keep its partitioning)
    val waysByChangeset = nodes
      .select('changeset, 'id, 'timestamp as 'updated)
      .join(nodesToWays, Seq("id"))
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
      .select('changeset, 'wayId as 'id, 'version, 'updated)

    val allWayVersions = waysByChangeset
      // Union with raw ways to include those in the time line (if they weren't already triggered by node modifications
      // at the same time)
      .union(ways.select('changeset, 'id, 'version, 'timestamp as 'updated))
      // If a node and a way were modified within the same changeset at different times, there will be multiple entries
      // per changeset (with different timestamps). There should only be one per changeset.
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
      .join(ways.select('id, 'version, 'nds, 'isArea), Seq("id", "version"))

    val explodedWays = allWayVersions
      .select('changeset, 'id, 'version, 'updated, 'isArea, posexplode_outer('nds) as Seq("idx", "ref"))
      // repartition including updated timestamp to avoid skew (version is insufficient, as
      // multiple instances may exist with the same version)
      .repartition('id, 'updated)

    val waysAndNodes = explodedWays
      .join(nodes.select('id as 'ref, 'timestamp, 'validUntil, 'lat, 'lon), Seq("ref"), "left_outer")
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))

    val wayGeoms = waysAndNodes
      .select('changeset, 'id, 'version, 'updated, 'isArea, 'idx, 'lat, 'lon)
      .groupByKey(row =>
        (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Timestamp]("updated"))
      )
      .mapGroups {
        case ((changeset, id, version, updated), rows) =>
          val nds = rows.toVector
          val isArea = nds.head.getAs[Boolean]("isArea")
          val geom = nds
            .sortWith((a, b) => a.getAs[Int]("idx") < b.getAs[Int]("idx"))
            .map { row =>
              Seq(Option(row.get(row.fieldIndex("lon"))).map(_.asInstanceOf[Double]).getOrElse(Double.NaN),
                  Option(row.get(row.fieldIndex("lat"))).map(_.asInstanceOf[Double]).getOrElse(Double.NaN))
            } match {
              // no coordinates provided
              case coords if coords.isEmpty => Some(GeomFactory.factory.createLineString(Array.empty[jts.Coordinate]))
              // some of the coordinates are empty; this is invalid
              case coords if coords.exists(Option(_).isEmpty) => None
              // some of the coordinates are invalid
              case coords if coords.exists(_.exists(_.isNaN)) => None
              // 1 pair of coordinates provided
              case coords if coords.length == 1 =>
                Some(GeomFactory.factory.createPoint(new jts.Coordinate(coords.head.head, coords.head.last)))
              case coords =>
                val coordinates = coords.map(xy => new jts.Coordinate(xy.head, xy.last)).toArray
                val line = GeomFactory.factory.createLineString(coordinates)

                if (isArea && line.getNumPoints >= 4 && line.isClosed)
                  Some(GeomFactory.factory.createPolygon(line.getCoordinateSequence))
                else
                  Some(line)
            }
          val geometry = geom match {
            case Some(g) if g.isValid => g
            case _ => null
          }
          (changeset, id, version, updated, geometry)
      }
      .toDF("changeset", "id", "version", "updated", "geom")

    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    wayGeoms
      // Assign `minorVersion` and rewrite `validUntil` to match
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)
      .withColumn("geometryChanged", !((lag('geom, 1) over idByUpdated) <=> 'geom))
      .join(ways.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(WayType) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion,
        'geometryChanged)
  }

  private def getRelationMembers(relations: DataFrame, geoms: DataFrame) = {
    implicit val ss: SparkSession = relations.sparkSession
    import ss.implicits._

    // way to relation lookup table (missing 'role, since it's not needed here)
    val waysToRelations = relations
      .select(explode('members) as 'member, 'id as 'relationId, 'version, 'timestamp, 'validUntil)
      .withColumn("type", $"member.type")
      .withColumn("id", $"member.ref")
      .drop('member)

    @transient val idByVersion = Window.partitionBy('id).orderBy('version)

    // Create a relation entry for each changeset in which a geometry was modified, containing the timestamp and
    // changeset of the geometry that triggered the association. This will later be used to assemble relations at each
    // of those points in time.
    // If you need authorship, join on changesets
    val relationsByChangeset = geoms
      .where('geometryChanged)
      .drop('validUntil)
      // re-calculate validUntil windows
      .withColumn("validUntil", lead('updated, 1) over idByVersion)
      // TODO when expanding beyond relations referring to ways, geoms should include 'type for the join to work
      // properly
      .withColumn("type", lit(WayType))
      .select('type, 'changeset, 'id, 'updated)
      .join(waysToRelations, Seq("id", "type"))
      .where(waysToRelations("timestamp") <= geoms("updated") and
        geoms("updated") < coalesce(waysToRelations("validUntil"), current_timestamp))
      .select('changeset, 'relationId as 'id, 'version, 'updated)

    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    val allRelationVersions = relationsByChangeset
      // Union with raw relations to include those in the time line (if they weren't already triggered by geometry
      // modifications at the same time)
      .union(relations.select('changeset, 'id, 'version, 'timestamp as 'updated))
      // If a node, a way, and/or a relation were modified within the same changeset at different times, there will be
      // multiple entries with different timestamps; this reduces them down to a single update per changeset.
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
      .join(relations.select('id, 'version, 'members), Seq("id", "version"))
      // assign `minorVersion` and rewrite `validUntil` to match
      // this is done early (at the expense of passing through the shuffle w/ exploded 'members) to avoid extremely
      // large partitions (see: Germany w/ 7k+ geometry versions) after geometries have been constructed
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)

    allRelationVersions
      .select('changeset, 'id, 'version, 'minorVersion, 'updated, 'validUntil, explode_outer('members) as "member")
      .select(
        'changeset,
        'id,
        'version,
        'minorVersion,
        'updated,
        'validUntil,
        'member.getField("type") as 'type,
        'member.getField("ref") as 'ref,
        'member.getField("role") as 'role
      )
      .distinct
  }

  /**
    * Reconstruct relation geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset). Output geometries will be MultiPolygons,
    * LineStrings, or MultiLineStrings depending on the relation type.
    *
    * @param _relations DataFrame containing relations to reconstruct.
    * @param geoms      DataFrame containing geometries to use in reconstruction.
    * @return Relation geometries.
    */
  def reconstructRelationGeometries(_relations: DataFrame, geoms: DataFrame): DataFrame = {
    val relations = preprocessRelations(_relations)

    reconstructMultiPolygonRelationGeometries(relations, geoms)
      .union(reconstructRouteRelationGeometries(relations, geoms))
  }

  /**
    * Reconstruct MultiPolygon relations.
    *
    * MultiPolygon relations are made up of way members with "inner" and "outer" roles. When individual way geometries
    * are not closed (i.e. suitable for use as linear rings), they will be joined together to form closed geometries.
    *
    * Boundaries (type=boundary) follow the same construction rules as MultiPolygons when polygonal geometry outputs
    * are desired.
    *
    * @param _relations DataFrame containing relations to reconstruct.
    * @param geoms      DataFrame containing geometries to use in reconstruction.
    * @return MultiPolygon relation geometries.
    */
  def reconstructMultiPolygonRelationGeometries(_relations: DataFrame, geoms: DataFrame): DataFrame = {
    implicit val ss: SparkSession = _relations.sparkSession
    import ss.implicits._
    ss.withJTS

    val relations = preprocessRelations(_relations)
      .where(isMultiPolygon('tags))

    val members = getRelationMembers(relations, geoms)
      .where('role.isin(MultiPolygonRoles: _*))
      // TODO when expanding beyond multipolygons, geoms should include 'type for the join to work properly
      .join(
      geoms.select(
        lit(WayType) as 'type,
        'id as "ref",
        'updated as 'memberUpdated,
        'validUntil as 'memberValidUntil,
        'geom), Seq("type", "ref"), "left_outer")
      .where(
        ('memberUpdated.isNull and 'memberValidUntil.isNull and 'geom.isNull) or // allow left outer join artifacts
          // through
          ('memberUpdated <= 'updated and 'updated < coalesce('memberValidUntil, current_timestamp)))
      .drop('memberUpdated)
      .drop('memberValidUntil)
      .drop('ref)

    val relationGeoms = members
        .groupByKey { row =>
          (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]
            ("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
        }
        .mapGroups {
          case ((changeset, id, version, minorVersion, updated, validUntil), rows) =>
            val members = rows.toVector
            val types = members.map(_.getAs[Byte]("type"))
            val roles = members.map(_.getAs[String]("role"))
            val geoms = members.map(_.getAs[jts.Geometry]("geom"))

            val geom = MultiPolygons.build(id, version, updated, types, roles, geoms).orNull

            (changeset, id, version, minorVersion, updated, validUntil, geom)
        }
        .toDF("changeset", "id", "version", "minorVersion", "updated", "validUntil", "geom")

    // Join metadata to avoid passing it through exploded shuffles
    relationGeoms
      .join(relations.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(RelationType) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion)
  }

  /**
    * Reconstruct route relations.
    *
    * Route relations are made of up linear ways. Where possible, contiguous ways with matching roles will be joined to
    * form longer segments. As multiple directions may be included in the relation (and segments may be discontiguous),
    * this produces MultiLineStrings in addition to LineStrings.
    *
    * @param _relations DataFrame containing relations to reconstruct.
    * @param geoms      DataFrame containing geometries to use in reconstruction.
    * @return LineString or MultiLineString route geometries.
    */
  def reconstructRouteRelationGeometries(_relations: DataFrame, geoms: DataFrame): DataFrame = {
    implicit val ss: SparkSession = _relations.sparkSession
    import ss.implicits._
    ss.withJTS

    val relations = preprocessRelations(_relations)
      .where(isRoute('tags))

    val members = getRelationMembers(relations, geoms)
      // TODO when expanding beyond way-based routes, geoms should include 'type for the join to work properly
      .join(
      geoms.select(
        lit(WayType) as 'type,
        'id as "ref",
        'updated as 'memberUpdated,
        'validUntil as 'memberValidUntil,
        'geom), Seq("type", "ref"), "left_outer")
      .where(
        ('memberUpdated.isNull and 'memberValidUntil.isNull and 'geom.isNull) or // allow left outer join artifacts
          // through
          ('memberUpdated <= 'updated and 'updated < coalesce('memberValidUntil, current_timestamp)))
      .drop('memberUpdated)
      .drop('memberValidUntil)
      .drop('ref)

    // leverage partitioning (avoids repeated (de-)serialization of merged coordinate arrays)
    val relationGeoms = members
      .groupByKey { row =>
        (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]
          ("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
      }
      .flatMapGroups {
        case ((changeset, id, version, minorVersion, updated, validUntil), rows) =>
          val members = rows.toVector
          val types = members.map(_.getAs[Byte]("type"))
          val roles = members.map(_.getAs[String]("role"))
          val geoms = members.map(_.getAs[jts.Geometry]("geom"))

          Routes.build(id, version, updated, types, roles, geoms) match {
            case Some(components) =>
              components.map {
                case ("", geom) =>
                  // no role
                  (changeset, id, Map.empty[String, String], version, minorVersion, updated, validUntil, geom)
                case (role, geom) =>
                  (changeset, id, Map("role" -> role), version, minorVersion, updated, validUntil, geom)
              }
            case None =>
              // no geometry
              Seq((changeset, id, Map.empty[String, String], version, minorVersion, updated, validUntil, null))
          }
      }
      .toDF("changeset", "id", "tags", "version", "minorVersion", "updated", "validUntil", "geom")

    // Join metadata to avoid passing it through exploded shuffles
    relationGeoms
      .join(relations.select('id, 'version, 'tags as 'originalTags, 'visible), Seq("id", "version"))
      .select(
        lit(RelationType) as '_type,
        'id,
        'geom,
        mergeTags('originalTags, 'tags) as 'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion)
  }

}
