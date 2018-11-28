package vectorpipe.osm.internal

import java.math.BigDecimal

import vectorpipe.osm.internal.ProcessOSM._

import geotrellis.vector.{GeomFactory, Polygon => GTPolygon}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp

import org.locationtech.jts.geom
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.prep.PreparedGeometryFactory

import com.google.common.collect.{Range, RangeMap, TreeRangeMap}

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.GenTraversable


package object functions {
  // Convert BigDecimals to doubles
  // Reduces size taken for representation at the expense of some precision loss.
  val asDouble: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.doubleValue).getOrElse(Double.NaN)
  }

  // Convert BigDecimals to floats
  // Reduces size taken for representation at the expense of more precision loss.
  val asFloat: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.floatValue).getOrElse(Float.NaN)
  }

  val ST_AsText: UserDefinedFunction = udf {
    Option(_: Geometry).map((_.toText())).getOrElse("")
  }

  // Using tag listings from [id-area-keys](https://github.com/osmlab/id-area-keys) @ v2.8.0.
  private val AreaKeys: Map[String, Map[String, Boolean]] = Map(
    "addr:*" -> Map(),
    "advertising" -> Map(
      "billboard" -> true
    ),
    "aerialway" -> Map(
      "cable_car" -> true,
      "chair_lift" -> true,
      "drag_lift" -> true,
      "gondola" -> true,
      "goods" -> true,
      "magic_carpet" -> true,
      "mixed_lift" -> true,
      "platter" -> true,
      "rope_tow" -> true,
      "t-bar" -> true
    ),
    "aeroway" -> Map(
      "runway" -> true,
      "taxiway" -> true
    ),
    "allotments" -> Map(),
    "amenity" -> Map(
      "bench" -> true
    ),
    "area:highway" -> Map(),
    "attraction" -> Map(
      "dark_ride" -> true,
      "river_rafting" -> true,
      "train" -> true,
      "water_slide" -> true
    ),
    "building" -> Map(),
    "camp_site" -> Map(),
    "club" -> Map(),
    "craft" -> Map(),
    "emergency" -> Map(
      "designated" -> true,
      "destination" -> true,
      "no" -> true,
      "official" -> true,
      "private" -> true,
      "yes" -> true
    ),
    "golf" -> Map(
      "hole" -> true,
      "lateral_water_hazard" -> true,
      "water_hazard" -> true
    ),
    "healthcare" -> Map(),
    "historic" -> Map(),
    "industrial" -> Map(),
    "junction" -> Map(
      "circular" -> true,
      "roundabout" -> true
    ),
    "landuse" -> Map(),
    "leisure" -> Map(
      "slipway" -> true,
      "track" -> true
    ),
    "man_made" -> Map(
      "breakwater" -> true,
      "crane" -> true,
      "cutline" -> true,
      "embankment" -> true,
      "groyne" -> true,
      "pier" -> true,
      "pipeline" -> true
    ),
    "military" -> Map(),
    "natural" -> Map(
      "cliff" -> true,
      "coastline" -> true,
      "ridge" -> true,
      "tree_row" -> true
    ),
    "office" -> Map(),
    "piste:type" -> Map(),
    "place" -> Map(),
    "playground" -> Map(
      "balancebeam" -> true,
      "slide" -> true,
      "zipwire" -> true
    ),
    "power" -> Map(
      "line" -> true,
      "minor_line" -> true
    ),
    "public_transport" -> Map(
      "platform" -> true
    ),
    "shop" -> Map(),
    "tourism" -> Map(),
    "waterway" -> Map(
      "canal" -> true,
      "dam" -> true,
      "ditch" -> true,
      "drain" -> true,
      "river" -> true,
      "stream" -> true,
      "weir" -> true
    )
  )

  private val MultiPolygonTypes = Set("multipolygon", "boundary")

  private val BooleanValues = Set("yes", "no", "true", "false", "1", "0")

  private val TruthyValues = Set("yes", "true", "1")

  private lazy val logger = Logger.getLogger(getClass)

  private val factory = GeomFactory.factory

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && BooleanValues.contains(tags("area").toLowerCase) =>
        TruthyValues.contains(tags("area").toLowerCase)
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(AreaKeys.keySet)
        matchingKeys.exists(k => !AreaKeys(k).contains(tags(k)))
    }

  val isArea: UserDefinedFunction = udf(_isArea)

  private val _isMultiPolygon = (tags: Map[String, String]) =>
    tags.contains("type") && MultiPolygonTypes.contains(tags("type").toLowerCase)

  val isMultiPolygon: UserDefinedFunction = udf(_isMultiPolygon)

  val isRoute: UserDefinedFunction = udf { (tags: Map[String, String]) =>
    tags.contains("type") && tags("type") == "route"
  }

  private val MemberSchema = ArrayType(
    StructType(
      StructField("type", ByteType, nullable = false) ::
        StructField("ref", LongType, nullable = false) ::
        StructField("role", StringType, nullable = false) ::
        Nil), containsNull = false)

  private val _compressMemberTypes = (members: Seq[Row]) =>
    members.map { row =>
      val t = row.getAs[String]("type") match {
        case "node" => NodeType
        case "way" => WayType
        case "relation" => RelationType
      }
      val ref = row.getAs[Long]("ref")
      val role = row.getAs[String]("role")

      Row(t, ref, role)
    }

  val compressMemberTypes: UserDefinedFunction = udf(_compressMemberTypes, MemberSchema)

  class AssemblyException(msg: String) extends Exception(msg)

  /**
    * Tests whether two {@link CoordinateSequence}s are equal.
    * To be equal, the sequences must be the same length.
    * They do not need to be of the same dimension,
    * but the ordinate values for the smallest dimension of the two
    * must be equal.
    * Two <code>NaN</code> ordinates values are considered to be equal.
    *
    * Ported to Scala from JTS 1.15.0
    *
    * @param cs1 a CoordinateSequence
    * @param cs2 a CoordinateSequence
    * @return true if the sequences are equal in the common dimensions
    */
  private def isEqual(cs1: CoordinateSequence, cs2: CoordinateSequence): Boolean = {
    if (cs1.size != cs2.size) {
      false
    } else {
      val dim = Math.min(cs1.getDimension, cs2.getDimension)
      (0 until cs1.size).forall(i => {
        (0 until dim).forall(d => {
          val v1 = cs1.getOrdinate(i, d)
          val v2 = cs2.getOrdinate(i, d)

          v1 == v2 || (v1 == Double.NaN && v2 == Double.NaN)
        })
      })
    }
  }

  private class ReversedCoordinateSequence(sequence: CoordinateSequence) extends CoordinateSequence {
    override def copy(): ReversedCoordinateSequence = this

    private def getIndex(i: Int): Int = size - 1 - i

    override def getDimension: Int = sequence.getDimension

    override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(getIndex(i))

    override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(getIndex(i))

    override def getCoordinate(index: Int, coord: Coordinate): Unit = sequence.getCoordinate(getIndex(index), coord)

    override def getX(index: Int): Double = sequence.getX(getIndex(index))

    override def getY(index: Int): Double = sequence.getY(getIndex(index))

    override def getOrdinate(index: Int, ordinateIndex: Int): Double =
      sequence.getOrdinate(getIndex(index), ordinateIndex)

    override def size(): Int = sequence.size

    override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
      sequence.setOrdinate(getIndex(index), ordinateIndex, value)

    private lazy val coordinates: Array[Coordinate] = {
      val coords = new Array[Coordinate](size())

      for (i <- size - 1 to 0) {
        coords(i) = getCoordinate(i)
      }

      coords
    }

    override def toCoordinateArray: Array[Coordinate] = coordinates

    override def expandEnvelope(env: Envelope): Envelope = sequence.expandEnvelope(env)

    override def clone(): AnyRef = new ReversedCoordinateSequence(sequence)
  }

  private class PartialCoordinateSequence(sequence: CoordinateSequence, offset: Int) extends CoordinateSequence {
    override def copy(): PartialCoordinateSequence = this

    private lazy val _size: Int = sequence.size() - offset

    private lazy val coordinates: Array[Coordinate] = {
      val coords = new Array[Coordinate](size())

      for (i <- 0 until size) {
        coords(i) = getCoordinate(i)
      }

      coords
    }

    override def getDimension: Int = sequence.getDimension

    override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(offset + i)

    override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(offset + i)

    override def getCoordinate(index: Int, coord: Coordinate): Unit = sequence.getCoordinate(offset + index, coord)

    override def getX(index: Int): Double = sequence.getX(offset + index)

    override def getY(index: Int): Double = sequence.getY(offset + index)

    override def getOrdinate(index: Int, ordinateIndex: Int): Double =
      sequence.getOrdinate(offset + index, ordinateIndex)

    override def size(): Int = _size

    override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
      sequence.setOrdinate(offset + index, ordinateIndex, value)

    override def toCoordinateArray: Array[Coordinate] = coordinates

    override def expandEnvelope(env: Envelope): Envelope = {
      for (i <- 0 until size) {
        env.expandToInclude(getX(i), getY(i))
      }

      env
    }

    override def clone(): AnyRef = new PartialCoordinateSequence(sequence, offset)
  }

  // rather than being a nested set of CoordinateSequences, this is a mutable wrapper to avoid deep call stacks
  private class VirtualCoordinateSequence(sequences: Seq[CoordinateSequence]) extends CoordinateSequence {
    override def copy(): VirtualCoordinateSequence = this

    private val rangeMap: RangeMap[Integer, CoordinateSequence] = TreeRangeMap.create[Integer, CoordinateSequence]

    sequences.zip(sequences.map(_.size).scanLeft(0)(_ + _).dropRight(1))
      .map { case (seq, offset) => (seq, Range.closed(offset: Integer, offset + seq.size - 1: Integer)) }
      .foreach { case (seq, range) => rangeMap.put(range, seq)}

    private var dimension: Int = sequences.map(_.getDimension).min

    private var _size: Int = sequences.map(_.size).sum

    private def getSequence(i: Int): (CoordinateSequence, Int) = {
      val entry = rangeMap.getEntry(i: Integer)

      (entry.getValue, i - entry.getKey.lowerEndpoint)
    }

    def append(sequence: CoordinateSequence): VirtualCoordinateSequence = {
      val upperEndpoint = rangeMap.span.upperEndpoint
      val range = Range.closed(upperEndpoint + 1: Integer, upperEndpoint + sequence.size: Integer)
      rangeMap.put(range, sequence)

      dimension = Math.min(dimension, sequence.getDimension)
      _size += sequence.size

      this
    }

    override def getDimension: Int = dimension

    override def getCoordinate(i: Int): Coordinate = {
      val (sequence, index) = getSequence(i)

      // bypass PackedCoordinateSequence.getCoordinate to prevent caching and associated allocation
      new Coordinate(sequence.getX(index), sequence.getY(index))
    }

    override def getCoordinateCopy(i: Int): Coordinate = {
      val (sequence, index) = getSequence(i)

      sequence.getCoordinateCopy(index)
    }

    override def getCoordinate(i: Int, coord: Coordinate): Unit = {
      val (sequence, index) = getSequence(i)

      sequence.getCoordinate(index, coord)
    }

    override def getX(i: Int): Double = {
      val (sequence, index) = getSequence(i)

      sequence.getX(index)
    }

    override def getY(i: Int): Double = {
      val (sequence, index) = getSequence(i)

      sequence.getY(index)
    }

    override def getOrdinate(i: Int, ordinateIndex: Int): Double = {
      val (sequence, index) = getSequence(i)

      sequence.getOrdinate(index, ordinateIndex)
    }

    override def size(): Int = _size

    override def setOrdinate(i: Int, ordinateIndex: Int, value: Double): Unit = {
      val (sequence, index) = getSequence(i)

      sequence.setOrdinate(index, ordinateIndex, value)
    }

    private lazy val coordinates: Array[Coordinate] = {
      val coords = new Array[Coordinate](size())

      for (i <- 0 until size) {
        coords(i) = getCoordinate(i)
      }

      coords
    }

    override def toCoordinateArray: Array[Coordinate] = coordinates

    override def expandEnvelope(env: Envelope): Envelope = {
      for (i <- 0 until size) {
        env.expandToInclude(getX(i), getY(i))
      }

      env
    }

    override def clone(): AnyRef = {
      // we're already playing fast and loose
      this
    }
  }

  // join segments together into rings
  @tailrec
  private def formRings(
    segments: GenTraversable[VirtualCoordinateSequence],
    rings: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence]
  ): GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        rings
      case Seq(h, t @ _ *) if h.getX(0) == h.getX(h.size - 1) && h.getY(0) == h.getY(h.size - 1) =>
        formRings(t, rings :+ h)
      case Seq(h, t @ _ *) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        formRings(t.find(line => x == line.getX(0) && y == line.getY(0)) match {
          case Some(next) =>
            h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line => isEqual(line, next))
          case None =>
            t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
              case Some(next) =>
                h.append(
                  new PartialCoordinateSequence(
                    new ReversedCoordinateSequence(next), 1
                  )
                ) +: t.filterNot(line => isEqual(line, next))
              case None => throw new AssemblyException("Unable to connect segments.")
            }
        }, rings)
    }
  }

  private def formRings(segments: GenTraversable[LineString]): GenTraversable[Polygon] =
    formRings(segments.map(_.getCoordinateSequence).map(s => new VirtualCoordinateSequence(Seq(s))))
      .map(factory.createPolygon)

  // join segments together
  @tailrec
  private def connectSegments(
    segments: GenTraversable[VirtualCoordinateSequence],
    lines: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence]
  ): GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        lines
      case Seq(h, t @ _ *) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        t.find(line => x == line.getX(0) && y == line.getY(0)) match {
          case Some(next) =>
            connectSegments(
              h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line => isEqual(line, next)),
              lines
            )
          case None =>
            t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
              case Some(next) =>
                connectSegments(
                  h.append(
                    new PartialCoordinateSequence(new ReversedCoordinateSequence(next), 1)
                  ) +: t.filterNot(line => isEqual(line, next)),
                  lines
                )
              case None => connectSegments(t, lines :+ h)
            }
        }
    }
  }

  private def connectSegments(segments: GenTraversable[LineString]): GenTraversable[LineString] =
    connectSegments(segments.map(_.getCoordinateSequence).map(s => new VirtualCoordinateSequence(Seq(s))))
      .map(factory.createLineString)

  private def getHoles(poly: Polygon): List[CoordinateSequence] = {
    for (i <- 0 until poly.getNumInteriorRing) yield
      poly.getInteriorRingN(i).getCoordinateSequence
  }.toList

  private def dissolveRings(rings: GenTraversable[Polygon]): (Seq[Polygon], Seq[Polygon]) = {
    val mp = factory.createMultiPolygon(rings.toArray).union
    val ps = for (i <- 0 until mp.getNumGeometries) yield mp.getGeometryN(i).asInstanceOf[Polygon]

    (
      ps.map(_.getExteriorRing).map(x => factory.createPolygon(x.getCoordinateSequence)),
      ps.flatMap(getHoles).map(factory.createPolygon)
    )
  }

  // TODO this (and accompanying functions) doesn't belong here
  def buildMultiPolygon(
    id: Long,
    version: Int,
    timestamp: Timestamp,
    types: Seq[Byte],
    roles: Seq[String],
    geometries: Seq[Geometry]
  ): Option[Geometry] = {
    if (types.zip(geometries).exists { case (t, g) => t == WayType && Option(g).isEmpty }) {
      // bail early if null values are present where they should exist (members w/ type=way)
      logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
      None
    } else {

      val geoms = geometries.map { geom =>
        geom match {
          case p: Polygon => Some(p.getExteriorRing())
          case l: LineString => Some(l)
          case _ => None
        }
      }

      val vertexCount = geoms.filter(_.isDefined).map(_.get).map(_.getNumPoints).sum

      logger.warn(s"${vertexCount.formatted("%,d")} vertices from ${types.size} members in $id @ $version ($timestamp)")

      val members: Seq[(String, LineString)] = roles.zip(geoms)
        .filter(_._2.isDefined)
        .map(x => (x._1, x._2.get))

      val (complete, partial) = members.foldLeft((Vector.empty[Polygon], Vector.empty[LineString])) {
        case ((c, p), (role, line: LineString)) =>
          role match {
            case ("outer" | "inner" | "") =>
              if (line.isClosed && line.getNumPoints >= 4)
                (c :+ factory.createPolygon(line.getCoordinateSequence), p)
              else
                (c, p :+ line)
            case _ => (c, p)
          }
      }

      try {
        val preparedFactory = new PreparedGeometryFactory

        val rings = complete ++ formRings(partial.sortWith(_.getNumPoints > _.getNumPoints))
        val preparedRings = rings.map(preparedFactory.create)

        // reclassify rings according to their topology (ignoring roles)
        val (classifiedOuters, classifiedInners) = rings.sortWith(_.getArea > _.getArea) match {
          case Seq(h, t@_ *) => t.foldLeft((Vector(h), Vector.empty[Polygon])) {
            case ((os, is), ring) =>
              // check the number of containing elements
              preparedRings.count(r => r != ring && r.contains(ring)) % 2 match {
                // if even, it's an outer ring
                case 0 => (os :+ ring, is)
                // if odd, it's an inner ring
                case 1 => (os, is :+ ring)
              }
          }
          case rs if rs.isEmpty => (Vector.empty[Polygon], Vector.empty[Polygon])
        }

        val (dissolvedOuters, addlInners) = dissolveRings(classifiedOuters)
        val (dissolvedInners, addlOuters) =
          dissolveRings(
            classifiedInners
            .map(_.getExteriorRing.getCoordinateSequence)
            .map(factory.createPolygon) ++ addlInners
          )

        val (polygons, _) = (dissolvedOuters ++ addlOuters)
          // sort by size (descending) to use rings as part of the largest available polygon
          .sortWith(_.getArea > _.getArea)
          // only use inners once if they're contained by multiple outer rings
          .foldLeft((Vector.empty[Polygon], dissolvedInners)) {
          case ((ps, is), outer) =>
            val preparedOuter = preparedFactory.create(outer)
            (
              ps :+ GTPolygon(
                outer.getExteriorRing.getCoordinateSequence,
                is
                  .filter(inner => preparedOuter.contains(inner))
                  .map(_.getExteriorRing.getCoordinateSequence)
                ).jtsGeom,
              is.filterNot(inner => preparedOuter.contains(inner))
            )
        }

        Some(polygons match {
          case Vector(p: Polygon) => p
          case ps => factory.createMultiPolygon(ps.toArray)
        })
      } catch {
        case e@(_: AssemblyException | _: IllegalArgumentException | _: TopologyException) =>
          logger.warn(s"Could not reconstruct relation $id @ $version ($timestamp): ${e.getMessage}")
          None
        case e: Throwable =>
          logger.warn(s"Could not reconstruct relation $id @ $version ($timestamp): $e")
          e.getStackTrace.foreach(logger.warn)
          None
      }
    }
  }

  def mergeTags: UserDefinedFunction = udf {
    (_: Map[String, String]) ++ (_: Map[String, String])
  }

  // TODO this (and accompanying functions) doesn't belong here
  def buildRoute(
    id: Long,
    version: Int,
    timestamp: Timestamp,
    types: Seq[Byte],
    roles: Seq[String],
    geometries: Seq[Geometry]
  ): Option[Seq[(String, Geometry)]] = {
    if (types.zip(geometries).exists { case (t, g) => t == WayType && Option(g).isEmpty }) {
      // bail early if null values are present where they should exist (members w/ type=way)
      logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
      None
    } else {
      val geoms = geometries.map { geom =>
        geom match {
          // polygons are not part of routes (unless they're platforms)
          case l: LineString => Some(l)
          case _ => None
        }
      }

      try {
        Some(roles.zip(geoms)
          .filter(_._2.isDefined)
          .map(x => (x._1, x._2.get))
          .groupBy { case (role, _) => role }
          .mapValues(_.map(_._2))
          .mapValues(connectSegments(_))
          .map { case (role, lines) =>
            lines match {
              case Seq(line) => (role, line)
              case _ => (role, factory.createMultiLineString(lines.toArray))
            }
          }.toSeq
        )
      } catch {
        case e@(_: AssemblyException | _: IllegalArgumentException | _: TopologyException) =>
          logger.warn(s"Could not reconstruct route relation $id @ $version ($timestamp): ${e.getMessage}")
          None
        case e: Throwable =>
          logger.warn(s"Could not reconstruct route relation $id @ $version ($timestamp): $e")
          e.getStackTrace.foreach(logger.warn)
          None
      }
    }
  }
}
