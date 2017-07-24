package vectorpipe.osm

import java.time.ZonedDateTime

import geotrellis.vector.{ Extent, Point }
import io.dylemma.spac._

// --- //

/** A sum type for OSM Elements. All Element types share some common attributes. */
sealed trait Element {
  def data: ElementData
}

object Element {
  implicit val elementMeta: Parser[ElementMeta] = (
    Parser.forMandatoryAttribute("id").map(_.toLong) ~
    Parser.forOptionalAttribute("user").map(_.getOrElse("anonymous")) ~
    Parser.forOptionalAttribute("uid").map(_.getOrElse("anonymous")) ~
    Parser.forMandatoryAttribute("changeset").map(_.toLong) ~
    Parser.forMandatoryAttribute("version").map(_.toLong) ~
    Parser.forMandatoryAttribute("timestamp") ~
    Parser.forOptionalAttribute("visible").map(_.map(_.toBoolean).getOrElse(false))).as(ElementMeta)

  /* <tag k='access' v='permissive' /> */
  implicit val tag: Parser[(String, String)] = (
    Parser.forMandatoryAttribute("k") ~ Parser.forMandatoryAttribute("v")).as({ case (k, v) => (k, v) }) // Hand-holding the typesystem.

  implicit val elementData: Parser[ElementData] = (
    elementMeta ~
    Splitter(* \ "tag").asListOf[(String, String)].map(_.toMap)).as((meta, tags) => ElementData(meta, tags, None))

  /* <node lat='49.5135613' lon='6.0095049' ... > */
  implicit val node: Parser[Node] = (
    Parser.forMandatoryAttribute("lat").map(_.toDouble) ~
    Parser.forMandatoryAttribute("lon").map(_.toDouble) ~
    elementData).as((lat, lon, d) => Node(lat, lon, d.copy(extra = Some(Right(Point(lon, lat))))))

  /*
   <way ... >
    <nd ref='3867860331'/>
    ...
   </way>
   */
  implicit val way: Parser[Way] = (
    Splitter(* \ "nd")
    .through(Parser.forMandatoryAttribute("ref").map(_.toLong))
    .parseToList
    .map(_.toVector) ~
    elementData).as(Way)

  /* <member type='way' ref='22902411' role='outer' /> */
  implicit val member: Parser[Member] = (
    Parser.forMandatoryAttribute("type") ~
    Parser.forMandatoryAttribute("ref").map(_.toLong) ~
    Parser.forMandatoryAttribute("role")).as(Member)

  implicit val relation: Parser[Relation] = (
    Splitter(* \ "member").asListOf[Member] ~ elementData).as(Relation)

  /**
   * The master parser.
   *
   * ===Usage===
   * {{{
   * val xml: InputStream = new FileInputStream("somefile.osm")
   *
   * val res: Try[(List[Node], List[Way], List[Relation])] = Element.elements.parse(xml)
   * }}}
   */
  val elements: Parser[(List[Node], List[Way], List[Relation])] = (
    Splitter("osm" \ "node").asListOf[Node] ~
    Splitter("osm" \ "way").asListOf[Way] ~
    Splitter("osm" \ "relation").asListOf[Relation]).as({
      case (ns, ws, rs) =>
        (ns, ws, rs)
    })
}

/**
 * Some point in the world, which could represent a location or small object
 *  like a park bench or flagpole.
 */
case class Node(
  lat: Double,
  lon: Double,
  data: ElementData) extends Element

/**
 * A string of [[Node]]s which could represent a road, or if connected back around
 *  to itself, a building, water body, landmass, etc.
 *
 *  Assumption: A Way has at least two nodes.
 */
case class Way(
  nodes: Vector[Long], /* Vector for O(1) indexing */
  data: ElementData) extends Element {
  /** Is it a Polyline, but not an "Area" even if closed? */
  def isLine: Boolean = !isClosed || (!isArea && isHighwayOrBarrier)

  def isClosed: Boolean = if (nodes.isEmpty) false else nodes(0) == nodes.last

  def isArea: Boolean = data.tagMap.get("area").map(_ == "yes").getOrElse(false)

  def isHighwayOrBarrier: Boolean = {
    val tags: Set[String] = data.tagMap.keySet

    tags.contains("highway") || tags.contains("barrier")
  }
}

case class Relation(
  members: Seq[Member],
  data: ElementData) extends Element {
  /** The IDs of sub-relations that this Relation points to. */
  def subrelations: Seq[Long] = members.filter(_.`type` == "relation").map(_.ref)
}

case class Member(
  `type`: String,
  ref: Long,
  role: String)

case class ElementData(meta: ElementMeta, tagMap: Map[String, String], extra: Option[Either[Extent, Point]])

/** All Element types have these attributes in common. */
case class ElementMeta(
  id: Long,
  user: String,
  userId: String,
  changeSet: Long,
  version: Long,
  timestamp: String,
  visible: Boolean)

/** Extra element-specific metadata. */
private[vectorpipe] sealed trait Extra

private[vectorpipe] case class NodeExtra(point: Point, ways: Seq[Long]) extends Extra

private[vectorpipe] case class WayExtra(relations: Seq[Long]) extends Extra

private[vectorpipe] case class GeomExtra(envelope: Extent) extends Extra
