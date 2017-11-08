package vectorpipe.osm

import java.time._

import cats.implicits._
import geotrellis.vector.{ Extent, Point }
import io.dylemma.spac._
import monocle.macros.Lenses

// --- //

/** A sum type for OSM Elements. All Element types share some common attributes. */
sealed trait Element {
  def meta: ElementMeta
}

private[vectorpipe] object Element {
  implicit val tag: Parser[(String, String)] = (
    Parser.forMandatoryAttribute("k") ~
      Parser.forMandatoryAttribute("v")
  ).as({ case (k, v) => (k, v) }) // Hand-holding the typesystem.

  implicit val elementMeta: Parser[ElementMeta] = (
    Parser.forMandatoryAttribute("id").map(_.toLong) ~
    Parser.forOptionalAttribute("user").map(_.getOrElse("anonymous")) ~
    Parser.forOptionalAttribute("uid").map(_.map(_.toLong).getOrElse(0L)) ~
    Parser.forMandatoryAttribute("changeset").map(_.toLong) ~
    Parser.forMandatoryAttribute("version").map(_.toLong) ~
    Parser.forMandatoryAttribute("timestamp").map(Instant.parse(_)) ~
    Parser.forOptionalAttribute("visible").map(_.map(_.toBoolean).getOrElse(false)) ~
    /* <tag k='access' v='permissive' /> */
    Splitter(* \ "tag").asListOf[(String, String)].map(_.toMap)
  ).as(ElementMeta.apply)

  /* <node lat='49.5135613' lon='6.0095049' ... > */
  implicit val node: Parser[(Long, Node)] = (
    Parser.forOptionalAttribute("lat").map(_.map(_.toDouble).getOrElse(0.0)) ~
    Parser.forOptionalAttribute("lon").map(_.map(_.toDouble).getOrElse(0.0)) ~
    elementMeta
  ).as((lat, lon, m) => (m.id, Node(lat, lon, m)))

  /*
   <way ... >
    <nd ref='3867860331'/>
    ...
   </way>
   */
  implicit val way: Parser[(Long, Way)] = (
    Splitter(* \ "nd").through(Parser.forMandatoryAttribute("ref").map(_.toLong)).parseToList.map(_.toVector) ~
    elementMeta
  ).as((ns, m) => (m.id, Way(ns, m)))

  /* <member type='way' ref='22902411' role='outer' /> */
  implicit val member: Parser[Member] = (
    Parser.forMandatoryAttribute("type") ~
    Parser.forMandatoryAttribute("ref").map(_.toLong) ~
    Parser.forMandatoryAttribute("role")
  ).as(Member)

  implicit val relation: Parser[(Long, Relation)] = (
    Splitter(* \ "member").asListOf[Member] ~
    elementMeta
  ).as((ms, m) => (m.id, Relation(ms, m)))

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
  val elements: Parser[(List[(Long, Node)], List[(Long, Way)], List[(Long, Relation)])] = (
    Splitter("osm" \ "node").asListOf[(Long, Node)] ~
    Splitter("osm" \ "way").asListOf[(Long, Way)] ~
    Splitter("osm" \ "relation").asListOf[(Long, Relation)]
  ).as { case (ns, ws, rs) => (ns, ws, rs) }
}

/**
 * Some point in the world, which could represent a location or small object
 *  like a park bench or flagpole.
 */
case class Node(lat: Double, lon: Double, meta: ElementMeta) extends Element

/**
 * A string of [[Node]]s which could represent a road, or if connected back around
 *  to itself, a building, water body, landmass, etc.
 *
 *  Assumption: A Way has at least two distinct nodes.
 */
case class Way(nodes: Vector[Long], meta: ElementMeta) extends Element {
  /** Is it a Polyline, but not an "Area" even if closed? */
  def isLine: Boolean = !isClosed || (!isArea && isHighwayOrBarrier)

  /* `nodes` is a Vector so that `lastOption` here is fast. */
  def isClosed: Boolean = (nodes.headOption, nodes.lastOption).mapN(_ === _).getOrElse(false)

  def isArea: Boolean = meta.tags.get("area").map(_ === "yes").getOrElse(false)

  def isHighwayOrBarrier: Boolean = {
    val tags: Set[String] = meta.tags.keySet

    tags.contains("highway") || tags.contains("barrier")
  }
}

case class Relation(
  members: List[Member],
  meta: ElementMeta
) extends Element {
  /** The IDs of sub-relations that this Relation points to. */
  def subrelations: Seq[Long] = members.filter(_.typeOf === "relation").map(_.ref)
}

case class Member(
  typeOf: String,
  ref: Long,
  role: String)

/** All Element types have these attributes in common. */
@Lenses case class ElementMeta(
  id: Long,
  user: String,
  userId: Long,
  changeSet: Long,
  version: Long,
  timestamp: Instant,
  visible: Boolean,
  tags: Map[String, String]
)
