package vectorpipe.model

import vectorpipe.internal.{NodeType, RelationType, WayType}

import scala.xml.Node

case class Member(`type`: Byte, ref: Long, role: String)

object Member {
  def typeFromString(str: String): Byte = str match {
      case "node"     => NodeType
      case "way"      => WayType
      case "relation" => RelationType
      case _ => null.asInstanceOf[Byte]
  }

  def stringFromByte(b: Byte): String = b match {
      case NodeType     => "node"
      case WayType      => "way"
      case RelationType => "relation"
  }

  def fromXML(node: Node): Member = {
    val `type` = typeFromString(node \@ "type")
    val ref = (node \@ "ref").toLong
    val role = node \@ "role"

    Member(`type`, ref, role)
  }
}
