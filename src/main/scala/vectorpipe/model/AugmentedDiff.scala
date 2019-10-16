package vectorpipe.model

import java.sql.Timestamp

import geotrellis.vector._

case class AugmentedDiff(sequence: Int,
                         `type`: Byte,
                         id: Long,
                         prevGeom: Option[Geometry],
                         geom: Geometry,
                         prevTags: Option[Map[String, String]],
                         tags: Map[String, String],
                         prevNds: Option[Seq[Long]],
                         nds: Seq[Long],
                         prevChangeset: Option[Long],
                         changeset: Long,
                         prevUid: Option[Long],
                         uid: Long,
                         prevUser: Option[String],
                         user: String,
                         prevUpdated: Option[Timestamp],
                         updated: Timestamp,
                         prevVisible: Option[Boolean],
                         visible: Boolean,
                         prevVersion: Option[Int],
                         version: Int,
                         minorVersion: Boolean)

object AugmentedDiff {
  def apply(sequence: Int,
            prev: Option[Feature[Geometry, ElementWithSequence]],
            curr: Feature[Geometry, ElementWithSequence]): AugmentedDiff = {
    val `type` = Member.typeFromString(curr.data.`type`)
    val minorVersion = prev.map(_.data.version).getOrElse(Int.MinValue) == curr.data.version

    AugmentedDiff(
      sequence,
      `type`,
      curr.data.id,
      prev.map(_.geom),
      curr.geom,
      prev.map(_.data.tags),
      curr.data.tags,
      prev.map(_.data.nds),
      curr.data.nds,
      prev.map(_.data.changeset),
      curr.data.changeset,
      prev.map(_.data.uid),
      curr.data.uid,
      prev.map(_.data.user),
      curr.data.user,
      prev.map(_.data.timestamp),
      curr.data.timestamp,
      prev.map(_.data.visible.getOrElse(true)),
      curr.data.visible.getOrElse(true),
      prev.map(_.data.version),
      curr.data.version,
      minorVersion
    )
  }
}
